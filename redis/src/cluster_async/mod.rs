//! This module provides async functionality for Redis Cluster.
//!
//! By default, [`ClusterConnection`] makes use of [`MultiplexedConnection`] and maintains a pool
//! of connections to each node in the cluster. While it  generally behaves similarly to
//! the sync cluster module, certain commands do not route identically, due most notably to
//! a current lack of support for routing commands to multiple nodes.
//!
//! Also note that pubsub functionality is not currently provided by this module.
//!
//! # Example
//! ```rust,no_run
//! use redis::cluster::ClusterClient;
//! use redis::AsyncCommands;
//!
//! async fn fetch_an_integer() -> String {
//!     let nodes = vec!["redis://127.0.0.1/"];
//!     let client = ClusterClient::new(nodes).unwrap();
//!     let mut connection = client.get_async_connection().await.unwrap();
//!     let _: () = connection.set("test", "test_data").await.unwrap();
//!     let rv: String = connection.get("test").await.unwrap();
//!     return rv;
//! }
//! ```
use std::{
    collections::HashMap,
    fmt, io,
    iter::Iterator,
    marker::Unpin,
    mem,
    pin::Pin,
    sync::Arc,
    task::{self, Poll},
    time::Duration,
};

use crate::{
    aio::{ConnectionLike, MultiplexedConnection},
    cluster::{get_connection_info, parse_slots, slot_cmd},
    cluster_client::ClusterParams,
    cluster_routing::{Route, RoutingInfo, Slot, SlotAddr, SlotAddrs, SlotMap},
    Cmd, ConnectionInfo, ErrorKind, IntoConnectionInfo, RedisError, RedisFuture, RedisResult,
    Value,
};

#[cfg(all(not(feature = "tokio-comp"), feature = "async-std-comp"))]
use crate::aio::{async_std::AsyncStd, RedisRuntime};
use futures::{
    future::{self, BoxFuture},
    prelude::*,
    ready, stream,
};
use log::trace;
use pin_project_lite::pin_project;
use rand::seq::IteratorRandom;
use rand::thread_rng;
use tokio::sync::{mpsc, oneshot};

const SLOT_SIZE: usize = 16384;

/// This represents an async Redis Cluster connection. It stores the
/// underlying connections maintained for each node in the cluster, as well
/// as common parameters for connecting to nodes and executing commands.
#[derive(Clone)]
pub struct ClusterConnection<C = MultiplexedConnection>(mpsc::Sender<Message<C>>);

impl<C> ClusterConnection<C>
where
    C: ConnectionLike + Connect + Clone + Send + Sync + Unpin + 'static,
{
    pub(crate) async fn new(
        initial_nodes: &[ConnectionInfo],
        cluster_params: ClusterParams,
    ) -> RedisResult<ClusterConnection<C>> {
        ClusterConnInner::new(initial_nodes, cluster_params)
            .await
            .map(|inner| {
                let (tx, mut rx) = mpsc::channel::<Message<_>>(100);
                let stream = async move {
                    let _ = stream::poll_fn(move |cx| rx.poll_recv(cx))
                        .map(Ok)
                        .forward(inner)
                        .await;
                };
                #[cfg(feature = "tokio-comp")]
                tokio::spawn(stream);
                #[cfg(all(not(feature = "tokio-comp"), feature = "async-std-comp"))]
                AsyncStd::spawn(stream);

                ClusterConnection(tx)
            })
    }
}

type ConnectionFuture<C> = future::Shared<BoxFuture<'static, C>>;
type ConnectionMap<C> = HashMap<String, ConnectionFuture<C>>;

struct ClusterConnInner<C> {
    connections: ConnectionMap<C>,
    slots: SlotMap,
    state: ConnectionState<C>,
    #[allow(clippy::complexity)]
    in_flight_requests: stream::FuturesUnordered<
        Pin<Box<Request<BoxFuture<'static, (Option<String>, RedisResult<Response>)>, Response, C>>>,
    >,
    refresh_error: Option<RedisError>,
    pending_requests: Vec<PendingRequest<Response, C>>,
    cluster_params: ClusterParams,
}

#[derive(Clone)]
enum CmdArg<C> {
    Cmd {
        cmd: Arc<Cmd>,
        func: fn(C, Arc<Cmd>) -> RedisFuture<'static, Response>,
    },
    Pipeline {
        pipeline: Arc<crate::Pipeline>,
        offset: usize,
        count: usize,
        func: fn(C, Arc<crate::Pipeline>, usize, usize) -> RedisFuture<'static, Response>,
    },
}

impl<C> CmdArg<C> {
    fn exec(&self, con: C) -> RedisFuture<'static, Response> {
        match self {
            Self::Cmd { cmd, func } => func(con, cmd.clone()),
            Self::Pipeline {
                pipeline,
                offset,
                count,
                func,
            } => func(con, pipeline.clone(), *offset, *count),
        }
    }

    fn route(&self) -> Option<Route> {
        fn route_for_command(cmd: &Cmd) -> Option<Route> {
            match RoutingInfo::for_routable(cmd) {
                Some(RoutingInfo::Random) => None,
                Some(RoutingInfo::MasterSlot(slot)) => Some(Route::new(slot, SlotAddr::Master)),
                Some(RoutingInfo::ReplicaSlot(slot)) => Some(Route::new(slot, SlotAddr::Replica)),
                Some(RoutingInfo::AllNodes) | Some(RoutingInfo::AllMasters) => None,
                _ => None,
            }
        }

        match self {
            Self::Cmd { ref cmd, .. } => route_for_command(cmd),
            Self::Pipeline { ref pipeline, .. } => {
                let mut iter = pipeline.cmd_iter();
                let route = iter.next().map(route_for_command)?;
                for cmd in iter {
                    if route != route_for_command(cmd) {
                        return None;
                    }
                }
                route
            }
        }
    }
}

enum Response {
    Single(Value),
    Multiple(Vec<Value>),
}

struct Message<C> {
    cmd: CmdArg<C>,
    sender: oneshot::Sender<RedisResult<Response>>,
}

enum RecoverFuture<C> {
    RecoverSlots(
        BoxFuture<'static, Result<(SlotMap, ConnectionMap<C>), (RedisError, ConnectionMap<C>)>>,
    ),
    RecoverConns(BoxFuture<'static, ConnectionMap<C>>),
}

enum ConnectionState<C> {
    PollComplete,
    Recover(RecoverFuture<C>),
}

impl<C> fmt::Debug for ConnectionState<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                ConnectionState::PollComplete => "PollComplete",
                ConnectionState::Recover(_) => "Recover",
            }
        )
    }
}

struct RequestInfo<C> {
    cmd: CmdArg<C>,
    route: Option<Route>,
}

pin_project! {
    #[project = RequestStateProj]
    enum RequestState<F> {
        None,
        Future {
            #[pin]
            future: F,
        },
        Sleep {
            #[pin]
            sleep: BoxFuture<'static, ()>,
        },
    }
}

struct PendingRequest<I, C> {
    retry: u32,
    sender: oneshot::Sender<RedisResult<I>>,
    info: RequestInfo<C>,
}

pin_project! {
    struct Request<F, I, C> {
        max_retries: u32,
        request: Option<PendingRequest<I, C>>,
        #[pin]
        future: RequestState<F>,
    }
}

#[must_use]
enum Next<I, C> {
    TryAgain {
        request: PendingRequest<I, C>,
    },
    ReestablishConnection {
        request: PendingRequest<I, C>,
        addr: String,
        error: RedisError,
    },
    RefreshSlots {
        request: PendingRequest<I, C>,
        error: RedisError,
    },
    Done,
}

impl<F, I, C> Future for Request<F, I, C>
where
    F: Future<Output = (Option<String>, RedisResult<I>)>,
    C: ConnectionLike,
{
    type Output = Next<I, C>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut task::Context) -> Poll<Self::Output> {
        let mut this = self.as_mut().project();
        if this.request.is_none() {
            return Poll::Ready(Next::Done);
        }
        let future = match this.future.as_mut().project() {
            RequestStateProj::Future { future } => future,
            RequestStateProj::Sleep { sleep } => {
                ready!(sleep.poll(cx));
                return Next::TryAgain {
                    request: self.project().request.take().unwrap(),
                }
                .into();
            }
            _ => panic!("Request future must be Some"),
        };
        match ready!(future.poll(cx)) {
            (_, Ok(item)) => {
                trace!("Ok");
                self.respond(Ok(item));
                Next::Done.into()
            }
            (addr, Err(err)) => {
                println!("Request error {}", err);

                let request = this.request.as_mut().unwrap();

                if request.retry >= *this.max_retries {
                    self.respond(Err(err));
                    return Next::Done.into();
                }
                request.retry = request.retry.saturating_add(1);

                match err.kind() {
                    ErrorKind::Moved | ErrorKind::Ask => {
                        return Next::RefreshSlots {
                            request: this.request.take().unwrap(),
                            error: err,
                        }
                        .into();
                    }
                    ErrorKind::TryAgain | ErrorKind::ClusterDown => {
                        // Sleep and retry.
                        let sleep_duration =
                            Duration::from_millis(2u64.pow(request.retry.clamp(7, 16)) * 10);
                        this.future.set(RequestState::Sleep {
                            #[cfg(feature = "tokio-comp")]
                            sleep: Box::pin(tokio::time::sleep(sleep_duration)),

                            #[cfg(all(not(feature = "tokio-comp"), feature = "async-std-comp"))]
                            sleep: Box::pin(async_std::task::sleep(sleep_duration)),
                        });
                        return self.poll(cx);
                    }
                    ErrorKind::IoError => match addr {
                        Some(addr) => Next::ReestablishConnection {
                            request: this.request.take().unwrap(),
                            addr: addr,
                            error: err,
                        }
                        .into(),
                        None => Next::RefreshSlots {
                            request: this.request.take().unwrap(),
                            error: err,
                        }
                        .into(),
                    },
                    _ => {
                        // try again w/ master node if replica fails:
                        if let Some(route) = &request.info.route {
                            match route.slot_addr() {
                                SlotAddr::Master => {}
                                SlotAddr::Replica => {
                                    request.info.route =
                                        Some(Route::new(route.slot(), SlotAddr::Master));
                                }
                            }
                        }
                        // is this appropriate?
                        Next::TryAgain {
                            request: this.request.take().unwrap(),
                        }
                        .into()
                    }
                }
            }
        }
    }
}

impl<F, I, C> Request<F, I, C>
where
    F: Future<Output = (Option<String>, RedisResult<I>)>,
    C: ConnectionLike,
{
    fn respond(self: Pin<&mut Self>, msg: RedisResult<I>) {
        // If `send` errors the receiver has dropped and thus does not care about the message
        let _ = self
            .project()
            .request
            .take()
            .expect("Result should only be sent once")
            .sender
            .send(msg);
    }
}

impl<C> ClusterConnInner<C>
where
    C: ConnectionLike + Connect + Clone + Send + Sync + 'static,
{
    async fn new(
        initial_nodes: &[ConnectionInfo],
        cluster_params: ClusterParams,
    ) -> RedisResult<Self> {
        let connections =
            Self::create_initial_connections(initial_nodes, cluster_params.clone()).await?;
        let mut connection = ClusterConnInner {
            connections,
            slots: Default::default(),
            in_flight_requests: Default::default(),
            refresh_error: None,
            pending_requests: Vec::new(),
            state: ConnectionState::PollComplete,
            cluster_params,
        };
        let (slots, connections) = connection.refresh_slots().await.map_err(|(err, _)| err)?;
        connection.slots = slots;
        connection.connections = connections;
        Ok(connection)
    }

    async fn create_initial_connections(
        initial_nodes: &[ConnectionInfo],
        params: ClusterParams,
    ) -> RedisResult<ConnectionMap<C>> {
        let connections = stream::iter(initial_nodes.iter().cloned())
            .map(|info| {
                let params = params.clone();
                async move {
                    let addr = info.addr.to_string();
                    let result = connect_and_check(&addr, params).await;
                    match result {
                        Ok(conn) => Some((addr, async { conn }.boxed().shared())),
                        Err(e) => {
                            trace!("Failed to connect to initial node: {:?}", e);
                            None
                        }
                    }
                }
            })
            .buffer_unordered(initial_nodes.len())
            .fold(
                HashMap::with_capacity(initial_nodes.len()),
                |mut connections: ConnectionMap<C>, conn| async move {
                    connections.extend(conn);
                    connections
                },
            )
            .await;
        if connections.is_empty() {
            return Err(RedisError::from((
                ErrorKind::IoError,
                "Failed to create initial connections",
            )));
        }
        Ok(connections)
    }

    fn refresh_connections(
        &mut self,
        addrs: Option<Vec<String>>,
    ) -> impl Future<Output = ConnectionMap<C>> {
        let mut connections = mem::take(&mut self.connections);
        let cluster_params = self.cluster_params.clone();
        let addrs = addrs.unwrap_or_else(|| {
            let mut addrs = self.slots.values().flatten().collect::<Vec<_>>();
            addrs.sort_unstable();
            addrs.dedup();
            addrs.into_iter().map(|a| a.clone()).collect()
        });

        async move {
            for addr in addrs {
                if !connections.contains_key(&addr) {
                    let new_connection = if let Some(conn) = connections.remove(&addr) {
                        let mut conn = conn.await;
                        match check_connection(&mut conn).await {
                            Ok(_) => Some(conn),
                            Err(_) => {
                                match connect_and_check(&addr, cluster_params.clone()).await {
                                    Ok(conn) => Some(conn),
                                    Err(_) => None,
                                }
                            }
                        }
                    } else {
                        match connect_and_check(&addr, cluster_params.clone()).await {
                            Ok(conn) => Some(conn),
                            Err(_) => None,
                        }
                    };
                    if let Some(new_connection) = new_connection {
                        connections.insert(addr, async { new_connection }.boxed().shared());
                    }
                }
            }
            connections
        }
    }

    // Query a node to discover slot-> master mappings.
    fn refresh_slots(
        &mut self,
    ) -> impl Future<Output = Result<(SlotMap, ConnectionMap<C>), (RedisError, ConnectionMap<C>)>>
    {
        let mut connections = mem::take(&mut self.connections);
        let cluster_params = self.cluster_params.clone();
        let new_connections = self.refresh_connections(None);

        async move {
            let mut result = Ok(SlotMap::new());
            for (_, conn) in connections.iter_mut() {
                let mut conn = conn.clone().await;
                let value = match conn.req_packed_command(&slot_cmd()).await {
                    Ok(value) => value,
                    Err(err) => {
                        result = Err(err);
                        continue;
                    }
                };
                match parse_slots(value, cluster_params.tls)
                    .and_then(|v| Self::build_slot_map(v, cluster_params.read_from_replicas))
                {
                    Ok(s) => {
                        result = Ok(s);
                        break;
                    }
                    Err(err) => result = Err(err),
                }
            }
            let slots = match result {
                Ok(slots) => slots,
                Err(err) => return Err((err, connections)),
            };

            Ok((slots, new_connections.await))
        }
    }

    fn build_slot_map(mut slots_data: Vec<Slot>, read_from_replicas: bool) -> RedisResult<SlotMap> {
        slots_data.sort_by_key(|slot_data| slot_data.start());
        let last_slot = slots_data.iter().try_fold(0, |prev_end, slot_data| {
            if prev_end != slot_data.start() {
                return Err(RedisError::from((
                    ErrorKind::ResponseError,
                    "Slot refresh error.",
                    format!(
                        "Received overlapping slots {} and {}..{}",
                        prev_end,
                        slot_data.start(),
                        slot_data.end()
                    ),
                )));
            }
            Ok(slot_data.end() + 1)
        })?;

        if usize::from(last_slot) != SLOT_SIZE {
            return Err(RedisError::from((
                ErrorKind::ResponseError,
                "Slot refresh error.",
                format!("Lacks the slots >= {last_slot}"),
            )));
        }
        let slot_map = slots_data
            .iter()
            .map(|slot| (slot.end(), SlotAddrs::from_slot(slot, read_from_replicas)))
            .collect();
        trace!("{:?}", slot_map);
        Ok(slot_map)
    }

    fn get_connection(&mut self, route: &Route) -> Option<(String, ConnectionFuture<C>)> {
        if let Some((_, node_addrs)) = self.slots.range(&route.slot()..).next() {
            let addr = node_addrs.slot_addr(route.slot_addr()).to_string();
            if let Some(conn) = self.connections.get(&addr) {
                return Some((addr, conn.clone()));
            }
        }

        None
        //     // Create new connection.
        //     //

        //     // FIXME THIS IS FUCKED:
        //     let (_, random_conn) = get_random_connection(&self.connections)?; // TODO Only do this lookup if the first check fails

        //     let connection_future = {
        //         let addr = addr.clone();
        //         let params = self.cluster_params.clone();
        //         async move {
        //             match connect_and_check(&addr, params).await {
        //                 Ok(conn) => conn,
        //                 Err(_) => random_conn.await,
        //             }
        //         }
        //     }
        //     .boxed()
        //     .shared();

        //     self.connections
        //         .insert(addr.clone(), connection_future.clone());

        //     Some((addr, connection_future))

        // } else {
        //     // Return a random connection
        //     get_random_connection(&self.connections)
        // }
    }

    fn try_request(
        &mut self,
        info: &RequestInfo<C>,
    ) -> impl Future<Output = (Option<String>, RedisResult<Response>)> {
        // TODO remove clone by changing the ConnectionLike trait
        let cmd = info.cmd.clone();
        let addr_conn_option = if info.route.is_none() {
            get_random_connection(&self.connections)
        } else {
            self.get_connection(info.route.as_ref().unwrap())
        };
        async move {
            let (addr, conn) = match addr_conn_option {
                Some((addr, conn)) => (Some(addr), conn),
                None => {
                    return (
                        None,
                        Err(RedisError::from((
                            ErrorKind::ClusterDown,
                            "Unable to obtain connection",
                        ))),
                    );
                }
            };
            let conn = conn.await;
            let result = cmd.exec(conn).await;
            (addr, result)
        }
    }

    fn poll_recover(
        &mut self,
        cx: &mut task::Context<'_>,
        mut future: RecoverFuture<C>,
    ) -> Poll<Result<(), RedisError>> {
        match future {
            RecoverFuture::RecoverSlots(mut future) => match future.as_mut().poll(cx) {
                Poll::Ready(Ok((slots, connections))) => {
                    trace!("Recovered with {} connections!", connections.len());
                    self.slots = slots;
                    self.connections = connections;
                    self.state = ConnectionState::PollComplete;
                    Poll::Ready(Ok(()))
                }
                Poll::Pending => {
                    self.state = ConnectionState::Recover(RecoverFuture::RecoverSlots((future)));
                    trace!("Recover not ready");
                    Poll::Pending
                }
                Poll::Ready(Err((err, connections))) => {
                    self.connections = connections;
                    self.state = ConnectionState::Recover(RecoverFuture::RecoverSlots(Box::pin(
                        self.refresh_slots(),
                    )));
                    Poll::Ready(Err(err))
                }
            },
            RecoverFuture::RecoverConns(mut future) => match future.as_mut().poll(cx) {
                Poll::Ready(connections) => {
                    trace!("Reestablished connections!");
                    self.connections = connections;
                    self.state = ConnectionState::PollComplete;
                    Poll::Ready(Ok(()))
                }
                Poll::Pending => {
                    self.state = ConnectionState::Recover(RecoverFuture::RecoverConns(future));
                    trace!("Recover not ready");
                    Poll::Pending
                }
            },
        }
    }

    fn poll_complete(&mut self, cx: &mut task::Context<'_>) -> Poll<PollFlushAction> {
        let mut poll_flush_action = PollFlushAction::None;

        if !self.pending_requests.is_empty() {
            let mut pending_requests = mem::take(&mut self.pending_requests);
            for request in pending_requests.drain(..) {
                // Drop the request if noone is waiting for a response to free up resources for
                // requests callers care about (load shedding). It will be ambigous whether the
                // request actually goes through regardless.
                if request.sender.is_closed() {
                    continue;
                }

                let future = self.try_request(&request.info);
                self.in_flight_requests.push(Box::pin(Request {
                    max_retries: self.cluster_params.retries,
                    request: Some(request),
                    future: RequestState::Future {
                        future: future.boxed(),
                    },
                }));
            }
            self.pending_requests = pending_requests;
        }

        loop {
            let result = match Pin::new(&mut self.in_flight_requests).poll_next(cx) {
                Poll::Ready(Some(result)) => result,
                Poll::Ready(None) | Poll::Pending => break,
            };
            let self_ = &mut *self;
            match result {
                Next::Done => {}
                Next::TryAgain { request } => {
                    let future = self.try_request(&request.info);
                    self.in_flight_requests.push(Box::pin(Request {
                        max_retries: self.cluster_params.retries,
                        request: Some(request),
                        future: RequestState::Future {
                            future: Box::pin(future),
                        },
                    }));
                }
                Next::RefreshSlots { request, error } => {
                    poll_flush_action =
                        poll_flush_action.change_state(PollFlushAction::RebuildSlots(error));
                    self.pending_requests.push(request);
                }
                Next::ReestablishConnection {
                    request,
                    addr,
                    error,
                } => {
                    poll_flush_action = poll_flush_action
                        .change_state(PollFlushAction::ReestablishConnection(vec![addr], error));

                    self.pending_requests.push(request);
                }
            }
        }

        if self.in_flight_requests.is_empty() {
            Poll::Ready(poll_flush_action)
        } else {
            Poll::Pending
        }
    }

    fn send_refresh_error(&mut self) {
        if self.refresh_error.is_some() {
            if let Some(mut request) = Pin::new(&mut self.in_flight_requests)
                .iter_pin_mut()
                .find(|request| request.request.is_some())
            {
                (*request)
                    .as_mut()
                    .respond(Err(self.refresh_error.take().unwrap()));
            } else if let Some(request) = self.pending_requests.pop() {
                let _ = request.sender.send(Err(self.refresh_error.take().unwrap()));
            }
        }
    }
}

enum PollFlushAction {
    None,
    RebuildSlots(RedisError),
    ReestablishConnection(Vec<String>, RedisError),
}

impl PollFlushAction {
    fn change_state(self, next_state: PollFlushAction) -> PollFlushAction {
        match self {
            PollFlushAction::None => next_state,
            PollFlushAction::RebuildSlots(e) => PollFlushAction::RebuildSlots(e),
            PollFlushAction::ReestablishConnection(mut addrs, _) => match next_state {
                PollFlushAction::RebuildSlots(e) => PollFlushAction::RebuildSlots(e),
                PollFlushAction::ReestablishConnection(new_addrs, error) => {
                    addrs.extend(new_addrs);
                    PollFlushAction::ReestablishConnection(addrs, error)
                }
                PollFlushAction::None => next_state,
            },
        }
    }
}

impl<C> Sink<Message<C>> for ClusterConnInner<C>
where
    C: ConnectionLike + Connect + Clone + Send + Sync + Unpin + 'static,
{
    type Error = ();

    fn poll_ready(
        mut self: Pin<&mut Self>,
        cx: &mut task::Context,
    ) -> Poll<Result<(), Self::Error>> {
        match mem::replace(&mut self.state, ConnectionState::PollComplete) {
            ConnectionState::PollComplete => Poll::Ready(Ok(())),
            ConnectionState::Recover(future) => {
                match ready!(self.as_mut().poll_recover(cx, future)) {
                    Ok(()) => Poll::Ready(Ok(())),
                    Err(err) => {
                        // We failed to reconnect, while we will try again we will report the
                        // error if we can to avoid getting trapped in an infinite loop of
                        // trying to reconnect
                        if let Some(mut request) = Pin::new(&mut self.in_flight_requests)
                            .iter_pin_mut()
                            .find(|request| request.request.is_some())
                        {
                            (*request).as_mut().respond(Err(err));
                        } else {
                            self.refresh_error = Some(err);
                        }
                        Poll::Ready(Ok(()))
                    }
                }
            }
        }
    }

    fn start_send(mut self: Pin<&mut Self>, msg: Message<C>) -> Result<(), Self::Error> {
        trace!("start_send");
        let Message { cmd, sender } = msg;

        let route = cmd.route();

        let info = RequestInfo { cmd, route };

        self.pending_requests.push(PendingRequest {
            retry: 0,
            sender,
            info,
        });
        Ok(())
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut task::Context,
    ) -> Poll<Result<(), Self::Error>> {
        trace!("poll_complete: {:?}", self.state);
        loop {
            self.send_refresh_error();

            match mem::replace(&mut self.state, ConnectionState::PollComplete) {
                ConnectionState::Recover(future) => {
                    match ready!(self.as_mut().poll_recover(cx, future)) {
                        Ok(()) => (),
                        Err(err) => {
                            // We failed to reconnect, while we will try again we will report the
                            // error if we can to avoid getting trapped in an infinite loop of
                            // trying to reconnect
                            self.refresh_error = Some(err);

                            // Give other tasks a chance to progress before we try to recover
                            // again. Since the future may not have registered a wake up we do so
                            // now so the task is not forgotten
                            cx.waker().wake_by_ref();
                            return Poll::Pending;
                        }
                    }
                }
                ConnectionState::PollComplete => match ready!(self.poll_complete(cx)) {
                    PollFlushAction::None => return Poll::Ready(Ok(())),
                    PollFlushAction::RebuildSlots(err) => {
                        trace!("Rebuilding slots {}", err);
                        // self.state = ConnectionState::Recover(Box::pin(async { let a = self.refresh_slots().await?; Ok(Some(a)) }));
                    }
                    PollFlushAction::ReestablishConnection(addrs, err) => {
                        trace!("Reestablishing conns {}", err);
                        // let future = {
                        //     let mut connections = mem::take(&mut self.connections);
                        //     let cluster_params = self.cluster_params.clone();

                        // })};
                        // self.state = ConnectionState::Recover(future);
                    }
                },
            }
        }
    }

    fn poll_close(
        mut self: Pin<&mut Self>,
        cx: &mut task::Context,
    ) -> Poll<Result<(), Self::Error>> {
        // Try to drive any in flight requests to completion
        match self.poll_complete(cx) {
            Poll::Ready(poll_flush_action) => match poll_flush_action {
                PollFlushAction::None => (),
                PollFlushAction::RebuildSlots(err) => Err(err).map_err(|_| ())?,
                PollFlushAction::ReestablishConnection(_, err) => Err(err).map_err(|_| ())?,
            },
            Poll::Pending => (),
        };
        // If we no longer have any requests in flight we are done (skips any reconnection
        // attempts)
        if self.in_flight_requests.is_empty() {
            return Poll::Ready(Ok(()));
        }

        self.poll_flush(cx)
    }
}

impl<C> ConnectionLike for ClusterConnection<C>
where
    C: ConnectionLike + Send + 'static,
{
    fn req_packed_command<'a>(&'a mut self, cmd: &'a Cmd) -> RedisFuture<'a, Value> {
        trace!("req_packed_command");
        let (sender, receiver) = oneshot::channel();
        Box::pin(async move {
            self.0
                .send(Message {
                    cmd: CmdArg::Cmd {
                        cmd: Arc::new(cmd.clone()), // TODO Remove this clone?
                        func: |mut conn, cmd| {
                            Box::pin(async move {
                                conn.req_packed_command(&cmd).await.map(Response::Single)
                            })
                        },
                    },
                    sender,
                })
                .await
                .map_err(|_| {
                    RedisError::from(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "redis_cluster: Unable to send command",
                    ))
                })?;
            receiver
                .await
                .unwrap_or_else(|_| {
                    Err(RedisError::from(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "redis_cluster: Unable to receive command",
                    )))
                })
                .map(|response| match response {
                    Response::Single(value) => value,
                    Response::Multiple(_) => unreachable!(),
                })
        })
    }

    fn req_packed_commands<'a>(
        &'a mut self,
        pipeline: &'a crate::Pipeline,
        offset: usize,
        count: usize,
    ) -> RedisFuture<'a, Vec<Value>> {
        let (sender, receiver) = oneshot::channel();
        Box::pin(async move {
            self.0
                .send(Message {
                    cmd: CmdArg::Pipeline {
                        pipeline: Arc::new(pipeline.clone()), // TODO Remove this clone?
                        offset,
                        count,
                        func: |mut conn, pipeline, offset, count| {
                            Box::pin(async move {
                                conn.req_packed_commands(&pipeline, offset, count)
                                    .await
                                    .map(Response::Multiple)
                            })
                        },
                    },
                    sender,
                })
                .await
                .map_err(|_| RedisError::from(io::Error::from(io::ErrorKind::BrokenPipe)))?;

            receiver
                .await
                .unwrap_or_else(|_| {
                    Err(RedisError::from(io::Error::from(io::ErrorKind::BrokenPipe)))
                })
                .map(|response| match response {
                    Response::Multiple(values) => values,
                    Response::Single(_) => unreachable!(),
                })
        })
    }

    fn get_db(&self) -> i64 {
        0
    }
}
/// Implements the process of connecting to a Redis server
/// and obtaining a connection handle.
pub trait Connect: Sized {
    /// Connect to a node, returning handle for command execution.
    fn connect<'a, T>(info: T) -> RedisFuture<'a, Self>
    where
        T: IntoConnectionInfo + Send + 'a;
}

impl Connect for MultiplexedConnection {
    fn connect<'a, T>(info: T) -> RedisFuture<'a, MultiplexedConnection>
    where
        T: IntoConnectionInfo + Send + 'a,
    {
        async move {
            let connection_info = info.into_connection_info()?;
            let client = crate::Client::open(connection_info)?;

            #[cfg(feature = "tokio-comp")]
            return client.get_multiplexed_tokio_connection().await;

            #[cfg(all(not(feature = "tokio-comp"), feature = "async-std-comp"))]
            return client.get_multiplexed_async_std_connection().await;
        }
        .boxed()
    }
}

async fn connect_and_check<C>(node: &str, params: ClusterParams) -> RedisResult<C>
where
    C: ConnectionLike + Connect + Send + 'static,
{
    let read_from_replicas = params.read_from_replicas;
    let info = get_connection_info(node, params)?;
    let mut conn = C::connect(info).await?;
    check_connection(&mut conn).await?;
    if read_from_replicas {
        // If READONLY is sent to primary nodes, it will have no effect
        crate::cmd("READONLY").query_async(&mut conn).await?;
    }
    Ok(conn)
}

async fn check_connection<C>(conn: &mut C) -> RedisResult<()>
where
    C: ConnectionLike + Send + 'static,
{
    let mut cmd = Cmd::new();
    cmd.arg("PING");
    cmd.query_async::<_, String>(conn).await?;
    Ok(())
}

fn get_random_connection<'a, C>(
    connections: &'a ConnectionMap<C>,
) -> Option<(String, ConnectionFuture<C>)>
where
    C: Clone,
{
    let addr = connections.keys().choose(&mut thread_rng())?.to_string();
    let conn = connections.get(&addr)?.clone();
    Some((addr, conn))
}
