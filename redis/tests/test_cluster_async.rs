#![cfg(feature = "cluster-async")]
mod support;
use redis::{cmd, AsyncCommands, RedisError, Script};
use tokio::sync::MutexGuard;
use once_cell::sync::Lazy,

use crate::support::*;

pub struct RedisProcess;
pub struct RedisLock(MutexGuard<'static, RedisProcess>);

impl RedisProcess {
    // Blocks until we have sole access.
    pub fn lock() -> RedisLock {
        static REDIS: Lazy<Mutex<RedisProcess>> = Lazy::new(|| Mutex::new(RedisProcess {}));

        // If we panic in a test we don't want subsequent to fail because of a poisoned error
        let redis_lock = REDIS
            .lock()
            .unwrap_or_else(|poison_error| poison_error.into_inner());
        RedisLock(redis_lock)
    }
}

#[test]
fn basic_cmd() {
    let cluster = TestClusterContext::new(3, 0);

    block_on_all(async move {
        let mut connection = cluster.async_connection().await;
        let () = cmd("SET")
            .arg("test")
            .arg("test_data")
            .query_async(&mut connection)
            .await?;
        let res: String = cmd("GET")
            .arg("test")
            .clone()
            .query_async(&mut connection)
            .await?;
        assert_eq!(res, "test_data");
        Ok(())
    })
    .map_err(|err: RedisError| err)
    .unwrap();
}

#[test]
fn basic_eval() {
    let cluster = TestClusterContext::new(3, 0);

    block_on_all(async move {
        let mut connection = cluster.async_connection().await;
        let res: String = cmd("EVAL")
            .arg(r#"redis.call("SET", KEYS[1], ARGV[1]); return redis.call("GET", KEYS[1])"#)
            .arg(1)
            .arg("key")
            .arg("test")
            .query_async(&mut connection)
            .await?;
        assert_eq!(res, "test");
        Ok(())
    })
    .map_err(|err: RedisError| err)
    .unwrap();
}

#[ignore] // TODO Handle running SCRIPT LOAD on all masters
#[test]
fn basic_script() {
    let cluster = TestClusterContext::new(3, 0);

    block_on_all(async move {
        let mut connection = cluster.async_connection().await;
        let res: String = Script::new(
            r#"redis.call("SET", KEYS[1], ARGV[1]); return redis.call("GET", KEYS[1])"#,
        )
        .key("key")
        .arg("test")
        .invoke_async(&mut connection)
        .await?;
        assert_eq!(res, "test");
        Ok(())
    })
    .map_err(|err: RedisError| err)
    .unwrap();
}

#[ignore] // TODO Handle pipe where the keys do not all go to the same node
#[test]
fn basic_pipe() {
    let cluster = TestClusterContext::new(3, 0);

    block_on_all(async move {
        let mut connection = cluster.async_connection().await;
        let mut pipe = redis::pipe();
        pipe.add_command(cmd("SET").arg("test").arg("test_data").clone());
        pipe.add_command(cmd("SET").arg("test3").arg("test_data3").clone());
        let () = pipe.query_async(&mut connection).await?;
        let res: String = connection.get("test").await?;
        assert_eq!(res, "test_data");
        let res: String = connection.get("test3").await?;
        assert_eq!(res, "test_data3");
        Ok(())
    })
    .map_err(|err: RedisError| err)
    .unwrap()
}

#[test]
fn proptests() {
    let env = std::cell::RefCell::new(FailoverEnv::new());

    proptest!(
        proptest::prelude::ProptestConfig { cases: 30, failure_persistence: None, .. Default::default() },
        |(requests in 0..15, value in 0..i32::max_value())| {
            test_failover(&mut env.borrow_mut(), requests, value)
        }
    );
}

#[test]
fn basic_failover() {
    test_failover(&mut FailoverEnv::new(), 10, 123);
}

struct FailoverEnv {
    env: RuntimeEnv,
    connection: redis_cluster_async::Connection,
}

impl FailoverEnv {
    fn new() -> Self {
        let env = RuntimeEnv::new();
        let connection = env
            .runtime
            .block_on(env.redis.client.get_connection())
            .unwrap();

        FailoverEnv { env, connection }
    }
}

async fn do_failover(redis: &mut redis::aio::MultiplexedConnection) -> Result<(), anyhow::Error> {
    cmd("CLUSTER").arg("FAILOVER").query_async(redis).await?;
    Ok(())
}

fn test_failover(env: &mut FailoverEnv, requests: i32, value: i32) {
    let completed = Cell::new(0);
    let completed = &completed;

    let FailoverEnv { env, connection } = env;

    let nodes = env.redis.nodes.clone();

    let test_future = async {
        (0..requests + 1)
            .map(|i| {
                let mut connection = connection.clone();
                let mut nodes = nodes.clone();
                async move {
                    if i == requests / 2 {
                        // Failover all the nodes, error only if all the failover requests error
                        nodes
                            .iter_mut()
                            .map(|node| do_failover(node))
                            .collect::<stream::FuturesUnordered<_>>()
                            .fold(
                                Err(anyhow::anyhow!("None")),
                                |acc: Result<(), _>, result: Result<(), _>| async move {
                                    acc.or_else(|_| result)
                                },
                            )
                            .await
                    } else {
                        let key = format!("test-{}-{}", value, i);
                        let () = cmd("SET")
                            .arg(&key)
                            .arg(i)
                            .clone()
                            .query_async(&mut connection)
                            .await?;
                        let res: i32 = cmd("GET")
                            .arg(key)
                            .clone()
                            .query_async(&mut connection)
                            .await?;
                        assert_eq!(res, i);
                        completed.set(completed.get() + 1);
                        Ok::<_, anyhow::Error>(())
                    }
                }
            })
            .collect::<stream::FuturesUnordered<_>>()
            .try_collect()
            .await
    };
    env.runtime
        .block_on(test_future)
        .unwrap_or_else(|err| panic!("{}", err));
    assert_eq!(completed.get(), requests, "Some requests never completed!");
}
