#![cfg(feature = "cluster-async")]
mod support;
use redis::{cmd, AsyncCommands, RedisError, Script};

use crate::support::*;

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
