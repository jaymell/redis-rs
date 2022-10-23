#![cfg(feature = "cluster_async")]
mod support;
use crate::support::*;

#[tokio::test]
async fn basic_cmd() {
    let cluster = TestClusterContext::new(3, 0);

    async {
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
    }
    .await
    .map_err(|err: RedisError| err)
    .unwrap()
}
