use futures::stream::StreamExt;
use redis::{AsyncCommands, AsyncIter};

#[tokio::main]
async fn main() -> redis::RedisResult<()> {
    let client = redis::Client::open("redis://127.0.0.1/").unwrap();
    let mut con = client.get_multiplexed_async_connection().await?;

    con.set("async-key1", b"foo").await?;

    loop {
        match con.get::<&str, String>("async-key1").await {
            Ok(g) => assert_eq!(g, "foo"),
            Err(e) => println!("{}", e.to_string()),
        }
    }

    Ok(())
}
