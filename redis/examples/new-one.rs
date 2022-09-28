use redis::{AsyncCommands, Client};

#[tokio::main]
async fn main() {
    let client = redis::Client::open("redis://127.0.0.1").unwrap();
    let mut conn = client.get_async_connection().await.unwrap();

    let worker = async { let _: i32 = conn.publish("test", "test").await.unwrap(); };
}
