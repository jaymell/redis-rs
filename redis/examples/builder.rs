use redis::{self, transaction, Commands, IntoConnectionInfo};

use std::collections::HashMap;
use std::env;

fn main() {
    let v: Vec<&str> = vec![&"redis://localhost:6379"];
    let builder = redis::cluster::ClusterClientBuilder::new(v);
    match builder.build() {
        Ok(_) => println!("Ok"),
        Err(e) => println!("Err"),
    };
}
