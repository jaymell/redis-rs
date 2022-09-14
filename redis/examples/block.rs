use redis::streams::{StreamReadOptions, StreamKey, StreamReadReply};
use redis::{self, transaction, Commands, Cmd};

use std::collections::HashMap;
use std::env;

/// This function demonstrates how a return value can be coerced into a
/// hashmap of tuples.  This is particularly useful for responses like
/// CONFIG GET or all most H functions which will return responses in
/// such list of implied tuples.
fn do_print_max_entry_limits(con: &mut redis::Connection) -> redis::RedisResult<()> {
    // since rust cannot know what format we actually want we need to be
    // explicit here and define the type of our response.  In this case
    // String -> int fits all the items we query for.
    let config: HashMap<String, isize> = redis::cmd("CONFIG")
        .arg("GET")
        .arg("*-max-*-entries")
        .query(con)?;

    println!("Max entry limits:");

    println!(
        "  max-intset:        {}",
        config.get("set-max-intset-entries").unwrap_or(&0)
    );
    println!(
        "  hash-max-ziplist:  {}",
        config.get("hash-max-ziplist-entries").unwrap_or(&0)
    );
    println!(
        "  list-max-ziplist:  {}",
        config.get("list-max-ziplist-entries").unwrap_or(&0)
    );
    println!(
        "  zset-max-ziplist:  {}",
        config.get("zset-max-ziplist-entries").unwrap_or(&0)
    );

    Ok(())
}

/// This is a pretty stupid example that demonstrates how to create a large
/// set through a pipeline and how to iterate over it through implied
/// cursors.
fn do_show_scanning(con: &mut redis::Connection) -> redis::RedisResult<()> {
    // This makes a large pipeline of commands.  Because the pipeline is
    // modified in place we can just ignore the return value upon the end
    // of each iteration.
    let mut pipe = redis::pipe();
    for num in 0..1000 {
        pipe.cmd("SADD").arg("my_set").arg(num).ignore();
    }

    // since we don't care about the return value of the pipeline we can
    // just cast it into the unit type.
    pipe.query(con)?;

    // since rust currently does not track temporaries for us, we need to
    // store it in a local variable.
    let mut cmd = redis::cmd("SSCAN");
    cmd.arg("my_set").cursor_arg(0);

    // as a simple exercise we just sum up the iterator.  Since the fold
    // method carries an initial value we do not need to define the
    // type of the iterator, rust will figure "int" out for us.
    let sum: i32 = cmd.iter::<i32>(con)?.sum();

    println!("The sum of all numbers in the set 0-1000: {}", sum);

    Ok(())
}

/// Demonstrates how to do an atomic increment in a very low level way.
fn do_atomic_increment_lowlevel(con: &mut redis::Connection) -> redis::RedisResult<()> {
    let key = "the_key";
    println!("Run low-level atomic increment:");

    // set the initial value so we have something to test with.
    redis::cmd("SET").arg(key).arg(42).query(con)?;

    loop {
        // we need to start watching the key we care about, so that our
        // exec fails if the key changes.
        redis::cmd("WATCH").arg(key).query(con)?;

        // load the old value, so we know what to increment.
        let val: isize = redis::cmd("GET").arg(key).query(con)?;

        // at this point we can go into an atomic pipe (a multi block)
        // and set up the keys.
        let response: Option<(isize,)> = redis::pipe()
            .atomic()
            .cmd("SET")
            .arg(key)
            .arg(val + 1)
            .ignore()
            .cmd("GET")
            .arg(key)
            .query(con)?;

        match response {
            None => {
                continue;
            }
            Some(response) => {
                let (new_val,) = response;
                println!("  New value: {}", new_val);
                break;
            }
        }
    }

    Ok(())
}

/// Demonstrates how to do an atomic increment with transaction support.
fn do_atomic_increment(con: &mut redis::Connection) -> redis::RedisResult<()> {
    let key = "the_key";
    println!("Run high-level atomic increment:");

    // set the initial value so we have something to test with.
    con.set(key, 42)?;

    // run the transaction block.
    let (new_val,): (isize,) = transaction(con, &[key], |con, pipe| {
        // load the old value, so we know what to increment.
        let val: isize = con.get(key)?;
        // increment
        pipe.set(key, val + 1).ignore().get(key).query(con)
    })?;

    // and print the result
    println!("New value: {}", new_val);

    Ok(())
}

/// Runs all the examples and propagates errors up.
fn do_redis_code(url: &str) -> redis::RedisResult<()> {
    // general connection handling
    let client = redis::Client::open(url)?;
    let mut con = client.get_connection()?;

    // read some config and print it.
    do_print_max_entry_limits(&mut con)?;

    // demonstrate how scanning works.
    do_show_scanning(&mut con)?;

    // shows an atomic increment.
    do_atomic_increment_lowlevel(&mut con)?;
    do_atomic_increment(&mut con)?;

    Ok(())
}

const WORKERS_GROUP: &str = "test-group";
/// Consumer group consumer name constant -- consumer groups contain consumers which receive
/// messages in a round-robin manner. Every worker uses the same consumer name such that they race
/// for messages instead of having them evenly distributed.
const WORKER_CONSUMER: &str = "test-consumer";


fn main() {
    // at this point the errors are fatal, let's just fail hard.
    let url = 
        "redis://127.0.0.1:6379/";

    let client = redis::Client::open(url).unwrap();
    let mut con = client.get_connection().unwrap();
    con.set_read_timeout(Some(std::time::Duration::from_secs(1))).unwrap();

    let cmd = Cmd::xread_options(
        &["test-queue" ],
        &[">"],
        &StreamReadOptions::default()
            .group(WORKERS_GROUP, WORKER_CONSUMER)
            .count(1)
            .block(500),
    );
    let k: StreamReadReply = cmd.query(&mut con).unwrap();
    println!("{:?}", k);

}
