extern crate clickhouse_rs;
extern crate futures;
use std::env;

mod util;

#[cfg(feature = "tls")]
async fn main(database_url: String) -> Result<(), Box<dyn Error>> {
    use futures::Future;
    use clickhouse_rs::Pool;

    env::set_var("RUST_LOG", "clickhouse_rs=debug");
    env_logger::init();

    let database_url = env::var("DATABASE_URL").unwrap_or_else(|_| {
        util::DATABASE_URL.into()
    });
    let pool = Pool::new(database_url);

    let done = pool.get_handle().await?;
    c.query("SELECT 1 AS Value").fetch_all();
    println!("{:?}", block);

    Ok(())
}

#[cfg(not(feature = "tls"))]
fn main() {
    env::set_var("RUST_LOG", "clickhouse_rs=debug");
    env_logger::init();

    panic!("Required ssl feature (cargo run --example tls --features tls)")
}


