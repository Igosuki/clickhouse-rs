extern crate clickhouse_rs;
extern crate futures;

use std::env;
use std::error::Error;

use clickhouse_rs::{Pool, types::{Block}};

async fn execute(database_url: String) -> Result<(), Box<dyn Error>> {
    env::set_var("RUST_LOG", "clickhouse_rs=debug");
    env_logger::init();

    let pool = Pool::new(database_url);

    let ddl = "
        CREATE TABLE array_table (
            nums Array(UInt32),
            text Array(String)
        ) Engine=Memory";

    let query = "SELECT nums, text FROM array_table";

    let block = Block::new()
        .column("nums", vec![vec![1_u32, 2, 3], vec![4, 5, 6]])
        .column("text", vec![vec!["A", "B", "C"], vec!["D", "E"]]);

    let mut c = pool.get_handle().await?;
    c.execute("DROP TABLE IF EXISTS array_table").await?;
    c.execute(ddl).await?;
    c.insert("array_table", block).await?;
    let block = c.query(query).fetch_all().await?;
    for row in block.rows() {
        let nums: Vec<u32> = row.get("nums")?;
        let text: Vec<&str> = row.get("text")?;
        println!("{:?},\t{:?}", nums, text);
    }

    Ok(())
}

#[cfg(all(feature = "tokio_io"))]
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let database_url =
        env::var("DATABASE_URL").unwrap_or_else(|_| util::DATABASE_URL.into());
    execute(database_url).await
}

#[cfg(feature = "async_std")]
fn main() {
    use async_std::task;
    let database_url =
        env::var("DATABASE_URL").unwrap_or_else(|_| "tcp://localhost:9000?compression=lz4".into());
    task::block_on(execute(database_url)).unwrap();
}

