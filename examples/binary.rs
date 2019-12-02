extern crate clickhouse_rs;
extern crate futures;

use std::env;

use clickhouse_rs::{types::Block, Pool};
use futures::Future;
use std::error::Error;

async fn execute(database_url: String) -> Result<(), Box<dyn Error>> {
    let ddl = "
        CREATE TABLE IF NOT EXISTS test_blob (
            text        String,
            fx_text     FixedString(4),
            opt_text    Nullable(String),
            fx_opt_text Nullable(FixedString(4))
        ) Engine=Memory";

    let block = Block::new()
        .column("text", vec![[0, 159, 146, 150].as_ref(), b"ABCD"])
        .column("fx_text", vec![b"ABCD".as_ref(), &[0, 159, 146, 150]])
        .column("opt_text", vec![Some(vec![0, 159, 146, 150]), None])
        .column("fx_opt_text", vec![None, Some(vec![0, 159, 146, 150])]);

    let database_url =
        env::var("DATABASE_URL").unwrap_or_else(|_| "tcp://localhost:9000?compression=lz4".into());
    let pool = Pool::new(database_url);

    let c = pool.get_handle();
    c.execute(ddl);
    c.insert("test_blob", block);
    c.query("SELECT text, fx_text, opt_text, fx_opt_text FROM test_blob").fetch_all();
    for row in block.rows() {
        let text: &[u8] = row.get("text")?;
        let fx_text: &[u8] = row.get("fx_text")?;
        let opt_text: Option<&[u8]> = row.get("opt_text")?;
        let fx_opt_text: Option<&[u8]> = row.get("fx_opt_text")?;
        println!(
            "{:?}\t{:?}\t{:?}\t{:?}",
            text, fx_text, opt_text, fx_opt_text
        );
    }

    Ok(())
}

#[cfg(all(feature = "tokio_io", not(feature = "tls")))]
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let database_url =
        env::var("DATABASE_URL").unwrap_or_else(|_| "tcp://localhost:9000?compression=lz4".into());
    execute(database_url).await
}

#[cfg(all(feature = "tokio_io", feature = "tls"))]
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let database_url = env::var("DATABASE_URL").unwrap_or_else(|_| {
        "tcp://localhost:9440?secure=true&skip_verify=true".into()
    });
    execute(database_url).await
}

#[cfg(feature = "async_std")]
fn main() {
    use async_std::task;
    let database_url =
        env::var("DATABASE_URL").unwrap_or_else(|_| "tcp://localhost:9000?compression=lz4".into());
    task::block_on(execute(database_url)).unwrap();
}
