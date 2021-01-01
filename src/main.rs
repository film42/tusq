pub mod config;
pub mod core;
pub mod pool;
pub mod proto;

use config::Config;
use pool::PgPooler;
use std::net::SocketAddr;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let bind_addr = "127.0.0.1:8432".parse::<SocketAddr>()?;
    println!("Listening on: {:?}", bind_addr);
    let listener = TcpListener::bind(bind_addr).await?;
    let config = Config::example();
    let pooler = PgPooler::new(config.clone());

    loop {
        let (client_conn, _) = listener.accept().await?;
        let client_info = format!("{:?}", client_conn);
        println!("Client connected: {:?}", client_info);
        tokio::spawn({
            // Build the client pgconn.
            let mut client_conn = core::PgConn::new(client_conn);

            //  // Build a db pool (unique per conn for now).
            //  let mut server_pool = pool::PgConnPool::new(config.clone());
            let pooler = pooler.clone();

            // Start the show.
            async move {
                // Parse the startup flow.
                let server_pool = match client_conn.handle_startup(pooler).await {
                    Ok(sm) => {
                        println!(
                            "Client established and ready for query: {:?}, startup: {:?}",
                            client_info, sm
                        );
                        sm
                    }
                    Err(err) => {
                        println!(
                            "Client closed with error: {:?}, conn: {:?}",
                            err, client_info
                        );
                        return;
                    }
                };

                // Run the txn loop.
                match core::spawn(client_conn, server_pool).await {
                    Ok(_) => println!("Client closed: {:?}", client_info),
                    Err(err) => println!(
                        "Client closed with error: {:?}, conn: {:?}",
                        err, client_info
                    ),
                }
            }
        });
    }
}
