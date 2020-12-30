pub mod core;
pub mod pool;
pub mod proto;

use std::net::SocketAddr;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let bind_addr = "127.0.0.1:8432".parse::<SocketAddr>().unwrap();
    println!("Listening on: {:?}", bind_addr);
    let listener = TcpListener::bind(bind_addr).await?;

    loop {
        let (client_conn, _) = listener.accept().await?;
        let client_info = format!("{:?}", client_conn);
        println!("Client connected: {:?}", client_info);
        tokio::spawn({
            // Build the client pgconn.
            let mut client_conn = core::PgConn::new(client_conn);

            // Start the show.
            async move {
                // Parse the startup flow.
                let startup_message = match client_conn.handle_startup().await {
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

                // TODO: Make this global and track state there. It's a stub for now.
                let server_pool = pool::PgConnPool::new(startup_message);

                // Run the txn loop.
                match core::spawn(client_conn, &server_pool).await {
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
