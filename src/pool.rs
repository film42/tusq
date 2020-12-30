use crate::core::net::write_all_with_timeout;
use crate::core::PgConn;
use crate::proto::StartupMessage;
use std::net::SocketAddr;
use tokio::net::TcpStream;

#[derive(Debug)]
pub struct PgConnPool {
    startup_message: StartupMessage,
}

impl PgConnPool {
    pub fn new(startup_message: StartupMessage) -> Self {
        Self { startup_message }
    }

    // Checks out a server.
    // TODO: For now it's just a hard-code create new conn to a static host.
    pub async fn checkout(&self) -> Result<PgConn, Box<dyn std::error::Error>> {
        // Connect to server.
        let addr = "127.0.0.1:5432".parse::<SocketAddr>().unwrap();
        let conn = TcpStream::connect(addr).await?;
        let mut server_conn = PgConn::new(conn);

        // Send startup message.
        let msg = self.startup_message.as_bytes();
        write_all_with_timeout(&mut server_conn.conn, &msg, None).await?;

        // Throw things away until we get "ready for query".
        // NOTE: We should be storing startup parameters because they are required to
        // send to the client.
        loop {
            server_conn.read_and_parse().await?;
            while let Some(msg) = server_conn.msgs.pop_front() {
                match msg.msg_type() {
                    'Z' => {
                        if let Some('I') = msg.transaction_type(&server_conn.buffer) {
                            return Ok(server_conn);
                        }
                    }
                    m => { /* Ignore everything else. */ }
                }
            }
        }
    }
}
