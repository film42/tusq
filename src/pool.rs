use crate::config::Config;
use crate::core::net::write_all_with_timeout;
use crate::core::PgConn;
use crate::proto::StartupMessage;
use std::net::SocketAddr;
use tokio::net::TcpStream;

#[derive(Debug)]
pub struct PgConnPool {
    config: Config,
}

impl PgConnPool {
    pub fn new(config: Config) -> Self {
        Self { config }
    }

    // Checks out a server.
    pub async fn checkout(
        &self,
        client_startup_message: &StartupMessage,
    ) -> Result<PgConn, Box<dyn std::error::Error>> {
        let dbname = client_startup_message
            .database_name()
            .expect("database was set");

        let database = self
            .config
            .databases
            .get(&dbname)
            .expect("database config to exist");

        let addr = format!(
            "{}:{}",
            database.get("host").expect("host was set"),
            database.get("port").expect("port was set")
        )
        .parse::<SocketAddr>()
        .expect("valid socket addr");

        println!("Connecting to database at: {:?}", addr);

        // Build the server startup_message.
        let mut startup_message = client_startup_message.clone();
        for (key, value) in database.iter() {
            let param_name = match key.as_str() {
                "user" => key.clone(),
                "dbname" => "database".to_string(),
                _ => {
                    println!("Found unknown database startup parameter: {:?}", &key);
                    continue;
                }
            };

            startup_message.parameters.insert(param_name, value.clone());
        }

        let conn = TcpStream::connect(addr).await?;
        let mut server_conn = PgConn::new(conn);

        // Send startup message.
        let msg = startup_message.as_bytes();
        write_all_with_timeout(&mut server_conn.conn, &msg, None).await?;

        // Grab server params and expect a ready for query message.
        loop {
            server_conn.read_and_parse().await?;
            while let Some(msg) = server_conn.msgs.pop_front() {
                match msg.msg_type() {
                    'Z' => {
                        if let Some('I') = msg.transaction_type(&server_conn.buffer) {
                            return Ok(server_conn);
                        }
                    }
                    'S' => {
                        if let Some((key, value)) = msg.server_parameter(&server_conn.buffer) {
                            server_conn.server_parameters.insert(key, value);
                        }
                    }
                    _ => { /* Ignore everything else. */ }
                }
            }
        }
    }
}
