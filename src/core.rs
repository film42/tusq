use crate::pool::PgConnPool;
use crate::proto::{messages, ProtoMessage, ProtoParser, StartupMessage};
use bytes::BytesMut;
use futures::future::select;
use futures::future::Either;
use net::write_all_with_timeout;
use std::collections::VecDeque;
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;

enum Op {
    CopyFromClientToServer(usize),
    CopyFromServerToClient(usize),
}

pub struct PgConn {
    pub(crate) conn: TcpStream,
    parser: ProtoParser,
    pub(crate) buffer: BytesMut,
    pub(crate) msgs: VecDeque<ProtoMessage>,
    buffer_prefix_offset: usize,
}

impl PgConn {
    pub fn new(conn: TcpStream) -> Self {
        let mut buffer = BytesMut::with_capacity(8096);
        buffer.resize(8096, 0);

        Self {
            conn,
            buffer,
            parser: ProtoParser::new(),
            msgs: VecDeque::new(),
            buffer_prefix_offset: 0,
        }
    }

    pub async fn write_auth_ok(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let msg = messages::auth_ok();
        write_all_with_timeout(&mut self.conn, &msg, None).await?;
        Ok(())
    }

    pub async fn write_ready_for_query(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let msg = messages::ready_for_query();
        write_all_with_timeout(&mut self.conn, &msg, None).await?;
        Ok(())
    }

    pub async fn handle_startup(&mut self) -> Result<StartupMessage, Box<dyn std::error::Error>> {
        let n = self.conn.read(&mut self.buffer).await?;

        // Parse startup message.
        let (n_parsed, startup_message) = self.parser.parse_startup(&mut self.buffer[..n])?;
        self.buffer_prefix_offset = n - n_parsed;
        let sm = startup_message.expect("todo: handle an incomplete startup message");

        // Finish auth stuff here.. should probably move later.
        self.write_auth_ok().await?;
        self.write_ready_for_query().await?;

        // Return original startup message.
        Ok(sm)
    }

    pub async fn read_and_parse(&mut self) -> Result<usize, Box<dyn std::error::Error>> {
        // Read but do not write over any un-parsed buffer.
        let n = self
            .conn
            .read(&mut self.buffer[self.buffer_prefix_offset..])
            .await?;

        // Parse and adjust the buffer prefix offset in case the buffer
        // included a small part of a message from the last read.
        let n_to_parse = self.buffer_prefix_offset + n;

        let n_parsed = self
            .parser
            .parse(&mut self.buffer[..n_to_parse], &mut self.msgs)?;
        self.buffer_prefix_offset = n_to_parse - n_parsed;

        // Return only the number of bytes pared.
        Ok(n_parsed)
    }
}

// Check out a server from the connection pool.
// STUB for now.
pub async fn checkout_server() -> Result<PgConn, Box<dyn std::error::Error>> {
    unimplemented!();
}

// Manage the entire client life-cycle.
pub async fn spawn(
    mut client_conn: PgConn,
    pool: &PgConnPool,
) -> Result<(), Box<dyn std::error::Error>> {
    // Outter transaction loop.
    loop {
        let n = client_conn.read_and_parse().await?;

        // Check to ensure it signals the beginning of a txn. Close otherwise.
        while let Some(msg) = client_conn.msgs.pop_front() {
            match msg.msg_type() {
                // We only check for complete or partial messages here. The point is to
                // detect the beginning of a transaction.
                'Q' => {}
                'X' => {
                    println!("Client sent close request. Closing connection.");
                    return Ok(());
                }
                msg_type => {
                    panic!(
                        "Client sent non-query or close command ({}). Closing connection.",
                        msg_type,
                    );
                }
            }
        }

        let mut server_conn = pool.checkout().await?;

        // Write those N bytes to the server.
        write_all_with_timeout(
            &mut server_conn.conn,
            &mut client_conn.buffer[..n],
            Some(std::time::Duration::from_secs(5)),
        )
        .await?;

        // Proxy between client and server until the client or server ends the txn.
        'transaction: loop {
            // Read from either socket and parse msgs.
            // We use an "op" here to avoid the annoying double-owned inside/ outside
            // the match / case clause.
            let op = match select(
                Box::pin(client_conn.read_and_parse()),
                Box::pin(server_conn.read_and_parse()),
            )
            .await
            {
                // Success case.
                Either::Left((Ok(client_n), _dropped_server_read)) => {
                    Op::CopyFromClientToServer(client_n)
                }
                Either::Right((Ok(server_n), _dropped_client_read)) => {
                    Op::CopyFromServerToClient(server_n)
                }

                // Error case.
                Either::Left((Err(err), _)) | Either::Right((Err(err), _)) => return Err(err),
            };

            // Copy all pending buffer from one to the other.
            match op {
                Op::CopyFromClientToServer(n) => {
                    write_all_with_timeout(
                        &mut server_conn.conn,
                        &mut client_conn.buffer[..n],
                        Some(std::time::Duration::from_secs(30)),
                    )
                    .await?;
                }
                Op::CopyFromServerToClient(n) => {
                    write_all_with_timeout(
                        &mut client_conn.conn,
                        &mut server_conn.buffer[..n],
                        None,
                    )
                    .await?;
                }
            };

            // Check protocol messages for changes.

            // Server Messages
            while let Some(msg) = server_conn.msgs.pop_front() {
                match msg.msg_type() {
                    'Z' => {
                        if let Some('I') = msg.transaction_type(&server_conn.buffer) {
                            println!("Transaction completed.");
                            break 'transaction;
                        }
                    }
                    'X' => {
                        println!("Server is closing the connection!");
                        panic!("Server is closing early");
                    }
                    _ => { /* Proxy and continue. */ }
                }
            }

            // Client Messages
            while let Some(msg) = client_conn.msgs.pop_front() {
                match msg.msg_type() {
                    'X' => {
                        println!("Client is closing the connection!");
                        panic!("Client is closing early");
                    }
                    _ => { /* Proxy and continue. */ }
                }
            }
        }
    }
}

pub mod net {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;
    use tokio::time;

    pub struct PgConn {}

    // Add helper function to handle a read with timeout.
    pub async fn read_or_timeout(
        conn: &mut TcpStream,
        buffer: &mut [u8],
        timeout: std::time::Duration,
    ) -> Result<Option<usize>, Box<dyn std::error::Error>> {
        match time::timeout(timeout, conn.read(buffer)).await {
            // Check for success or error from write.
            Ok(Ok(n)) => Ok(Some(n)),
            Ok(Err(err)) => Err(Box::new(err)),

            // Operation timed out.
            Err(_) => Ok(None),
        }
    }

    // Add helper function to handle a write with timeout.
    pub async fn write_all_with_timeout(
        conn: &mut TcpStream,
        buffer: &[u8],
        timeout: Option<std::time::Duration>,
    ) -> Result<Option<usize>, Box<dyn std::error::Error>> {
        if timeout.is_none() {
            conn.write_all(buffer).await?;
            return Ok(Some(buffer.len()));
        }

        let timeout = timeout.expect("never None");
        match time::timeout(timeout, conn.write_all(buffer)).await {
            // Check for success or error from write.
            Ok(Ok(_)) => Ok(Some(buffer.len())),
            Ok(Err(err)) => Err(Box::new(err)),

            // Operation timed out.
            Err(_) => Ok(None),
        }
    }
}