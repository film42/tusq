pub mod proto;

pub mod core {
    use crate::net::write_all_with_timeout;
    use crate::proto::{ProtoMessage, ProtoParser};
    use bytes::{BufMut, Bytes, BytesMut};
    use futures::future::select;
    use futures::future::Either;
    use std::collections::VecDeque;
    use tokio::io::{AsyncRead, AsyncReadExt};
    use tokio::net::TcpStream;

    pub struct ClientConnection {
        // Client state
        client_conn: TcpStream,
        client_parser: ProtoParser,
        client_buffer: BytesMut,
        client_msgs: VecDeque<ProtoMessage>,
        client_buffer_prefix_offset: usize,

        // Server state during transaction
        // TODO: Do we store these on the client conn or the server?
        server_parser: ProtoParser,
        server_buffer: BytesMut,
        server_msgs: VecDeque<ProtoMessage>,
        server_buffer_prefix_offset: usize,
    }

    impl ClientConnection {
        // Check out a server from the connection pool.
        pub async fn checkout_server(&mut self) -> Result<TcpStream, Box<dyn std::error::Error>> {
            unimplemented!();
        }

        pub async fn read_and_parse_from_client(
            &mut self,
        ) -> Result<usize, Box<dyn std::error::Error>> {
            let n = self
                .client_conn
                .read(&mut self.client_buffer[self.client_buffer_prefix_offset..])
                .await?;

            // Parse and adjust the client buffer prefix offset in case the buffer
            // included a small part of a message from the last read.
            let n_parsed = self
                .client_parser
                .parse(&mut self.client_buffer[..n], &mut self.client_msgs)?;
            self.client_buffer_prefix_offset = n - n_parsed;

            // Return only the number of bytes pared.
            Ok(n_parsed)
        }

        pub async fn read_and_parse_from_server(
            &mut self,
            server_conn: &mut TcpStream,
        ) -> Result<usize, Box<dyn std::error::Error>> {
            let n = server_conn
                .read(&mut self.server_buffer[self.server_buffer_prefix_offset..])
                .await?;

            // Parse and adjust the server buffer prefix offset in case the buffer
            // included a small part of a message from the last read.
            let n_parsed = self
                .server_parser
                .parse(&mut self.server_buffer[..n], &mut self.server_msgs)?;
            self.server_buffer_prefix_offset = n - n_parsed;

            // Return only the number of bytes pared.
            Ok(n_parsed)
        }

        // Manage the entire client life-cycle.
        pub async fn spawn(&mut self) -> Result<(), Box<dyn std::error::Error>> {
            // Outter transaction loop.
            loop {
                let n = self.read_and_parse_from_client().await?;

                // Check to ensure it signals the beginning of a txn. Close otherwise.
                while let Some(msg) = self.client_msgs.pop_front() {
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

                let mut server_conn = self.checkout_server().await?;

                // Write those N bytes to the server.
                write_all_with_timeout(
                    &mut server_conn,
                    &mut self.client_buffer[..n],
                    Some(std::time::Duration::from_secs(5)),
                )
                .await?;

                // Proxy between client and server until the client or server ends the txn.
                loop {
                    match select(
                        Box::pin(self.read_and_parse_from_client()),
                        Box::pin(self.read_and_parse_from_server(&mut server_conn)),
                    )
                    .await
                    {
                        // Success case.
                        Either::Left((Ok(client_n), _dropped_server_read)) => {}
                        Either::Right((Ok(server_n), _dropped_client_read)) => {}

                        // Error case.
                        Either::Left((Err(err), _)) | Either::Right((Err(err), _)) => {
                            return Err(err)
                        }
                    }
                }
            }
            Ok(())
        }
    }

    //    async fn usage() {
    //        let msgs = VecDeque::new();
    //
    //        // Read some N bytes.
    //        let n = client.read(&mut client_buffer).await?;
    //        let n_parsed = client_parser.parse(&mut client_buffer[..n], msgs)?;
    //
    //        // Check to ensure it signals the beginning of a txn. Close otherwise.
    //        for msg in msgs {
    //            match msg.msg_type() {
    //                // We only check for complete or partial messages here. The point is to
    //                // detect the beginning of a transaction.
    //                'Q' => {}
    //                'X' => println!("Client sent close request. Closing connection."),
    //                msg_type => {
    //                    println!(
    //                        "Client sent non-query or close command ({}). Closing connection.",
    //                        msg_type
    //                    );
    //                    return;
    //                }
    //            }
    //        }
    //
    //        // Write those N bytes to the server.
    //        crate::net::write_all(
    //            &mut server,
    //            &mut client_buffer[..n_parsed],
    //            std::time::Duration::from_secs(5),
    //        )
    //        .await?;
    //
    //        // Run the txn loop.
    //        loop {
    //            let read_server_bytes_to_copy = async {
    //                let n = server
    //                    .read(&mut server_buffer[server_buffer_prefix_offset..])
    //                    .await?;
    //                let n_with_offset = server_buffer_prefix_offset + n;
    //                let n_parsed =
    //                    server_parser.parse(&mut server_buffer[..n_with_offset], server_msgs)?;
    //                n_parsed
    //            };
    //
    //            let read_clients_bytes_to_copy = async {
    //                let n = client
    //                    .read(&mut client_buffer[client_buffer_prefix_offset..])
    //                    .await?;
    //                let n_with_offset = client_buffer_prefix_offset + n;
    //                let n_parsed =
    //                    client_parser.parse(&mut client_buffer[..n_with_offset], client_msgs)?;
    //                n_parsed
    //            };
    //
    //            // Avoid multiple exclusive owns so we'll return an operation that needs
    //            // to be performed next. The 'n' returned is the parsed buffer size.
    //            let operation =
    //                match futures::future::select(read_server_bytes_to_copy, read_client_bytes_tp_copy)
    //                    .await
    //                {
    //                    // Convert to operation.
    //                    Either::Left((Ok(n), _dropped_client_read)) => CopyFromServertoClient(n),
    //                    Either::Right((Ok(n), _dropped_server_read)) => CopyFromClientToServer(n),
    //                    // Return err.
    //                    Either::Left((Err(err), _)) | Either::Right((Err(err), _)) => return Err(err),
    //                };
    //
    //            // Copy all buffer to either the client or the server.
    //            match operation {
    //                CopyFromServerToClient(n) => {
    //                    crate::net::write_all_with_timeout(&mut client, &server_buffer[..n], None)
    //                        .await?;
    //                }
    //                CopyFromClientToServer(n) => {
    //                    crate::net::write_all_with_timeout(&mut server, &client_buffer[..n], None)
    //                        .await?;
    //                }
    //            }
    //
    //            // Now that we've copied the bytes from either side, we can check for msgs
    //            // To see if we've arrived at an error or a clean end of txn.
    //
    //            // Server Messages
    //            while let Some(msg) = server_msgs.pop_front() {
    //                match msg.msg_type() {
    //                    'Z' => match msg.transaction_type() {
    //                        Some('I') => {
    //                            println!("Transaction completed.");
    //                            break;
    //                        }
    //                    },
    //                    'X' => println!("Server is closing the connection!"),
    //                }
    //            }
    //
    //            // Client Messages
    //            while let Some(msg) = server_msgs.pop_front() {
    //                match msg.msg_type() {
    //                    'X' => println!("Client is closing the connection!"),
    //                }
    //            }
    //        }
    //    }
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
