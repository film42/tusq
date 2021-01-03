use crate::pool::{PgConnPool, PgPooler};
use crate::proto::{messages, ProtoMessage, ProtoParser, ProtoStartup, StartupMessage};
use bytes::BytesMut;
use futures::future::select;
use futures::future::Either;
use net::write_all_with_timeout;
use std::collections::{BTreeMap, VecDeque};
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
    incomplete_buffer: BytesMut,
    incomplete_buffer_len: usize,
    pub(crate) is_broken: bool,
    pub(crate) is_active_transaction: bool,
    pub(crate) msgs: VecDeque<ProtoMessage>,
    pub(crate) server_parameters: BTreeMap<String, String>,
    pub(crate) startup_message: Option<StartupMessage>,
}

impl PgConn {
    pub fn new(conn: TcpStream) -> Self {
        let mut buffer = BytesMut::with_capacity(8096);
        buffer.resize(8096, 0);

        let mut incomplete_buffer = BytesMut::with_capacity(8);
        incomplete_buffer.resize(8, 0);

        Self {
            conn,
            buffer,
            incomplete_buffer,
            incomplete_buffer_len: 0,
            is_broken: false,
            is_active_transaction: false,
            parser: ProtoParser::new(),
            msgs: VecDeque::new(),
            server_parameters: BTreeMap::new(),
            startup_message: None,
        }
    }

    pub fn database_name(&self) -> Option<String> {
        if let Some(ref startup_message) = self.startup_message {
            return Some(
                startup_message
                    .parameters
                    .get("database")
                    .expect("database was set")
                    .clone(),
            );
        }
        None
    }

    pub async fn write_auth_ok(&mut self) -> anyhow::Result<()> {
        let msg = messages::auth_ok();
        write_all_with_timeout(&mut self.conn, &msg, None).await?;
        Ok(())
    }

    pub async fn write_ready_for_query(&mut self) -> anyhow::Result<()> {
        let msg = messages::ready_for_query();
        write_all_with_timeout(&mut self.conn, &msg, None).await?;
        Ok(())
    }

    pub async fn write_server_parameters(
        &mut self,
        params: &BTreeMap<String, String>,
    ) -> anyhow::Result<()> {
        let mut payload = vec![];
        for (key, value) in params.iter() {
            let msg = messages::server_parameter(key, value);
            payload.extend_from_slice(&msg);
        }
        write_all_with_timeout(&mut self.conn, &payload, None).await?;
        Ok(())
    }

    pub async fn handle_startup(
        &mut self,
        mut pooler: PgPooler,
    ) -> anyhow::Result<bb8::Pool<PgConnPool>> {
        let n = self.conn.read(&mut self.buffer).await?;
        if n == 0 {
            anyhow::bail!("Client disconnected: EOF");
        }

        // Parse startup message.
        let (_n_parsed, startup) = self.parser.parse_startup(&self.buffer[..n])?;

        // Check if we received an SSLRequest or StartupMessage.
        let sm = match startup {
            Some(ProtoStartup::SSLRequest) => {
                log::trace!("Client sent an SSLRequest...denying.");
                // If an SSL request, we'll deny for now and continue.
                write_all_with_timeout(&mut self.conn, &[b'N'], None).await?;

                // Read and await a startup message after denying SSL.
                let n = self.conn.read(&mut self.buffer).await?;
                if n == 0 {
                    anyhow::bail!("Client disconnected: EOF");
                }
                let (_n_parsed, startup) = self.parser.parse_startup(&self.buffer[..n])?;

                // Pluck out the startup message or bail with error message.
                match startup {
                    Some(ProtoStartup::Message(startup_message)) => startup_message,
                    Some(msg) => {
                        anyhow::bail!("Received invalid startup message from client: {:?}", msg)
                    }
                    None => anyhow::bail!("Missing or incomplete startup message from client"),
                }
            }
            Some(ProtoStartup::CancelRequest) => {
                log::trace!("Cancel request received.");
                anyhow::bail!("Cancel request is not supported.")
            }
            Some(ProtoStartup::Message(startup_message)) => startup_message,
            None => anyhow::bail!("Missing or incomplete startup message from client"),
        };
        log::trace!("Client sent a StartupMessage: {:?}", &sm);

        self.startup_message = Some(sm.clone());

        // TODO: Check startup message and configuration to conduct an Authn flow.
        self.write_auth_ok().await?;

        // HACK: This is duplicating work.
        // Write server parameters from a working real server.. should move later.
        let pool = pooler.get_pool(sm.clone()).await?;
        let server_conn = pool
            .get()
            .await
            .map_err(|err| anyhow::anyhow!("Connection Poool: {:?}", err))?;
        self.write_server_parameters(&server_conn.server_parameters)
            .await?;
        drop(server_conn);

        // Signal read for query.. should probably move later.
        self.write_ready_for_query().await?;

        // Return original startup message.
        Ok(pool)
    }

    // Ensure the connection is open and in a "would block" state, meaning
    // there is no outstanding buffer.
    pub fn is_valid(&mut self) -> anyhow::Result<bool> {
        match self.conn.try_read(&mut self.buffer) {
            Ok(0) => anyhow::bail!("Connection is closed: EOF"),
            Ok(_) => {
                anyhow::bail!("Connection has readable buffer. Closing due to uncertain state.")
            }
            Err(ref err) if err.kind() == std::io::ErrorKind::WouldBlock => Ok(true),
            Err(err) => anyhow::bail!("Error checking connection: {:?}", err),
        }
    }

    #[inline]
    pub async fn read_and_parse(&mut self) -> anyhow::Result<usize> {
        // Copy any incomplete buffer data to new buffer.
        for idx in 0..self.incomplete_buffer_len {
            self.buffer[idx] = self.incomplete_buffer[idx];
        }

        // Read as much data as possible to the main buffer.
        let n = self
            .conn
            .read(&mut self.buffer[self.incomplete_buffer_len..])
            .await?;

        // Conn has indicated an EOF.
        if n == 0 {
            self.is_broken = true;
            anyhow::bail!("Disconnected: EOF");
        }

        // Attempt to parse the buffer.
        let n_to_parse = self.incomplete_buffer_len + n;

        let n_parsed = self
            .parser
            .parse(&self.buffer[..n_to_parse], &mut self.msgs)?;

        // Copy any unparsed bytes to the incomplete buffer which will be copied
        // to the next buffer when this method is called.
        self.incomplete_buffer_len = n_to_parse - n_parsed;
        for idx in 0..self.incomplete_buffer_len {
            self.incomplete_buffer[idx] = self.buffer[idx + n_parsed];
        }

        // Return only the number of bytes pared.
        Ok(n_parsed)
    }
}

// Manage the entire client life-cycle.
pub async fn spawn(
    mut client_conn: PgConn,
    pool: bb8::Pool<PgConnPool>,
    mut shutdown: tokio::sync::watch::Receiver<String>,
) -> anyhow::Result<()> {
    // Outter transaction loop.
    loop {
        // Read and parse. Bail if we get an EOF. Close connection if tusq is shutting down.
        #[rustfmt::skip]
        let n = tokio::select! {
            _ = shutdown.changed() => return Ok(()),
            res = client_conn.read_and_parse() => res?,
        };
        if n == 0 {
            return Ok(());
        }

        // Check to ensure it signals the beginning of a txn. Close otherwise.
        while let Some(msg) = client_conn.msgs.pop_front() {
            match msg.msg_type() {
                // We only check for complete or partial messages here. The point is to
                // detect the beginning of a transaction.
                'Q' => {}
                'X' => {
                    log::info!("Client sent close request. Closing connection.");
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

        // Keep valid lifetime for the startup message.
        let mut server_conn = pool
            .get()
            .await
            .map_err(|err| anyhow::anyhow!("Connection Poool: {:?}", err))?;

        // Mark that we're entering a transaction for the connection pool to clean up.
        server_conn.is_active_transaction = true;

        // Write those N bytes to the server.
        write_all_with_timeout(
            &mut server_conn.conn,
            &client_conn.buffer[..n],
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
                        &client_conn.buffer[..n],
                        Some(std::time::Duration::from_secs(30)),
                    )
                    .await?;
                }
                Op::CopyFromServerToClient(n) => {
                    write_all_with_timeout(&mut client_conn.conn, &server_conn.buffer[..n], None)
                        .await?;
                }
            };

            // Check protocol messages for changes.

            // Server Messages
            while let Some(msg) = server_conn.msgs.pop_front() {
                // println!("SRV->CLT: {:?}", msg);

                match msg.msg_type() {
                    'Z' => {
                        if let Some('I') = msg.transaction_type(&server_conn.buffer) {
                            // Signal the connection is safe to be used by a new client.
                            server_conn.is_active_transaction = false;
                            break 'transaction;
                        }
                    }
                    'X' => {
                        log::warn!("Server is closing the connection!");
                        panic!("Server is closing early");
                    }
                    _ => { /* Proxy and continue. */ }
                }
            }

            // Client Messages
            while let Some(msg) = client_conn.msgs.pop_front() {
                // println!("CLT->SRV: {:?}", msg);

                match msg.msg_type() {
                    'X' => {
                        log::warn!("Client is closing the connection!");
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
    ) -> anyhow::Result<Option<usize>> {
        let inner = match time::timeout(timeout, conn.read(buffer)).await {
            // Check for success or error from write.
            Ok(Ok(n)) => Ok(Some(n)),
            Ok(Err(err)) => Err(err),

            // Operation timed out.
            Err(_) => Ok(None),
        }?;

        Ok(inner)
    }

    // Add helper function to handle a write with timeout.
    pub async fn write_all_with_timeout(
        conn: &mut TcpStream,
        buffer: &[u8],
        timeout: Option<std::time::Duration>,
    ) -> anyhow::Result<Option<usize>> {
        if timeout.is_none() {
            conn.write_all(buffer).await?;
            return Ok(Some(buffer.len()));
        }

        let timeout = timeout.expect("never None");
        let inner = match time::timeout(timeout, conn.write_all(buffer)).await {
            // Check for success or error from write.
            Ok(Ok(_)) => Ok(Some(buffer.len())),
            Ok(Err(err)) => Err(err),

            // Operation timed out.
            Err(_) => Ok(None),
        }?;

        Ok(inner)
    }
}
