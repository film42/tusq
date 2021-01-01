use crate::config::Config;
use crate::core::net::write_all_with_timeout;
use crate::core::PgConn;
use crate::proto::StartupMessage;
use async_trait::async_trait;
use bb8::{ManageConnection, Pool, PooledConnection};
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::Mutex;

#[derive(Debug)]
pub struct PgConnPool {
    config: Config,
    startup_message: StartupMessage,
}

impl PgConnPool {
    pub fn new(config: Config, startup_message: StartupMessage) -> Self {
        Self {
            config,
            startup_message,
        }
    }
}

#[async_trait]
impl ManageConnection for PgConnPool {
    type Connection = PgConn;
    type Error = anyhow::Error;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        let dbname = self
            .startup_message
            .database_name()
            .expect("database was set");

        let database_options = self
            .config
            .databases
            .get(&dbname)
            .expect("database config to exist");

        let addr = format!(
            "{}:{}",
            database_options.get("host").expect("host was set"),
            database_options.get("port").expect("port was set")
        )
        .parse::<SocketAddr>()
        .expect("valid socket addr");

        // Build the server startup_message.
        let mut startup_message = self.startup_message.clone();
        for (key, value) in database_options.iter() {
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
        startup_message
            .parameters
            .insert("application_name".into(), "tusq".into());

        println!("Connecting to database: {:?}", startup_message);

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

    async fn is_valid(&self, _conn: &mut PooledConnection<'_, Self>) -> Result<(), Self::Error> {
        Ok(())
    }

    fn has_broken(&self, _conn: &mut Self::Connection) -> bool {
        false
    }
}

#[derive(Clone)]
pub struct PgPooler {
    config: Config,
    pools: Arc<Mutex<BTreeMap<String, bb8::Pool<PgConnPool>>>>,
}

impl PgPooler {
    pub fn new(config: Config) -> PgPooler {
        PgPooler {
            config,
            pools: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    pub async fn get_pool(
        &mut self,
        startup_message: StartupMessage,
    ) -> anyhow::Result<bb8::Pool<PgConnPool>> {
        // TODO: We assume the DB is always set.
        let database = startup_message
            .database_name()
            .expect("database name was set");

        // Get lock around "pools", get or insert new pool, and clone.
        let mut pools = self.pools.lock().await;
        let pool = match pools.entry(database) {
            Entry::Occupied(pool) => pool.into_mut(),
            Entry::Vacant(pools) => {
                // TODO: Better to unlock here while connecting? Probably? Nested locking per
                // database?
                // TODO: Make size params on the config.
                let manager = PgConnPool::new(self.config.clone(), startup_message);
                let pool = Pool::builder().max_size(50).build(manager).await?;
                pools.insert(pool)
            }
        }
        .clone();

        Ok(pool)
    }
}
