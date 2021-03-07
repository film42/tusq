use serde::Deserialize;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio::sync::{RwLock, RwLockReadGuard};

#[derive(Debug, Clone)]
pub struct UpdatableConfig {
    inner: Arc<RwLock<Config>>,
}

impl UpdatableConfig {
    pub fn new(config: Config) -> Self {
        Self {
            inner: Arc::new(RwLock::new(config)),
        }
    }

    pub async fn get(&self) -> RwLockReadGuard<'_, Config> {
        self.inner.read().await
    }

    pub async fn update(&self, new_config: Config) {
        let mut config = self.inner.write().await;
        *config = new_config;
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    pub bind_address: String,
    pub databases: BTreeMap<String, Database>,

    #[serde(default = "SystemTime::now")]
    pub updated_at: SystemTime,
}
impl Config {
    pub async fn from_file(path: &str) -> anyhow::Result<Config> {
        let mut file = File::open(path).await?;
        let mut contents = vec![];
        file.read_to_end(&mut contents).await?;
        let config: Config = toml::from_slice(&contents)?;
        Ok(config)
    }

    pub fn example() -> Self {
        // Create a map with required database options.
        let db = Database {
            port: "5432".into(),
            host: "127.0.0.1".into(),
            dbname: "dispatch_development".into(),
            user: "testuser".into(),
            password: Some("123456".into()),
            pool_size: 25,
        };

        // Use above options to create an aliased database.
        let mut databases = BTreeMap::new();
        databases.insert("my_db_alias".into(), db);

        Self {
            updated_at: SystemTime::now(),
            bind_address: "localhost:8432".into(),
            databases,
        }
    }
}

fn default_port() -> String {
    "5432".to_string()
}

const fn default_pool_size() -> u32 {
    25
}

#[derive(Deserialize, Debug, Clone)]
pub struct Database {
    pub dbname: String,
    pub user: String,
    pub host: String,
    pub password: Option<String>,

    #[serde(default = "default_port")]
    pub port: String,

    #[serde(default = "default_pool_size")]
    pub pool_size: u32,
}

impl Database {
    pub fn startup_parameters(&self) -> BTreeMap<String, String> {
        let mut params = BTreeMap::new();
        params.insert("database".into(), self.dbname.clone());
        params.insert("user".into(), self.user.clone());
        params
    }
}
