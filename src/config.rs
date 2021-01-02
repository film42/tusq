use serde::Deserialize;
use std::collections::BTreeMap;

#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    pub bind_address: String,
    pub databases: BTreeMap<String, Database>,
}
impl Config {
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
