use std::collections::BTreeMap;

#[derive(Debug, Clone)]
pub struct Config {
    pub bind_address: String,
    pub databases: BTreeMap<String, BTreeMap<String, String>>,
}

impl Config {
    pub fn example() -> Self {
        // Create a map with required database options.
        let mut database_options = BTreeMap::new();
        database_options.insert("port".into(), "5432".into());
        database_options.insert("host".into(), "127.0.0.1".into());
        database_options.insert("dbname".into(), "dispatch_development".into());
        database_options.insert("user".into(), "postgres".into());
        database_options.insert("sslmode".into(), "disable".into());

        // Use above options to create an aliased database.
        let mut databases = BTreeMap::new();
        databases.insert("my_db_alias".into(), database_options);

        Self {
            bind_address: "localhost:8432".into(),
            databases,
        }
    }
}
