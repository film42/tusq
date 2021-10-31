# tusq

A postgres transactional connection pooler written in rust. It currenlty uses an 8KB buffer for each client or server connection. It is as fast as pgbouncer in basic `select * from table` benchmarks.
My goal for this project is to write a postgres connection pooler that is very fast and written in a highly readable way, making outside contribution easy.

### Running

Use cargo to build this project. After running and connecting via psql, you'll see the following:

```
$ cargo run --release -- --config examples/config.toml

[2021-01-02T16:49:38Z INFO  tusq] Listening on: 127.0.0.1:8432
[2021-01-02T16:49:40Z INFO  tusq] Client connected: "PollEvented { io: Some(TcpStream { addr: 127.0.0.1:8432, peer: 127.0.0.1:35822, fd: 10 }) }"
[2021-01-02T16:49:40Z INFO  tusq::pool] Connecting to database: StartupMessage { protocol_version: 196608, parameters: {"application_name": "tusq", "client_encoding": "UTF8", "database": "dispatch_development", "user": "testuser"} }
Client closed: "PollEvented { io: Some(TcpStream { addr: 127.0.0.1:8432, peer: 127.0.0.1:35822, fd: 10 }) }"
```

### Config 

Currently using TOML for config files:

```toml
bind_address = "127.0.0.1:8432"

[databases]
some_db = { user = "postgres", password = "123456", dbname = "yolo_db", host = "127.0.0.1" }
```

You can also specify `port`, and `pool_size` for each database.

You can send a `SIGHUP` to the running tusq process for a live config reload.

### TODO

1. Support SSL.
3. Better configuration.
4. Benchmarking.
5. Canceling queries.


### License

MIT
