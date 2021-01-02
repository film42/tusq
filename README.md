# tusq

Yet another playground project to port pgbouncer to rust

### Running

Use cargo to build this project. After running and connecting via psql, you'll see the following:

```
$ cargo run --release -- --config examples/config.toml

[2021-01-02T16:49:38Z INFO  tusq] Listening on: 127.0.0.1:8432
[2021-01-02T16:49:40Z INFO  tusq] Client connected: "PollEvented { io: Some(TcpStream { addr: 127.0.0.1:8432, peer: 127.0.0.1:35822, fd: 10 }) }"
[2021-01-02T16:49:40Z INFO  tusq::pool] Connecting to database: StartupMessage { protocol_version: 196608, parameters: {"application_name": "tusq", "client_encoding": "UTF8", "database": "dispatch_development", "user": "testuser"} }
Client closed: "PollEvented { io: Some(TcpStream { addr: 127.0.0.1:8432, peer: 127.0.0.1:35822, fd: 10 }) }"
```

### TODO

1. Support SSL.
2. Smarter startup message parsing.
3. Better configuration.
4. Benchmarking.
5. Listen for server closed before yielding server to client.
7. Configuration reloading.
