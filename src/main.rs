pub mod config;
pub mod core;
pub mod pool;
pub mod proto;

use clap::Parser;
use config::{Config, UpdatableConfig};
use pool::PgPooler;
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tokio::signal::unix::{signal, SignalKind};

#[derive(Parser)]
struct Opts {
    #[clap(short, long, default_value = "tusq.toml")]
    config: String,
}

async fn listen_for_clients(
    listener: TcpListener,
    pooler: PgPooler,
    shutdown: tokio::sync::watch::Receiver<String>,
    worker: waitgroup::Worker,
) -> anyhow::Result<()> {
    loop {
        let (client_conn, _) = listener.accept().await?;
        let client_info = format!("{:?}", client_conn);
        log::info!("Client connected: {:?}", client_info);
        tokio::spawn({
            // Build the client pgconn.
            let mut client_conn = core::PgConn::new(client_conn)?;

            // Build a db pool (unique per conn for now).
            let pooler = pooler.clone();

            // Graceful shutdown tools.
            let shutdown = shutdown.clone();
            let worker = worker.clone();

            // Start the show.
            async move {
                // Retain the worker until the async block exits. This keeps it in scope.
                let _worker = worker;

                // Parse the startup flow.
                let server_pool = match client_conn.handle_startup(pooler).await {
                    Ok(sm) => {
                        log::trace!(
                            "Client established and ready for query: {:?}, startup: {:?}",
                            client_info,
                            sm
                        );
                        sm
                    }
                    Err(err) => {
                        log::warn!(
                            "Client closed with error: {:?}, conn: {:?}",
                            err,
                            client_info
                        );
                        return;
                    }
                };

                // Run the txn loop.
                match core::spawn(client_conn, server_pool, shutdown).await {
                    Ok(_) => println!("Client closed: {:?}", client_info),
                    Err(err) => println!(
                        "Client closed with error: {:?}, conn: {:?}",
                        err, client_info
                    ),
                }
            }
        });
    }
}

fn say_hello() {
    log::info!(
        r#"
        ,----,
      ,/   .`|
    ,`   .'  :
  ;    ;     /                     ,----.
.'___,/    ,'       ,--,          /   /  \-.
|    :     |      ,'_ /|  .--.--.|   :    :|
;    |.';  ; .--. |  | : /  /    |   | .\  .
`----'  |  ,'_ /| :  . ||  :  /`..   ; |:  |
    '   :  |  ' | |  . .|  :  ;_ '   .  \  |
    |   |  |  | ' |  | | \  \    `\   `.   |
    '   :  :  | : ;  ; |  `----.   `--'""| |
    ;   |.''  :  `--'   \/  /`--'  / |   | |
    '---'  :  ,      .-.'--'.     /  |   | :
            `--`----'     `--'---'   `---'.|
                                       `---`"#
    );
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init_from_env(env_logger::Env::default().default_filter_or("info"));
    say_hello();

    let opts: Opts = Opts::parse();
    let config = Config::from_file(&opts.config).await?;

    let bind_addr = config.bind_address.parse::<SocketAddr>()?;
    log::info!("Listening on: {:?}", bind_addr);
    let listener = TcpListener::bind(bind_addr).await?;
    let config = UpdatableConfig::new(config);
    let pooler = PgPooler::new(config.clone());

    // Shutdown signal
    let mut sigterm = signal(SignalKind::terminate()).expect("signal should register");
    let mut sigint = signal(SignalKind::interrupt()).expect("signal should register");
    let shutdown = futures::future::select(Box::pin(sigterm.recv()), Box::pin(sigint.recv()));

    // Client close signal
    let (tx, rx) = tokio::sync::watch::channel("".into());
    let wg = waitgroup::WaitGroup::new();

    tokio::spawn({
        let config_path = opts.config.clone();
        let config = config.clone();
        let mut sighup = signal(SignalKind::hangup()).expect("signal should register");

        async move {
            loop {
                sighup.recv().await;
                log::warn!("Reloading config from disk...");

                match Config::from_file(&config_path).await {
                    // Swap the config.
                    Ok(new_config) => {
                        config.update(new_config).await;
                        log::warn!("Reload done.");
                    }
                    Err(err) => log::warn!("Reload failed: {:?}.", err),
                }
            }
        }
    });

    // Listen and await shutdown
    tokio::select! {
        _ = shutdown => {
            // This listener is now dropped.
            log::warn!("Shutdown received... waiting for clients to finish transactions.");
            tx.send("gracefully shutdown".into())?;
        }
        res = listen_for_clients(listener, pooler, rx.clone(), wg.worker()) => {
            log::warn!("Listener exited: {:?}", res);
        }
    }

    // Wait for shutdown or for second signal.
    tokio::select! {
        _ = wg.wait() => { /* Successful shutdown */ }
        _ = futures::future::select(Box::pin(sigterm.recv()), Box::pin(sigint.recv())) => {
            log::warn!("Second shutdown signal received! Stopping now.")
        }
    }

    log::warn!("Good bye!");
    Ok(())
}
