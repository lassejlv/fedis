mod auth;
mod command;
mod config;
mod logging;
mod persistence;
mod protocol;
mod server;
mod stats;
mod store;

use config::Config;
use server::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    logging::init()?;
    let config = Config::from_env_and_args()?;
    let server = Server::new(config).await?;
    server.run().await
}
