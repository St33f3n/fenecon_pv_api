use std::time::Duration;

use crate::config::Config;
use crate::mqtt::SolarMqttClient;
use color_eyre::{Result, eyre::eyre};
use tracing::{Level, debug, error, info, warn};
use tracing_subscriber::FmtSubscriber;

mod cache;
mod calculator;
mod collector;
mod config;
mod db;
mod mqtt;

#[cfg(test)]
mod test;

#[tokio::main]
async fn main() -> Result<()> {
    setup()?;

    println!("Hello, world!");

    return Ok(());
}

fn setup() -> Result<()> {
    if std::env::var("RUST_LIB_BACKTRACE").is_err() {
        unsafe { std::env::set_var("RUST_LIB_BACKTRACE", "0") }
    }

    // Install enhanced error reporting
    color_eyre::install()?;

    // Set default log level if not specified
    if std::env::var("RUST_LOG").is_err() {
        unsafe { std::env::set_var("RUST_LOG", "info") }
    }

    setup_logging_env();
    Ok(())
}

fn setup_logging_env() {
    FmtSubscriber::builder()
        .with_max_level(Level::DEBUG)
        .with_target(false)
        .with_thread_ids(true)
        .with_file(true)
        .with_line_number(true)
        .pretty()
        .init();
}
