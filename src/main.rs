mod analysis;
mod feeds;

use crate::feeds::LiveFeed;

use kraken_async_rs::test_support::set_up_logging;

use tracing::{info, warn};

#[tokio::main]
async fn main() -> Result<(), String> {
    set_up_logging("trade-bot.log");

    let mut feed = match LiveFeed::new(10, 5, vec!["ETH/EUR".to_string()]).await {
        Ok(feed) => feed,
        Err(message) => return Err(message),
    };

    loop {
        match feed.consume().await {
            Ok(message) => info!("{:?}", message),
            Err(message) => warn!("{:?}", message),
        };
    }
}
