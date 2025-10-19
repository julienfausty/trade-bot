use kraken_async_rs::test_support::set_up_logging;
use kraken_async_rs::wss::{KrakenMessageStream, KrakenWSSClient, WS_KRAKEN, WS_KRAKEN_AUTH};
use kraken_async_rs::wss::{Message, OhlcSubscription, WssMessage};
use std::time::Duration;
use tokio::time::timeout;
use tokio_stream::StreamExt;
use tracing::{info, warn};

struct Feed {
    // timeout of the websocket connection
    timeout: u64,

    // Websocket stream to Kraken server
    stream: KrakenMessageStream<WssMessage>,
}

impl Feed {
    // Create a new web socket feed to Kraken server for OHLC data with specified time interval
    // (in s) and timeout (in min) following provided tickers.
    pub async fn new(timeout: u64, interval: i32, tickers: Vec<String>) -> Result<Feed, String> {
        let mut client = KrakenWSSClient::new_with_tracing(WS_KRAKEN, WS_KRAKEN_AUTH, true, true);
        let mut stream = match client.connect::<WssMessage>().await {
            Ok(stream) => stream,
            Err(message) => return Err(format!("{:?}", message)),
        };

        let ohlc_params = OhlcSubscription::new(tickers, interval);

        let subscription = Message::new_subscription(ohlc_params, 0);

        match stream.send(&subscription).await {
            Ok(_) => (),
            Err(message) => return Err(format!("{:?}", message)),
        };

        Ok(Feed { timeout, stream })
    }

    // Poll for data from the feed
    pub async fn consume(&mut self) -> Result<WssMessage, String> {
        match timeout(Duration::from_secs(self.timeout), self.stream.next()).await {
            Ok(Some(communication)) => match communication {
                Ok(message) => Ok(message),
                Err(error) => Err(format!("{:?}", error)),
            },
            Ok(None) => Err("Received None message in feed.".to_string()),
            Err(contained) => Err(format!("{:?}", contained)),
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), String> {
    set_up_logging("trade-bot.log");

    let mut feed = match Feed::new(10, 5, vec!["ETH/EUR".to_string()]).await {
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
