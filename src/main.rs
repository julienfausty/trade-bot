use kraken_async_rs::clients::core_kraken_client::CoreKrakenClient;
use kraken_async_rs::clients::http_response_types::ResultErrorResponse;
use kraken_async_rs::clients::kraken_client::KrakenClient;
use kraken_async_rs::crypto::nonce_provider::{IncreasingNonceProvider, NonceProvider};
use kraken_async_rs::request_types::{CandlestickInterval, OHLCRequest, StringCSV};
use kraken_async_rs::response_types::OHLC;
use kraken_async_rs::secrets::secrets_provider::{SecretsProvider, StaticSecretsProvider};
use kraken_async_rs::test_support::set_up_logging;
use kraken_async_rs::wss::{KrakenMessageStream, KrakenWSSClient, WS_KRAKEN, WS_KRAKEN_AUTH};
use kraken_async_rs::wss::{Message, OhlcSubscription, WssMessage};

use tokio::sync::Mutex;
use tokio::time::timeout;
use tokio_stream::StreamExt;

use tracing::{info, warn};

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;

struct LiveFeed {
    // timeout of the websocket connection
    timeout: u64,

    // Websocket stream to Kraken server
    stream: KrakenMessageStream<WssMessage>,
}

impl LiveFeed {
    // Create a new web socket feed to Kraken server for OHLC data with specified time interval
    // (in s) and timeout (in min) following provided tickers.
    pub async fn new(
        timeout: u64,
        interval: i32,
        tickers: Vec<String>,
    ) -> Result<LiveFeed, String> {
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

        Ok(LiveFeed { timeout, stream })
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

pub trait CandlestickIntervalConvertible {
    fn into_candlestick_interval(&self) -> CandlestickInterval
    where
        Self: PartialOrd<i32>,
    {
        if *self < 5 {
            CandlestickInterval::Minute
        } else if *self < 15 {
            CandlestickInterval::Minutes5
        } else if *self < 30 {
            CandlestickInterval::Minutes15
        } else if *self < 60 {
            CandlestickInterval::Minutes30
        } else if *self < 240 {
            CandlestickInterval::Hour
        } else if *self < 1440 {
            CandlestickInterval::Hours4
        } else if *self < 10080 {
            CandlestickInterval::Day
        } else if *self < 21600 {
            CandlestickInterval::Week
        } else {
            CandlestickInterval::Days15
        }
    }
}

impl CandlestickIntervalConvertible for i32 {}

struct HistoricalFeed {
    queue: VecDeque<HashMap<String, OHLC>>,
}

impl HistoricalFeed {
    // Create a historical feed from Kraken server using OHLC data with specified time interval
    // (in s) and timeout (in min) for provided tickers.
    pub async fn new(
        ago: i64,
        interval: i32,
        tickers: Vec<String>,
    ) -> Result<HistoricalFeed, String> {
        let secrets_provider: Box<Arc<Mutex<dyn SecretsProvider>>> =
            Box::new(Arc::new(Mutex::new(StaticSecretsProvider::new("", ""))));
        let nonce_provider: Box<Arc<Mutex<dyn NonceProvider>>> =
            Box::new(Arc::new(Mutex::new(IncreasingNonceProvider::new())));

        let mut client = CoreKrakenClient::new(secrets_provider, nonce_provider);

        let server_time = match client.get_server_time().await {
            Ok(response) => {
                if let ResultErrorResponse {
                    result: Some(time), ..
                } = response
                {
                    time.unix_time
                } else {
                    return Err(format!("{:?}", response.error));
                }
            }
            Err(network_error) => return Err(format!("{:?}", network_error)),
        };

        let ohlc_request = OHLCRequest::builder(StringCSV::new(tickers).to_string())
            .since(server_time - ago)
            .interval(interval.into_candlestick_interval())
            .build();

        let ohlc_map = match client.get_ohlc(&ohlc_request).await {
            Ok(response) => {
                if let ResultErrorResponse {
                    result: Some(ohlc), ..
                } = response
                {
                    ohlc.ohlc
                } else {
                    return Err(format!("{:?}", response.error));
                }
            }
            Err(network_err) => return Err(format!("{:?}", network_err)),
        };

        if ohlc_map.is_empty() {
            return Ok(HistoricalFeed {
                queue: VecDeque::new(),
            });
        }

        let lengths: Vec<usize> = ohlc_map.iter().map(|(_, ohlc)| ohlc.len()).collect();

        let reference = lengths[0];
        if !lengths.iter().all(|&length| length == reference) {
            return Err("Returned OHLC data does not have the same lengths.".into());
        }

        let queue: VecDeque<HashMap<String, OHLC>> = (0..reference)
            .map(|index| {
                ohlc_map
                    .iter()
                    .map(|(ticker, data)| (ticker.clone(), data[index].clone()))
                    .collect()
            })
            .collect();

        Ok(HistoricalFeed { queue })
    }

    pub async fn consume(&mut self) -> Option<HashMap<String, OHLC>> {
        self.queue.pop_front()
    }
}

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
