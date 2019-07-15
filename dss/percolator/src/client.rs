use crate::service::{TSOClient, TransactionClient};
use crate::msg::*;
use std::time::{Duration, Instant};

//extern crate tokio;
//use tokio::timer::Timeout;
use futures::{Async, Future, Poll};
use futures_timer::Delay;
use labrpc::*;

// BACKOFF_TIME_MS is the wait time before retrying to send the request.
// It should be exponential growth. e.g.
//|  retry time  |  backoff time  |
//|--------------|----------------|
//|      1       |       100      |
//|      2       |       200      |
//|      3       |       400      |
const BACKOFF_TIME_MS: u64 = 100;
// RETRY_TIMES is the maximum number of times a client attempts to send a request.
const RETRY_TIMES: usize = 3;

struct Timeout {
    future: RpcFuture<TimestampResponse>,
    delay: Delay,
    waiting_on_future: bool,
}

// concept taken from 
// https://docs.rs/tokio-timer/0.2.11/src/tokio_timer/timeout.rs.html#127-130
//
// `wait`ing on a Timeout future will
// return Ok(v) if the internal Future returns Ok(v) within the timeout period
// for all other cases, returns Err(Error::Timeout) *at the moment of the timeout*
//      this implies that even if the internal future returns an error early,
//      the Timeout will not return a response untill the timeout interval is up.
// *caveat* If the internal timer fails, `wait` will yield an Err(Error::Other(s))
impl Timeout {
    pub fn new(future: RpcFuture<TimestampResponse>, timeout: Duration) -> Timeout {
        let delay = Delay::new(timeout);
        let waiting_on_future = true;
        Timeout {future, delay, waiting_on_future}
    }
}

impl Future for Timeout {
    type Item = TimestampResponse;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        // race between the future and the timer
        // if the future "wins" the race, but returns an error,
        // wait for the timer to finish
        if self.waiting_on_future {
            match self.future.poll() {
                Ok(Async::Ready(v)) => return Ok(Async::Ready(v)),
                Ok(Async::NotReady) => {},
                Err(e)              => self.waiting_on_future = false,
            }
        }

        match self.delay.poll() {
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Ok(Async::Ready(_)) => Err(Error::Timeout),
            Err(e)              => Err(Error::Other("timeout error: error ".to_string())),
        }
    }
}

/// Client mainly has two purposes:
/// One is getting a monotonically increasing timestamp from TSO (Timestamp Oracle).
/// The other is do the transaction logic.
#[derive(Clone)]
pub struct Client {
    // Your definitions here.
    tso_client: TSOClient,
    txn_client: TransactionClient,
}

impl Client {
    /// Creates a new Client.
    pub fn new(tso_client: TSOClient, txn_client: TransactionClient) -> Client {
        // Your code here.
        Client {tso_client, txn_client}
    }

    /// Gets a timestamp from a TSO.
    pub fn get_timestamp(&self) -> Result<u64> {
        let mut result: Result<u64> = Err(Error::Other("Unknown error".to_string()));
        let mut backoff_time_ms = BACKOFF_TIME_MS;
        for i in 0..RETRY_TIMES {
            let timestamp_fut = self.tso_client.get_timestamp(&TimestampRequest {});
            let timeout = Timeout::new(timestamp_fut, Duration::from_millis(backoff_time_ms));
            match timeout.wait() {
                Ok(resp) => return Ok(resp.timestamp),
                Err(e)   => result = Err(e),
            };
            backoff_time_ms *= 2;
        }

        result
    }

    /// Begins a new transaction.
    pub fn begin(&mut self) {
        // Your code here.
        unimplemented!()
    }

    /// Gets the value for a given key.
    pub fn get(&self, key: Vec<u8>) -> Result<Vec<u8>> {
        // Your code here.
        unimplemented!()
    }

    /// Sets keys in a buffer until commit time.
    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>) {
        // Your code here.
        unimplemented!()
    }

    /// Commits a transaction.
    pub fn commit(&self) -> Result<bool> {
        // Your code here.
        unimplemented!()
    }
}
