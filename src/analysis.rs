use rbtree::RBTree;

use std::{
    collections::HashMap,
    ops::{Add, AddAssign, Div, Sub},
    sync::RwLock,
};

#[derive(Debug, Clone)]
pub struct OHLC {
    pub time: i64,
    pub open: f32,
    pub high: f32,
    pub low: f32,
    pub close: f32,
    pub vwap: f32,
    pub volume: f32,
    pub count: i64,
}

impl Add for OHLC {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        Self {
            time: self.time + other.time,
            open: self.open + other.open,
            high: self.high + other.high,
            low: self.low + other.low,
            close: self.close + other.close,
            vwap: self.vwap + other.vwap,
            volume: self.volume + other.volume,
            count: self.count + other.count,
        }
    }
}

impl AddAssign for OHLC {
    fn add_assign(&mut self, rhs: Self) {
        *self = self.clone() + rhs;
    }
}

impl Sub for OHLC {
    type Output = Self;

    fn sub(self, other: Self) -> Self {
        Self {
            time: self.time - other.time,
            open: self.open - other.open,
            high: self.high - other.high,
            low: self.low - other.low,
            close: self.close - other.close,
            vwap: self.vwap - other.vwap,
            volume: self.volume - other.volume,
            count: self.count - other.count,
        }
    }
}

impl Div<usize> for OHLC {
    type Output = Self;

    fn div(self, rhs: usize) -> Self {
        Self {
            time: self.time / (rhs as i64),
            open: self.open / (rhs as f32),
            high: self.high / (rhs as f32),
            low: self.low / (rhs as f32),
            close: self.close / (rhs as f32),
            vwap: self.vwap / (rhs as f32),
            volume: self.volume / (rhs as f32),
            count: self.count / (rhs as i64),
        }
    }
}

pub struct MovingStatistics {
    universe_window: usize,
    universe: RwLock<RBTree<i64, OHLC>>,
}

impl MovingStatistics {
    pub fn new(universe_window: usize) -> MovingStatistics {
        MovingStatistics {
            universe_window,
            universe: RwLock::new(RBTree::new()),
        }
    }

    pub async fn update(&mut self, unit: OHLC) -> Result<Option<OHLC>, String> {
        let mut popped = None;
        let mut write_universe = match self.universe.write() {
            Ok(write_lock) => write_lock,
            Err(error) => return Err(format!("{:?}", error)),
        };

        if write_universe.len() == self.universe_window {
            match write_universe.pop_first() {
                Some(pair) => {
                    popped = Some(pair.1);
                }
                None => (),
            }
        }

        write_universe.insert(unit.time, unit);
        Ok(popped)
    }

    pub async fn means(&self, windows: Vec<usize>) -> Result<HashMap<usize, Vec<OHLC>>, String> {
        for window in &windows {
            if *window > self.universe_window {
                return Err(format!(
                    "Tried to analyse moving window ({:}) that was larger than universe window {:}.",
                    window, self.universe_window
                ));
            }
        }

        let read_universe = match self.universe.read() {
            Ok(read_lock) => read_lock,
            Err(error) => return Err(format!("{:?}", error)),
        };

        let mut sum_buffer = OHLC {
            time: 0,
            open: 0.0,
            high: 0.0,
            low: 0.0,
            close: 0.0,
            vwap: 0.0,
            volume: 0.0,
            count: 0,
        };

        let mut prefix_sum: Vec<OHLC> = read_universe
            .iter()
            .map(|element| {
                let cloned = sum_buffer.clone();
                sum_buffer += element.1.clone();
                return cloned;
            })
            .collect();

        prefix_sum.push(sum_buffer);

        let mut means = HashMap::new();

        for window in &windows {
            let number_shifts = self.universe_window - *window;

            let moving_average: Vec<OHLC> = (0..number_shifts)
                .map(|i_shift| {
                    (prefix_sum[i_shift].clone() - prefix_sum[i_shift - window].clone()) / *window
                })
                .rev()
                .collect();

            means.insert(window.clone(), moving_average);
        }

        Ok(means)
    }
}
