use rbtree::RBTree;

use std::{
    collections::HashMap,
    ops::{Add, AddAssign, Div, Sub},
    sync::RwLock,
};

#[derive(Debug, Clone, PartialEq)]
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

impl OHLC {
    fn new_zero() -> OHLC {
        OHLC {
            time: 0,
            open: 0.0,
            high: 0.0,
            low: 0.0,
            close: 0.0,
            vwap: 0.0,
            volume: 0.0,
            count: 0,
        }
    }
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
                None => return Ok(None),
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
            .map(|(_, element)| {
                let cloned = sum_buffer.clone();
                sum_buffer += element.clone();
                return cloned;
            })
            .collect();

        prefix_sum.push(sum_buffer);

        let mut means = HashMap::new();

        for window in &windows {
            let number_shifts = self.universe_window - (*window - 1);

            let moving_average: Vec<OHLC> = (0..number_shifts)
                .map(|i_shift| {
                    (prefix_sum[i_shift + *window].clone() - prefix_sum[i_shift].clone()) / *window
                })
                .collect();

            means.insert(window.clone(), moving_average);
        }

        Ok(means)
    }

    pub async fn deviations(
        &self,
        means: &HashMap<usize, Vec<OHLC>>,
    ) -> Result<HashMap<usize, Vec<OHLC>>, String> {
        for window in means.keys() {
            if *window > self.universe_window {
                return Err(format!(
                    "Tried to analyse moving window ({:}) that was larger than universe window {:}.",
                    window, self.universe_window
                ));
            }

            if means[window].len() != (self.universe_window - *window + 1) {
                return Err(format!(
                    "Means passed to deviation calculation don't have correct length for window {:}: expected {:}, found {:}",
                    *window,
                    self.universe_window - *window + 1,
                    means[window].len()
                ));
            }
        }

        let read_universe = match self.universe.read() {
            Ok(read_lock) => read_lock,
            Err(error) => return Err(format!("{:?}", error)),
        };

        let mut cloned: HashMap<usize, Vec<OHLC>> = means
            .iter()
            .map(|(window, values)| (window.clone(), vec![OHLC::new_zero(); values.len()]))
            .collect();

        for (index, (_, ohlc)) in read_universe.iter().enumerate() {
            for (window, values) in means {
                let deviation = ohlc.clone() - values[index].clone();
                let _ = (0..*window)
                    .map(|shift| cloned[window][index + shift] += deviation.clone())
                    .collect::<Vec<_>>();
            }
        }

        Err("Not implemented yet".to_string())
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    fn zero_ohlc() -> OHLC {
        OHLC::new_zero()
    }

    fn const_ohlc(val: f32) -> OHLC {
        OHLC {
            time: val as i64,
            open: val,
            high: val,
            low: val,
            close: val,
            vwap: val,
            volume: val,
            count: val as i64,
        }
    }

    async fn analytical_case(universe: usize) -> MovingStatistics {
        let mut stats = MovingStatistics::new(universe);
        for i_universe in 0..universe {
            let _ = stats.update(const_ohlc(i_universe as f32)).await;
        }
        stats
    }

    #[tokio::test]
    async fn test_vanilla_update() {
        let mut stats = MovingStatistics::new(10);

        assert!(stats.update(zero_ohlc()).await == Ok(None));
        assert!(stats.update(zero_ohlc()).await == Ok(None));
    }

    #[tokio::test]
    async fn test_null_universe_update() {
        let mut stats = MovingStatistics::new(0);

        let updated = stats.update(zero_ohlc()).await;
        assert!(updated.is_ok());
        assert!(updated == Ok(None));
    }

    #[tokio::test]
    async fn test_one_universe_update() {
        let mut stats = MovingStatistics::new(1);

        let updated = stats.update(zero_ohlc()).await;
        assert!(updated.is_ok());
        assert!(updated == Ok(None));

        let updated = stats.update(zero_ohlc()).await;
        assert!(updated == Ok(Some(zero_ohlc())));
    }

    #[tokio::test]
    async fn test_analytical_analysis() {
        let universe = 100;
        let stats = analytical_case(universe).await;

        let means_result = stats.means(vec![1, 2, 5, 10, 100]).await;
        assert!(means_result.is_ok());
        let means = means_result.unwrap();

        assert!(means.len() == 5);

        assert!(means.contains_key(&1));
        assert!(
            means[&1]
                == (0..universe)
                    .map(|val| const_ohlc(val as f32))
                    .collect::<Vec<_>>()
        );

        assert!(means.contains_key(&2));
        assert!(
            means[&2]
                == (0..(universe - 1))
                    .map(|val| (const_ohlc(val as f32) + const_ohlc((val + 1) as f32)) / 2)
                    .collect::<Vec<_>>()
        );

        assert!(means.contains_key(&5));
        assert!(
            means[&5]
                == (2..(universe - 2))
                    .map(|val| const_ohlc(val as f32))
                    .collect::<Vec<_>>()
        );

        assert!(means.contains_key(&10));
        assert!(
            means[&10]
                == (4..(universe - 5))
                    .map(|val| (const_ohlc(val as f32) + const_ohlc((val + 1) as f32)) / 2)
                    .collect::<Vec<_>>()
        );

        assert!(means.contains_key(&100));
        assert!(means[&100] == vec![const_ohlc(49.5)]);
    }

    #[tokio::test]
    async fn test_window_larger_than_universe() {
        let stats = analytical_case(10).await;

        assert!(stats.means(vec![11]).await.is_err());
    }
}
