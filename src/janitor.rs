use bincode::{deserialize, serialize};
use chrono::prelude::*;
use chrono::Duration;
use database::Database;
use entities::aggregation::NewAggregationStrategy;
use entities::duration::TimeUnit;
use entities::point::{Point, QueryOptions};
use entities::series::RetentionPolicy;
use rocksdb::WriteBatch;
use std::str;
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::Instant;
use tokio::prelude::*;
use tokio::timer::Interval;

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct JanitorConfig {
  interval: ::entities::duration::Duration,
}

impl Default for JanitorConfig {
  fn default() -> JanitorConfig {
    JanitorConfig {
      interval: ::entities::duration::Duration {
        time_unit: TimeUnit::Minutes,
        value: 5,
      },
    }
  }
}

pub fn start_janitor(config: &Option<JanitorConfig>, db: Arc<RwLock<Database>>) {
  let config = config.as_ref().map_or_else(Default::default, Clone::clone);

  thread::spawn(move || {
    let duration = Duration::from(&config.interval).to_std().unwrap();
    let interval = Interval::new(Instant::now() + duration, duration);

    let task = interval
      .for_each(move |_| {
        info!("Running janitor");

        let series = { db.read().unwrap().list_series().unwrap() };

        let series = series.into_iter().filter_map(|series| {
          let name = series.name;
          series.retention_policy.map(|policy| (name, policy))
        });

        series.for_each(|(series_name, policy)| {
          debug!("Running janitor on series {}", series_name);
          garbage_collect_series(&db, &series_name, &policy).unwrap();
          compact_series(&db, &series_name, policy).unwrap();
        });

        future::done(Ok(()))
      }).map_err(|e| panic!("interval errored; err={:?}", e));

    tokio::run(task);
  });
}

fn garbage_collect_series(
  db: &Arc<RwLock<Database>>,
  series_name: &str,
  policy: &RetentionPolicy,
) -> Result<(), rocksdb::Error> {
  match policy.drop_after.as_ref() {
    Some(drop_after) => {
      let drop_until = Utc::now() - drop_after;
      trace!("Drop until {}", drop_until);
      db.write().unwrap().delete_by_query(
        &series_name,
        Some(QueryOptions::with(|options| {
          options.until = Some(drop_until);
        })),
      )
    }
    None => Ok(()),
  }
}

fn compact_series(
  db: &Arc<RwLock<Database>>,
  series_name: &str,
  policy: RetentionPolicy,
) -> Result<(), rocksdb::Error> {
  policy.compact.into_iter().try_fold(None, |from, compact| {
    let until = Some(Utc::now() - &compact.after);
    let mut db = db.write().unwrap();
    let aggregation_strategy = NewAggregationStrategy {
      over: compact.aggregate.over,
      function: compact.aggregate.function,
    };
    let query_options = QueryOptions {
      from: from,
      until: until,
      aggregate: Some(aggregation_strategy.clone()),
    };

    let mut points = db
      .iter_points(&series_name, Some(query_options))
      .map(|(key, value)| (key, deserialize::<Point>(&value).unwrap()));

    match points.next() {
      Some((first_key, first)) => {
        let duration = (&aggregation_strategy.over).into();
        let mut batch = WriteBatch::default();
        let mut count = 1;
        let mut start_key = first_key;
        let mut start_time = first.time;
        let mut value = first.value;

        for (key, point) in points {
          if point.time - start_time >= duration {
            let aggregated_point = Point {
              time: start_time,
              value: aggregation_strategy.function.finish(value, count),
            };

            count = 1;
            start_time = point.time;
            value = point.value;

            trace!(
              "creating aggregation {}",
              str::from_utf8(&start_key).unwrap()
            );
            batch.put(&start_key, &serialize(&aggregated_point).unwrap())?;
          } else {
            count += 1;
            value = aggregation_strategy.function.reduce(value, point.value);
            trace!("compacting {}", str::from_utf8(&key).unwrap());
            batch.delete(&key)?;
          }
        }

        trace!(
          "creating aggregation {}",
          str::from_utf8(&start_key).unwrap()
        );
        batch.put(
          &start_key,
          &serialize(&Point {
            time: start_time,
            value: aggregation_strategy.function.finish(value, count),
          }).unwrap(),
        )?;

        db.write(batch)
      }
      None => Ok(()),
    }.map(|_| until)
  })?;

  Ok(())
}
