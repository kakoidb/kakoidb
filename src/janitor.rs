use bincode::serialize;
use chrono::prelude::*;
use chrono::Duration;
use database::Database;
use entities::aggregation::NewAggregationStrategy;
use entities::point::{Point, QueryOptions};
use entities::series::RetentionPolicy;
use rocksdb::WriteBatch;
use std::str;
use std::sync::{Arc, RwLock, RwLockWriteGuard};
use std::thread;
use std::time::Instant;
use tokio::prelude::*;
use tokio::timer::Interval;

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct JanitorConfig {
  interval: String,
}

impl Default for JanitorConfig {
  fn default() -> JanitorConfig {
    JanitorConfig {
      interval: "5 minutes".to_string(),
    }
  }
}

pub fn start_janitor(
  config: &Option<JanitorConfig>,
  db: Arc<RwLock<Database>>,
) -> Result<(), &str> {
  let config = config.as_ref().map_or_else(Default::default, Clone::clone);
  let interval = ::entities::duration::Duration::from_string(&config.interval)
    .ok_or("Invalid duration for interval")?;

  thread::spawn(move || {
    let duration = Duration::from(&interval).to_std().unwrap();
    let interval = Interval::new(Instant::now() + duration, duration);

    let task = interval
      .for_each(move |_| {
        info!("Running janitor");

        let mut db_mut = db.write().unwrap();
        let series = db_mut.list_series().unwrap();

        let series = series.into_iter().filter_map(|series| {
          let name = series.name;
          series.retention_policy.map(|policy| (name, policy))
        });

        series.for_each(|(series_name, policy)| {
          debug!("Running janitor on series {}", series_name);
          garbage_collect_series(&mut db_mut, &series_name, &policy).unwrap();
          compact_series(&mut db_mut, &series_name, policy).unwrap();
        });

        future::done(Ok(()))
      }).map_err(|e| panic!("interval errored; err={:?}", e));

    tokio::run(task);
  });

  Ok(())
}

fn garbage_collect_series(
  db: &mut RwLockWriteGuard<Database>,
  series_name: &str,
  policy: &RetentionPolicy,
) -> Result<(), rocksdb::Error> {
  match policy.drop_after.as_ref() {
    Some(drop_after) => {
      let drop_until = Utc::now() - drop_after;
      trace!("Drop until {}", drop_until);
      db.delete_by_query(
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
  db: &mut RwLockWriteGuard<Database>,
  series_name: &str,
  policy: RetentionPolicy,
) -> Result<(), rocksdb::Error> {
  policy
    .compact
    .into_iter()
    .try_fold(None, |since, compact| {
      let until = Some(Utc::now() - &compact.after);
      debug!("range {:?} -> {:?}", &since, &until);
      let aggregation_strategy = NewAggregationStrategy {
        over: compact.aggregate.over,
        function: compact.aggregate.function,
      };
      let query_options = QueryOptions {
        since: since,
        until: until,
        aggregate: Some(aggregation_strategy.clone()),
      };

      let mut points = db.iter_points(&series_name, Some(query_options));

      match points.next() {
        Some(first) => {
          let duration = (&aggregation_strategy.over).into();
          let mut batch = WriteBatch::default();
          let mut count = 1;
          let mut start_time = first.time;
          let mut value = first.value;

          for point in points {
            if point.time - start_time >= duration {
              let aggregated_point = Point {
                time: start_time,
                value: aggregation_strategy.function.finish(value, count),
              };

              count = 1;
              start_time = point.time;
              value = point.value;

              debug!("creating aggregation {}", &start_time);
              batch.put(
                &format!("points::{}::{}", &series_name, &start_time).into_bytes(),
                &serialize(&aggregated_point).unwrap(),
              )?;
            } else {
              count += 1;
              value = aggregation_strategy.function.reduce(value, point.value);
              debug!("compacting {}", &point.time);
              batch.delete(&format!("points::{}::{}", &series_name, &point.time).into_bytes())?;
            }
          }

          debug!("creating aggregation2 {}", &start_time);
          batch.put(
            &format!("points::{}::{}", &series_name, &start_time).into_bytes(),
            &serialize(&Point {
              time: start_time,
              value: aggregation_strategy.function.finish(value, count),
            }).unwrap(),
          )?;

          db.write(batch)?;
          Ok(())
        }
        None => Ok(()),
      }.map(|_| until)
    })?;

  Ok(())
}
