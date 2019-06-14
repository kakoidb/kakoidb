use crate::entities::point::StoragePoint;
use crate::entities::point::{NewPoint, Point, QueryOptions};
use crate::entities::series::{NewSeries, Series};
use bincode::{deserialize, serialize};
use rocksdb::{Direction, IteratorMode, WriteBatch, DB};
use std::fmt;
use std::path::Path;
use std::str;

#[derive(PartialEq, Debug, Clone)]
pub enum Error {
  SeriesMissing(String),
  Inner(rocksdb::Error),
}

impl fmt::Display for Error {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    match self {
      Error::Inner(error) => error.fmt(f),
      Error::SeriesMissing(series_name) => write!(f, "Series \"{}\" do not exist", series_name),
    }
  }
}

pub struct Database {
  db: DB,
}

impl Database {
  pub fn open<P: AsRef<Path>>(path: P) -> Database {
    Database {
      db: DB::open_default(path).unwrap(),
    }
  }

  fn iter_prefix(&self, key_prefix: String) -> impl Iterator<Item = (Box<[u8]>, Box<[u8]>)> + '_ {
    let key_prefix_bytes = key_prefix.into_bytes();
    let prefix_length = key_prefix_bytes.len();

    self
      .db
      .iterator(IteratorMode::From(&key_prefix_bytes, Direction::Forward))
      .take_while(move |(key, _)| &key[..prefix_length] == key_prefix_bytes.as_slice())
  }

  fn iter_series(&self) -> impl Iterator<Item = (Box<[u8]>, Box<[u8]>)> + '_ {
    self.iter_prefix("series::".to_string())
  }

  fn iter_points_serialized(
    &self,
    series_name: &str,
    options: Option<QueryOptions>,
  ) -> impl Iterator<Item = (Box<[u8]>, Box<[u8]>)> + '_ {
    let options = options.unwrap_or(Default::default());
    let start_key = match options.since {
      Some(since) => format!("points::{}::{}", series_name, since.to_rfc3339()),
      None => format!("points::{}", series_name),
    }
    .into_bytes();
    let end_key = match options.until {
      Some(until) => format!("points::{}::{}", series_name, until.to_rfc3339()),
      None => format!("points:;{}", series_name),
    }
    .into_bytes();

    self
      .db
      .iterator(IteratorMode::From(&start_key, Direction::Forward))
      .take_while(move |(key, _)| &**key <= end_key.as_slice())
  }

  pub fn iter_points(
    &self,
    series_name: &str,
    options: Option<QueryOptions>,
  ) -> impl Iterator<Item = Point> + '_ {
    let prefix_length = format!("points::{}::", series_name).len();
    self
      .iter_points_serialized(&series_name, options.clone())
      .filter_map(move |(key, value)| {
        let key = String::from_utf8_lossy(&key[prefix_length..]);

        Some(Point {
          time: match key.parse() {
            Ok(time) => time,
            Err(err) => {
              warn!(
                "Could not parse key \"{}\" as date. It is excluded from the result",
                key
              );
              debug!(
                "Parse error: {:?}",
                err
              );
              return None;
            }
          },
          value: match deserialize::<StoragePoint>(&value) {
            Ok(point) => point.value,
            Err(err) => {
              warn!(
                "Could not parse value of key \"{}\" as a StoragePoint. It is excluded from the result",
                key
              );
              debug!(
                "Parse error: {:?}",
                err
              );
              return None;
            }
          },
        })
      })
  }

  pub fn list_series(&self) -> Result<Vec<Series>, Error> {
    Ok(
      self
        .iter_series()
        .map(|(_, value)| deserialize(&value).unwrap())
        .collect(),
    )
  }

  pub fn get_series(&self, name: &str) -> Result<Option<Series>, rocksdb::Error> {
    match self.db.get(&format!("series::{}", name).into_bytes())? {
      Some(series) => Ok(Some(deserialize(&series).unwrap())),
      None => Ok(None),
    }
  }

  pub fn create_series(&self, new_series: NewSeries) -> Result<Series, rocksdb::Error> {
    let series = Series::from(new_series);
    self.db.put(
      &format!("series::{}", &series.name).into_bytes(),
      &serialize(&series).unwrap(),
    )?;
    Ok(series)
  }

  pub fn delete_series(&self, series_name: &str) -> Result<(), rocksdb::Error> {
    let points = self.iter_points_serialized(series_name, None);

    let mut batch = WriteBatch::default();

    batch.delete(&format!("series::{}", series_name).into_bytes())?;

    for (point, _) in points {
      batch.delete(&point)?;
    }

    self.db.write(batch)
  }

  pub fn query(
    &self,
    series_name: &str,
    options: Option<QueryOptions>,
  ) -> Result<Vec<Point>, Error> {
    let mut points = self.iter_points(&series_name, options.clone());
    let options = options.unwrap_or(Default::default());

    let points = match options
      .aggregate
      .and_then(|a| points.next().map(|p| (a, p)))
    {
      Some((aggregation, first)) => {
        let duration = (&aggregation.over).into();
        let mut count = 1;
        let mut start_time = first.time;
        let mut value = first.value;

        let mut points: Vec<Point> = points
          .filter_map(|point| {
            if point.time - start_time >= duration {
              let aggregated_point = Point {
                time: start_time,
                value: aggregation.function.finish(value, count),
              };
              count = 1;
              start_time = point.time;
              value = point.value;
              Some(aggregated_point)
            } else {
              count += 1;
              value = aggregation.function.reduce(value, point.value);
              None
            }
          })
          .collect();

        points.push(Point {
          time: start_time,
          value: aggregation.function.finish(value, count),
        });

        points
      }
      None => points.collect(),
    };

    Ok(points)
  }

  pub fn write(&mut self, batch: WriteBatch) -> Result<(), rocksdb::Error> {
    self.db.write(batch)
  }

  pub fn delete_by_query(
    &mut self,
    series_name: &str,
    options: Option<QueryOptions>,
  ) -> Result<(), rocksdb::Error> {
    let mut batch = WriteBatch::default();
    let points = self.iter_points_serialized(series_name, options);

    for (point, _) in points {
      println!("Deleting {}", str::from_utf8(&point).unwrap());
      batch.delete(&point)?;
    }

    self.db.write(batch)
  }

  pub fn create_point(&self, series_name: &str, new_point: NewPoint) -> Result<Point, Error> {
    if self
      .db
      .get(&format!("series::{}", series_name).into_bytes())
      .map_err(Error::Inner)?
      .is_none()
    {
      return Err(Error::SeriesMissing(series_name.to_string()));
    }

    let point = StoragePoint {
      value: new_point.value,
    };
    self
      .db
      .put(
        &format!("points::{}::{}", &series_name, &new_point.time.to_rfc3339()).into_bytes(),
        &serialize(&point).unwrap(),
      )
      .map_err(Error::Inner)?;
    Ok(Point {
      time: new_point.time,
      value: new_point.value,
    })
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::entities::point::{NewPoint, Point};
  use crate::entities::series::{NewSeries, Series};
  use chrono::Utc;
  use tempdir::TempDir;

  fn db_test<T>(test: T) -> ()
  where
    T: FnOnce(&Database) -> (),
  {
    let tmp_dir = TempDir::new("kakoi_db_test").unwrap();
    let db_path = tmp_dir.path().join("db");
    let db = Database::open(db_path);

    test(&db);

    tmp_dir.close().unwrap();
  }

  #[test]
  fn test_create_series_basic() {
    db_test(|db| {
      let created_series = db.create_series(NewSeries {
        name: "test-series".to_string(),
        retention_policy: None,
      });

      assert_eq!(
        created_series,
        Ok(Series::from(NewSeries {
          name: "test-series".to_string(),
          retention_policy: None,
        }))
      );

      let all_series = db.list_series();

      assert_eq!(
        all_series,
        Ok(vec![Series::from(NewSeries {
          name: "test-series".to_string(),
          retention_policy: None,
        })])
      );
    });
  }

  #[test]
  fn test_create_point_basic() {
    db_test(|db| {
      db.create_series(NewSeries {
        name: "test-series".to_string(),
        retention_policy: None,
      })
      .unwrap();

      let now = Utc::now();
      let created_point = db.create_point(
        "test-series",
        NewPoint {
          time: now,
          value: 1.0,
        },
      );

      assert_eq!(
        created_point,
        Ok(Point {
          time: now,
          value: 1.0
        })
      );

      let all_points = db.query("test-series", None);

      assert_eq!(
        all_points,
        Ok(vec![Point {
          time: now,
          value: 1.0
        }])
      );
    });
  }

  #[test]
  fn test_create_point_no_series() {
    db_test(|db| {
      let now = Utc::now();
      let created_point = db.create_point(
        "test-series",
        NewPoint {
          time: now,
          value: 1.0,
        },
      );

      assert_eq!(
        created_point,
        Err(Error::SeriesMissing("test-series".to_string()))
      );

      let all_points = db.query("test-series", None);

      assert_eq!(all_points, Ok(vec![]));
    });
  }
}
