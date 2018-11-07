use bincode::{deserialize, serialize};
use entities::point::{NewPoint, Point, QueryOptions};
use entities::series::{NewSeries, Series};
use rocksdb::{Direction, Error, IteratorMode, WriteBatch, DB};
use std::path::Path;
use std::str;

pub struct Database {
  db: DB,
}

impl Database {
  pub fn open<P: AsRef<Path>>(path: P) -> Database {
    Database {
      db: DB::open_default(path).unwrap(),
    }
  }

  fn iter_prefix(&self, key_prefix: String) -> impl Iterator<Item = (Box<[u8]>, Box<[u8]>)> {
    let key_prefix_bytes = key_prefix.into_bytes();
    let prefix_length = key_prefix_bytes.len();

    self
      .db
      .iterator(IteratorMode::From(&key_prefix_bytes, Direction::Forward))
      .take_while(move |(key, _)| &key[..prefix_length] == key_prefix_bytes.as_slice())
  }

  fn iter_series(&self) -> impl Iterator<Item = (Box<[u8]>, Box<[u8]>)> {
    self.iter_prefix("series::".to_string())
  }

  pub fn iter_points(
    &self,
    series_name: &str,
    options: Option<QueryOptions>,
  ) -> impl Iterator<Item = (Box<[u8]>, Box<[u8]>)> {
    let options = options.unwrap_or(Default::default());
    let start_key = match options.from {
      Some(from) => format!("points::{}::{}", series_name, from),
      None => format!("points::{}", series_name),
    }.into_bytes();
    let end_key = match options.until {
      Some(until) => format!("points::{}::{}", series_name, until),
      None => format!("points:;{}", series_name),
    };

    self
      .db
      .iterator(IteratorMode::From(&start_key, Direction::Forward))
      .take_while(move |(key, _)| str::from_utf8(key).unwrap().to_string() <= end_key)
  }

  pub fn list_series(&self) -> Result<Vec<Series>, Error> {
    Ok(
      self
        .iter_series()
        .map(|(_, value)| deserialize(&value).unwrap())
        .collect(),
    )
  }

  pub fn get_series(&self, name: String) -> Result<Option<Series>, Error> {
    match self.db.get(&format!("series::{}", name).into_bytes())? {
      Some(series) => Ok(Some(deserialize(&series).unwrap())),
      None => Ok(None),
    }
  }

  pub fn create_series(&self, new_series: NewSeries) -> Result<Series, Error> {
    let series = Series::from(new_series);
    self.db.put(
      &format!("series::{}", &series.name).into_bytes(),
      &serialize(&series).unwrap(),
    )?;
    Ok(series)
  }

  pub fn delete_series(&self, series_name: &str) -> Result<(), Error> {
    let points = self.iter_points(series_name, None);

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
    let mut points = self
      .iter_points(&series_name, options.clone())
      .map(|(_, value)| deserialize::<Point>(&value).unwrap());
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
          }).collect();

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

  pub fn write(&mut self, batch: WriteBatch) -> Result<(), Error> {
    self.db.write(batch)
  }

  pub fn delete_by_query(
    &mut self,
    series_name: &str,
    options: Option<QueryOptions>,
  ) -> Result<(), Error> {
    let mut batch = WriteBatch::default();
    let points = self.iter_points(series_name, options);

    for (point, _) in points {
      println!("Deleting {}", str::from_utf8(&point).unwrap());
      batch.delete(&point)?;
    }

    self.db.write(batch)
  }

  pub fn create_point(&self, series_name: &str, new_point: NewPoint) -> Result<Point, Error> {
    let point = Point {
      time: new_point.time,
      value: new_point.value,
    };
    self.db.put(
      &format!("points::{}::{}", &series_name, &point.time).into_bytes(),
      &serialize(&point).unwrap(),
    )?;
    Ok(point)
  }
}
