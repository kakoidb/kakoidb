use chrono::prelude::*;
use juniper::Value;
use std::fmt;
use std::ops::{Add, Sub};

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, GraphQLEnum)]
pub enum TimeUnit {
  Minutes,
  Hours,
  Days,
  Weeks,
  Years,
}

impl TimeUnit {
  fn from(string: &str) -> Option<TimeUnit> {
    match string {
      "m" | "minute" | "minutes" => Some(TimeUnit::Minutes),
      "h" | "hour" | "hours" => Some(TimeUnit::Hours),
      "d" | "day" | "days" => Some(TimeUnit::Days),
      "w" | "week" | "weeks" => Some(TimeUnit::Weeks),
      "y" | "year" | "years" => Some(TimeUnit::Years),
      _ => None,
    }
  }
}

impl fmt::Display for TimeUnit {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    match self {
      TimeUnit::Minutes => write!(f, "minutes"),
      TimeUnit::Hours => write!(f, "hours"),
      TimeUnit::Days => write!(f, "days"),
      TimeUnit::Weeks => write!(f, "weeks"),
      TimeUnit::Years => write!(f, "years"),
    }
  }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct Duration {
  pub time_unit: TimeUnit,
  pub value: i32,
}

impl Duration {
  pub fn from_string(s: &str) -> Option<Duration> {
    let parts = s.split(" ").collect::<Vec<&str>>();
    if parts.len() != 2 {
      return None;
    }
    let value = parts[0].parse::<i32>();
    let unit = TimeUnit::from(parts[1]);

    match (value, unit) {
      (Ok(value), Some(unit)) => Some(Duration {
        value: value,
        time_unit: unit,
      }),
      _ => None,
    }
  }
}

impl<'a> From<&'a Duration> for chrono::Duration {
  fn from(duration: &Duration) -> Self {
    match duration.time_unit {
      TimeUnit::Minutes => chrono::Duration::minutes(duration.value as i64),
      TimeUnit::Hours => chrono::Duration::hours(duration.value as i64),
      TimeUnit::Days => chrono::Duration::days(duration.value as i64),
      TimeUnit::Weeks => chrono::Duration::weeks(duration.value as i64),
      TimeUnit::Years => chrono::Duration::days(365 * (duration.value as i64)),
    }
  }
}

impl<'a> Add<&'a Duration> for DateTime<Utc> {
  type Output = DateTime<Utc>;

  fn add(self, rhs: &Duration) -> DateTime<Utc> {
    let duration = chrono::Duration::from(rhs);

    self + duration
  }
}

impl<'a> Sub<&'a Duration> for DateTime<Utc> {
  type Output = DateTime<Utc>;

  fn sub(self, rhs: &Duration) -> DateTime<Utc> {
    let duration = chrono::Duration::from(rhs);

    self - duration
  }
}

graphql_scalar!(Duration {
    description: "duration of time"

    resolve(&self) -> Value {
      Value::String(format!("{} {}", self.value, self.time_unit))
    }

    from_input_value(v: &InputValue) -> Option<Duration> {
      v.as_string_value().and_then(Duration::from_string)
    }
});
