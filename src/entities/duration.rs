use chrono::prelude::*;
use juniper::{
  parser::{ParseError, ScalarToken, Token},
  ParseScalarResult, Value,
};
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
    let parts = s.split(' ').collect::<Vec<&str>>();
    if parts.len() != 2 {
      return None;
    }
    let value = parts[0].parse::<i32>();
    let unit = TimeUnit::from(parts[1]);

    match (value, unit) {
      (Ok(value), Some(unit)) => Some(Duration {
        value,
        time_unit: unit,
      }),
      _ => None,
    }
  }
}

impl<'a> From<&'a Duration> for chrono::Duration {
  fn from(duration: &Duration) -> Self {
    match duration.time_unit {
      TimeUnit::Minutes => chrono::Duration::minutes(i64::from(duration.value)),
      TimeUnit::Hours => chrono::Duration::hours(i64::from(duration.value)),
      TimeUnit::Days => chrono::Duration::days(i64::from(duration.value)),
      TimeUnit::Weeks => chrono::Duration::weeks(i64::from(duration.value)),
      TimeUnit::Years => chrono::Duration::days(i64::from(365 * duration.value)),
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

graphql_scalar!(Duration as "Duration" where Scalar = <S> {
    description: "duration of time"

    resolve(&self) -> Value {
      Value::scalar(format!("{} {}", self.value, self.time_unit))
    }

    from_input_value(v: &InputValue) -> Option<Duration> {
      v.as_scalar_value::<String>()
        .and_then(|s| Duration::from_string(s))
    }

    from_str<'a>(value: ScalarToken<'a>) -> ParseScalarResult<'a, S> {
      if let ScalarToken::String(value) =  value {
        Ok(S::from(value.to_owned()))
      } else {
        Err(ParseError::UnexpectedToken(Token::Scalar(value)))
      }
    }
});

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_string_parse() {
    assert_eq!(
      Duration::from_string("1 minute"),
      Some(Duration {
        time_unit: TimeUnit::Minutes,
        value: 1
      })
    );
    assert_eq!(
      Duration::from_string("2 minutes"),
      Some(Duration {
        time_unit: TimeUnit::Minutes,
        value: 2
      })
    );
    assert_eq!(
      Duration::from_string("1 hour"),
      Some(Duration {
        time_unit: TimeUnit::Hours,
        value: 1
      })
    );
    assert_eq!(
      Duration::from_string("5 hours"),
      Some(Duration {
        time_unit: TimeUnit::Hours,
        value: 5
      })
    );
    assert_eq!(
      Duration::from_string("1 day"),
      Some(Duration {
        time_unit: TimeUnit::Days,
        value: 1
      })
    );
    assert_eq!(
      Duration::from_string("10 days"),
      Some(Duration {
        time_unit: TimeUnit::Days,
        value: 10
      })
    );
    assert_eq!(
      Duration::from_string("1 week"),
      Some(Duration {
        time_unit: TimeUnit::Weeks,
        value: 1
      })
    );
    assert_eq!(
      Duration::from_string("12 weeks"),
      Some(Duration {
        time_unit: TimeUnit::Weeks,
        value: 12
      })
    );
    assert_eq!(
      Duration::from_string("1 year"),
      Some(Duration {
        time_unit: TimeUnit::Years,
        value: 1
      })
    );
    assert_eq!(
      Duration::from_string("42 years"),
      Some(Duration {
        time_unit: TimeUnit::Years,
        value: 42
      })
    );
  }

  #[test]
  fn test_minute_arithmetic() {
    assert_eq!(
      Utc.ymd(2014, 7, 8).and_hms(9, 10, 11) + &Duration::from_string("5 minutes").unwrap(),
      Utc.ymd(2014, 7, 8).and_hms(9, 15, 11)
    );

    assert_eq!(
      Utc.ymd(2014, 7, 8).and_hms(9, 10, 11) - &Duration::from_string("15 minutes").unwrap(),
      Utc.ymd(2014, 7, 8).and_hms(8, 55, 11)
    );
  }

  #[test]
  fn test_hour_arithmetic() {
    assert_eq!(
      Utc.ymd(2014, 7, 8).and_hms(9, 10, 11) + &Duration::from_string("5 hours").unwrap(),
      Utc.ymd(2014, 7, 8).and_hms(14, 10, 11)
    );

    assert_eq!(
      Utc.ymd(2014, 7, 8).and_hms(9, 10, 11) - &Duration::from_string("15 hours").unwrap(),
      Utc.ymd(2014, 7, 7).and_hms(18, 10, 11)
    );
  }

  #[test]
  fn test_day_arithmetic() {
    assert_eq!(
      Utc.ymd(2014, 7, 8).and_hms(9, 10, 11) + &Duration::from_string("5 days").unwrap(),
      Utc.ymd(2014, 7, 13).and_hms(9, 10, 11)
    );

    assert_eq!(
      Utc.ymd(2014, 7, 8).and_hms(9, 10, 11) - &Duration::from_string("15 days").unwrap(),
      Utc.ymd(2014, 6, 23).and_hms(9, 10, 11)
    );
  }

  #[test]
  fn test_week_arithmetic() {
    assert_eq!(
      Utc.ymd(2014, 7, 8).and_hms(9, 10, 11) + &Duration::from_string("5 weeks").unwrap(),
      Utc.ymd(2014, 8, 12).and_hms(9, 10, 11)
    );

    assert_eq!(
      Utc.ymd(2014, 7, 8).and_hms(9, 10, 11) - &Duration::from_string("15 weeks").unwrap(),
      Utc.ymd(2014, 3, 25).and_hms(9, 10, 11)
    );
  }

  #[test]
  fn test_year_arithmetic() {
    assert_eq!(
      Utc.ymd(2014, 7, 8).and_hms(9, 10, 11) + &Duration::from_string("5 years").unwrap(),
      Utc.ymd(2019, 7, 7).and_hms(9, 10, 11)
    );

    assert_eq!(
      Utc.ymd(2014, 7, 8).and_hms(9, 10, 11) - &Duration::from_string("15 years").unwrap(),
      Utc.ymd(1999, 7, 12).and_hms(9, 10, 11)
    );
  }
}
