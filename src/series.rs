use chrono::{DateTime, Utc};

#[derive(Serialize, Deserialize, PartialEq, Debug, GraphQLObject)]
#[graphql(description = "A collection of data over time")]
pub struct Series {
  pub name: String,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, GraphQLInputObject)]
#[graphql(description = "A collection of data over time")]
pub struct NewSeries {
  pub name: String,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, GraphQLObject)]
#[graphql(description = "Data at a specific time")]
pub struct Point {
  pub time: DateTime<Utc>,
  pub value: f64,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, GraphQLInputObject)]
#[graphql(description = "Data at a specific time")]
pub struct NewPoint {
  pub time: DateTime<Utc>,
  pub value: f64,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, GraphQLEnum)]
pub enum AggregationFunction {
  Oldest,
  Newest,
  Max,
  Min,
  Avg,
}

impl AggregationFunction {
  pub fn reduce(&self, prev: f64, current: f64) -> f64 {
    match self {
      AggregationFunction::Oldest => prev,
      AggregationFunction::Newest => current,
      AggregationFunction::Max => prev.max(current),
      AggregationFunction::Min => prev.min(current),
      AggregationFunction::Avg => prev + current,
    }
  }

  pub fn finish(&self, value: f64, count: u64) -> f64 {
    match self {
      AggregationFunction::Oldest => value,
      AggregationFunction::Newest => value,
      AggregationFunction::Max => value,
      AggregationFunction::Min => value,
      AggregationFunction::Avg => value / (count as f64),
    }
  }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, GraphQLEnum)]
pub enum TimeUnit {
  Minutes,
  Hours,
  Days,
  Years,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, GraphQLInputObject)]
pub struct Duration {
  pub time_unit: TimeUnit,
  pub value: i32,
}

impl From<Duration> for chrono::Duration {
  fn from(duration: Duration) -> Self {
    match duration.time_unit {
      TimeUnit::Minutes => chrono::Duration::minutes(duration.value as i64),
      TimeUnit::Hours => chrono::Duration::hours(duration.value as i64),
      TimeUnit::Days => chrono::Duration::days(duration.value as i64),
      TimeUnit::Years => chrono::Duration::days(365 * (duration.value as i64)),
    }
  }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, GraphQLInputObject)]
pub struct AggregationStrategy {
  pub function: AggregationFunction,
  pub duration: Duration,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, GraphQLInputObject, Default)]
pub struct QueryOptions {
  pub from: Option<DateTime<Utc>>,
  pub until: Option<DateTime<Utc>>,
  pub aggregation: Option<AggregationStrategy>,
}
