use crate::entities::aggregation::NewAggregationStrategy;
use chrono::{DateTime, Utc};

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct StoragePoint {
  pub value: f64,
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

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, GraphQLInputObject, Default)]
pub struct QueryOptions {
  pub since: Option<DateTime<Utc>>,
  pub until: Option<DateTime<Utc>>,
  pub aggregate: Option<NewAggregationStrategy>,
}

impl QueryOptions {
  pub fn with<F>(setter: F) -> QueryOptions
  where
    F: FnOnce(&mut QueryOptions) -> (),
  {
    let mut options: QueryOptions = Default::default();
    setter(&mut options);
    options
  }
}
