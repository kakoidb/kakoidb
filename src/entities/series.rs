use crate::entities::aggregation::{AggregationStrategy, NewAggregationStrategy};
use crate::entities::duration::Duration;

pub const CURRENT_STORAGE_VERSION: i32 = 0;

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, GraphQLObject)]
pub struct CompactionStrategy {
  pub after: Duration,
  pub aggregate: AggregationStrategy,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, GraphQLInputObject)]
pub struct NewCompactionStrategy {
  pub after: Duration,
  pub aggregate: NewAggregationStrategy,
}

impl From<NewCompactionStrategy> for CompactionStrategy {
  fn from(strategy: NewCompactionStrategy) -> Self {
    Self {
      after: strategy.after,
      aggregate: AggregationStrategy::from(strategy.aggregate),
    }
  }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, GraphQLObject)]
pub struct RetentionPolicy {
  pub compact: Vec<CompactionStrategy>,
  pub drop_after: Option<Duration>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, GraphQLInputObject, Default)]
pub struct NewRetentionPolicy {
  pub compact: Option<Vec<NewCompactionStrategy>>,
  pub drop_after: Option<Duration>,
}

impl From<NewRetentionPolicy> for RetentionPolicy {
  fn from(policy: NewRetentionPolicy) -> Self {
    RetentionPolicy {
      compact: policy.compact.map_or_else(
        || vec![],
        |compact| compact.into_iter().map(CompactionStrategy::from).collect(),
      ),
      drop_after: policy.drop_after,
    }
  }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, GraphQLObject)]
#[graphql(description = "A collection of data over time")]
pub struct Series {
  pub name: String,
  pub retention_policy: Option<RetentionPolicy>,
  #[graphql(skip)]
  pub storage_version: i32,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, GraphQLInputObject)]
#[graphql(description = "A collection of data over time")]
pub struct NewSeries {
  pub name: String,
  pub retention_policy: Option<NewRetentionPolicy>,
}

impl From<NewSeries> for Series {
  fn from(series: NewSeries) -> Self {
    Series {
      name: series.name,
      retention_policy: series.retention_policy.map(RetentionPolicy::from),
      storage_version: CURRENT_STORAGE_VERSION,
    }
  }
}
