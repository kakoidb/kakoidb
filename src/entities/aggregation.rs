use crate::entities::duration::Duration;

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, GraphQLEnum)]
pub enum AggregationFunction {
  Oldest,
  Newest,
  Max,
  Min,
  Sum,
  Avg,
}

impl AggregationFunction {
  pub fn reduce(&self, prev: f64, current: f64) -> f64 {
    match self {
      AggregationFunction::Oldest => prev,
      AggregationFunction::Newest => current,
      AggregationFunction::Max => prev.max(current),
      AggregationFunction::Min => prev.min(current),
      AggregationFunction::Sum => prev + current,
      AggregationFunction::Avg => prev + current,
    }
  }

  pub fn finish(&self, value: f64, count: u64) -> f64 {
    match self {
      AggregationFunction::Oldest => value,
      AggregationFunction::Newest => value,
      AggregationFunction::Max => value,
      AggregationFunction::Min => value,
      AggregationFunction::Sum => value,
      AggregationFunction::Avg => value / (count as f64),
    }
  }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, GraphQLObject)]
pub struct AggregationStrategy {
  pub function: AggregationFunction,
  pub over: Duration,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, GraphQLInputObject)]
pub struct NewAggregationStrategy {
  pub function: AggregationFunction,
  pub over: Duration,
}

impl From<NewAggregationStrategy> for AggregationStrategy {
  fn from(strategy: NewAggregationStrategy) -> Self {
    Self {
      function: strategy.function,
      over: strategy.over,
    }
  }
}
