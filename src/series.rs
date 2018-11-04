#[derive(Serialize, Deserialize, PartialEq, Debug, GraphQLObject)]
#[graphql(description="A collection of data over time")]
pub struct Series {
    pub name: String,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, GraphQLInputObject)]
#[graphql(description="A collection of data over time")]
pub struct NewSeries {
    pub name: String,
}