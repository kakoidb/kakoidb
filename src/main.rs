extern crate env_logger;
#[macro_use]
extern crate log as irrelevant_log;
#[macro_use] extern crate juniper;
extern crate juniper_warp;
extern crate warp;
extern crate rocksdb;
#[macro_use] extern crate serde_derive;
extern crate bincode;

use juniper::{FieldResult, Variables};
use warp::{http::Response, log, Filter};

mod database;
mod series;
use database::{Database};
use series::{Series, NewSeries};


// #[derive(GraphQLEnum)]
// enum Episode {
//     NewHope,
//     Empire,
//     Jedi,
// }

struct Context {
    db: Database
}

impl juniper::Context for Context {}

struct Query;

graphql_object!(Query: Context |&self| {

    field apiVersion() -> &str {
        "0.1"
    }

    // Arguments to resolvers can either be simple types or input objects.
    // The executor is a special (optional) argument that allows accessing the context.
    field series(&executor, name: String) -> FieldResult<Option<Series>> {
        // Get the context from the executor.
        let context = executor.context();
        // // Get a db connection.
        // let connection = context.pool.get_connection()?;
        // Execute a db query.
        // Note the use of `?` to propagate errors.
        // let human = connection.find_human(&id)?;
        // Return the result.
        let series = context.db.get_series(name)?;
        Ok(series)
    }
});

struct Mutation;

graphql_object!(Mutation: Context |&self| {

    field createSeries(&executor, new_series: NewSeries) -> FieldResult<Series> {
        // let db = executor.context().pool.get_connection()?;
        // let human: Human = db.insert_human(&new_human)?;
        // Ok(human)
        // let context = executor.context();
        let db = &executor.context().db;
        Ok(db.create_series(new_series)?)
        // Ok(Series {
        //     name: new_series.name
        // })
    }
});

type Schema = juniper::RootNode<'static, Query, Mutation>;

fn schema() -> Schema {
    Schema::new(Query, Mutation)
}

fn main() {
    ::std::env::set_var("RUST_LOG", "warp_server");
    env_logger::init();

    let log = log("warp_server");

    let homepage = warp::path::end().map(|| {
        Response::builder()
            .header("content-type", "text/html")
            .body(format!(
                "<html><h1>juniper_warp</h1><div>visit <a href=\"/graphiql\">/graphiql</a></html>"
            ))
    });

    info!("Listening on 127.0.0.1:8080");

    let state = warp::any().map(move || Context {
        db: database::Database::open("./test.db".to_string()),
    });
    let graphql_filter = juniper_warp::make_graphql_filter(schema(), state.boxed());

    warp::serve(
        warp::get2()
            .and(warp::path("graphiql"))
            .and(juniper_warp::graphiql_handler("/graphql"))
            .or(homepage)
            .or(warp::path("graphql").and(graphql_filter))
            .with(log),
    ).run(([127, 0, 0, 1], 8080));
}