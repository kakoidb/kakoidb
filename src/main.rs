extern crate env_logger;
#[macro_use]
extern crate log as irrelevant_log;
#[macro_use]
extern crate juniper;
extern crate juniper_warp;
extern crate rocksdb;
extern crate warp;
#[macro_use]
extern crate serde_derive;
extern crate bincode;
extern crate chrono;
use std::sync::Arc;

use juniper::FieldResult;
use warp::{http::Response, log, Filter};

mod database;
mod series;
use database::Database;
use series::{NewPoint, NewSeries, Point, QueryOptions, Series};

struct Context {
    db: Arc<Database>,
}

impl juniper::Context for Context {}

// graphql_object!(Series: Context |&self| {

//     field query(&executor, series_name: String) -> FieldResult<Vec<Point>> {
//         let db = &executor.context().db;
//         Ok(db.query(series_name)?)
//     }
// });

struct Query;

graphql_object!(Query: Context |&self| {

    field apiVersion() -> &str {
        "0.1"
    }

    field list_series(&executor) -> FieldResult<Vec<Series>> {
        let db = &executor.context().db;
        Ok(db.list_series()?)
    }

    field series(&executor, name: String) -> FieldResult<Option<Series>> {
        let db = &executor.context().db;
        Ok(db.get_series(name)?)
    }

    field query(&executor, series_name: String, options: Option<QueryOptions>) -> FieldResult<Vec<Point>> {
        let db = &executor.context().db;
        Ok(db.query(series_name, options)?)
    }
});

struct Mutation;

graphql_object!(Mutation: Context |&self| {

    field create_series(&executor, new_series: NewSeries) -> FieldResult<Series> {
        let db = &executor.context().db;
        Ok(db.create_series(new_series)?)
    }

    field delete_series(&executor, series_name: String) -> FieldResult<Option<Series>> {
        let db = &executor.context().db;
        db.delete_series(series_name)?;
        Ok(None)
    }

    field create_point(&executor, series_name: String, new_point: NewPoint) -> FieldResult<Point> {
        let db = &executor.context().db;
        Ok(db.create_point(series_name, new_point)?)
    }
});

type Schema = juniper::RootNode<'static, Query, Mutation>;

fn schema() -> Schema {
    Schema::new(Query, Mutation)
}

fn main() {
    let db = Arc::new(database::Database::open("./test.db".to_string()));
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

    let state = warp::any().map(move || Context { db: db.clone() });
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
