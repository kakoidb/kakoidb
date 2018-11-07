use database::Database;
use entities::point::{NewPoint, Point, QueryOptions};
use entities::series::{NewSeries, Series};
use juniper::FieldResult;
use std::net::IpAddr;
use std::sync::{Arc, RwLock};
use warp::{http::Response, log, Filter};

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Default)]
pub struct ApiConfig {
    host: Option<String>,
    port: Option<u16>,
}

struct Context {
    db: Arc<RwLock<Database>>,
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
        Ok(db.read().unwrap().list_series()?)
    }

    field series(&executor, name: String) -> FieldResult<Option<Series>> {
        let db = &executor.context().db;
        Ok(db.read().unwrap().get_series(name)?)
    }

    field query(&executor, series_name: String, options: Option<QueryOptions>) -> FieldResult<Vec<Point>> {
        let db = &executor.context().db;
        Ok(db.read().unwrap().query(&series_name, options)?)
    }
});

struct Mutation;

graphql_object!(Mutation: Context |&self| {

    field create_series(&executor, new_series: NewSeries) -> FieldResult<Series> {
        let db = &executor.context().db;
        Ok(db.read().unwrap().create_series(new_series)?)
    }

    field delete_series(&executor, series_name: String) -> FieldResult<Option<Series>> {
        let db = &executor.context().db;
        db.read().unwrap().delete_series(&series_name)?;
        Ok(None)
    }

    field create_point(&executor, series_name: String, new_point: NewPoint) -> FieldResult<Point> {
        let db = &executor.context().db;
        Ok(db.read().unwrap().create_point(&series_name, new_point)?)
    }
});

type Schema = juniper::RootNode<'static, Query, Mutation>;

fn schema() -> Schema {
    Schema::new(Query, Mutation)
}

pub fn start_api(config: &Option<ApiConfig>, db: Arc<RwLock<Database>>) {
    let config = config.as_ref().map_or_else(Default::default, Clone::clone);
    let host: IpAddr = config
        .host
        .unwrap_or("127.0.0.1".to_string())
        .parse()
        .unwrap();
    let port = config.port.unwrap_or(7766);
    let log = log("api");

    let homepage = warp::path::end().map(|| {
        Response::builder()
            .header("content-type", "text/html")
            .body(format!(
                "<html><h1>juniper_warp</h1><div>visit <a href=\"/graphiql\">/graphiql</a></html>"
            ))
    });

    info!("Listening on {}:{}", host, port);

    let state = warp::any().map(move || Context { db: db.clone() });
    let graphql_filter = juniper_warp::make_graphql_filter(schema(), state.boxed());

    warp::serve(
        warp::get2()
            .and(warp::path("graphiql"))
            .and(juniper_warp::graphiql_handler("/graphql"))
            .or(homepage)
            .or(warp::path("graphql").and(graphql_filter))
            .with(log),
    ).run((host, port));
}
