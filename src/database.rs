use rocksdb::{DB, Error};
use series::{Series, NewSeries};

use bincode::{serialize, deserialize};

pub struct Database {
  db: DB
}

impl Database {
  pub fn open(path: String) -> Database {
    Database {
      db: DB::open_default(path).unwrap()
    }
  }

  pub fn get_series(&self, name: String) -> Result<Option<Series>, Error> {
    match self.db.get(&format!("series::{}", name).into_bytes())? {
      Some(series) => Ok(Some(deserialize(&series).unwrap())),
      None => Ok(None),
    }
  }

  pub fn create_series(&self, new_series: NewSeries) -> Result<Series, Error> {
    let series = Series {
      name: new_series.name
    };
    self.db.put(&format!("series::{}", &series.name).into_bytes(), &serialize(&series).unwrap())?;
    Ok(series)
  }
}

//  let db = DB::open_default("path/for/rocksdb/storage").unwrap();
//  db.put(b"my key", b"my value");
//  match db.get(b"my key") {
//     Ok(Some(value)) => println!("retrieved value {}", value.to_utf8().unwrap()),
//     Ok(None) => println!("value not found"),
//     Err(e) => println!("operational problem encountered: {}", e),
//  }
//  db.delete(b"my key").unwrap();