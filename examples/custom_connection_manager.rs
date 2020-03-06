#[macro_use]
extern crate diesel;

use diesel::{
    Connection,
    prelude::*,
    r2d2::{Pool, ManageConnection, Error as R2D2Error},
};
use std::{
    error::Error,
    marker::PhantomData,
};
use tokio_diesel::*;
use uuid::Uuid;

// Connection Manager
struct DynamicConnectionManager<Conn> {
    get_database_url: Box<dyn Fn() -> String + Send + Sync + 'static>,
    _marker: PhantomData<fn() -> Conn>,
}

impl<Conn> DynamicConnectionManager<Conn> {
    fn new(get_database_url: impl Fn() -> String + Send + Sync + 'static) -> Self {
        Self {
            get_database_url: Box::new(get_database_url),
            _marker: PhantomData,
        }
    }
}

impl<Conn> ManageConnection for DynamicConnectionManager<Conn>
where
    Conn: Connection + 'static,
{
    type Connection = Conn;
    type Error = R2D2Error;

    fn connect(&self) -> Result<Conn, R2D2Error> {
        Conn::establish(&(&self.get_database_url)()).map_err(R2D2Error::ConnectionError)
    }

    fn is_valid(&self, conn: &mut Conn) -> Result<(), Self::Error> {
        conn.execute("SELECT 1").map(|_| ()).map_err(R2D2Error::QueryError)
    }

    fn has_broken(&self, _conn: &mut Conn) -> bool {
        false
    }
}

// Schema
table! {
    users (id) {
        id -> Uuid,
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Connect
    let manager =
        DynamicConnectionManager::<PgConnection>::new(|| "postgres://postgres@localhost/tokio_diesel__test".into());
    let pool = Pool::builder().build(manager)?;

    // Add
    println!("add a user");
    diesel::insert_into(users::table)
        .values(users::id.eq(Uuid::new_v4()))
        .execute_async(&pool)
        .await?;

    // Count
    let num_users: i64 = users::table.count().get_result_async(&pool).await?;
    println!("now there are {:?} users", num_users);

    Ok(())
}
