#![feature(async_await)]

#[macro_use]
extern crate diesel;

use diesel::{
    prelude::*,
    r2d2::{ConnectionManager, Pool},
};
use tokio_diesel::*;
use uuid::Uuid;

// Schema
table! {
    users (id) {
        id -> Uuid,
    }
}

#[runtime::main(runtime_tokio::Tokio)]
async fn main() -> Result<(), failure::Error> {
    // Connect
    let manager =
        ConnectionManager::<PgConnection>::new("postgres://postgres@localhost/tokio_diesel__test");
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
