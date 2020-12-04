#[macro_use]
extern crate diesel;

use diesel::{
    prelude::*,
    r2d2::{ConnectionManager, Pool},
    sql_query,
};
use std::error::Error;
use tokio_diesel::*;
use uuid::Uuid;

// Schema
table! {
    users (id) {
        id -> Uuid,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_db_ops() -> Result<(), Box<dyn Error>> {
    let manager = ConnectionManager::<PgConnection>::new("postgres://postgres@localhost");
    let pool = Pool::builder().build(manager)?;

    let _ = sql_query(include_str!("./create_users.sql"))
        .execute_async(&pool)
        .await;

    // Add
    println!("add a user");
    diesel::insert_into(users::table)
        .values(users::id.eq(Uuid::new_v4()))
        .execute_async(&pool)
        .await?;

    // Count
    let num_users: i64 = users::table.count().get_result_async(&pool).await?;
    println!("now there are {:?} users", num_users);

    assert!(num_users > 0);

    Ok(())
}
