use async_trait::async_trait;
use diesel::{
    connection::SimpleConnection,
    dsl::Limit,
    query_dsl::{
        methods::{ExecuteDsl, LimitDsl, LoadQuery},
        RunQueryDsl,
    },
    r2d2::{ManageConnection, Pool},
    result::QueryResult,
    Connection,
};
use std::{error::Error as StdError, fmt};
use tokio::task;

pub type AsyncResult<R> = Result<R, AsyncError>;

#[derive(Debug)]
pub enum AsyncError {
    // Failed to checkout a connection
    Checkout(r2d2::Error),

    // The query failed in some way
    Error(diesel::result::Error),
}

pub trait OptionalExtension<T> {
    fn optional(self) -> Result<Option<T>, AsyncError>;
}

impl<T> OptionalExtension<T> for AsyncResult<T> {
    fn optional(self) -> Result<Option<T>, AsyncError> {
        match self {
            Ok(value) => Ok(Some(value)),
            Err(AsyncError::Error(diesel::result::Error::NotFound)) => Ok(None),
            Err(e) => Err(e),
        }
    }
}

impl fmt::Display for AsyncError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            AsyncError::Checkout(ref err) => err.fmt(f),
            AsyncError::Error(ref err) => err.fmt(f),
        }
    }
}

impl StdError for AsyncError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match *self {
            AsyncError::Checkout(ref err) => Some(err),
            AsyncError::Error(ref err) => Some(err),
        }
    }
}

#[async_trait]
pub trait AsyncSimpleConnection<Conn>
where
    Conn: 'static + SimpleConnection,
{
    async fn batch_execute_async(&self, query: &str) -> AsyncResult<()>;
}

#[async_trait]
impl<Conn, M> AsyncSimpleConnection<Conn> for Pool<M>
where
    Conn: 'static + Connection,
    M: ManageConnection<Connection = Conn>,
{
    #[inline]
    async fn batch_execute_async(&self, query: &str) -> AsyncResult<()> {
        let self_ = self.clone();
        let query = query.to_string();
        task::spawn_blocking(move || {
            let conn = self_.get().map_err(AsyncError::Checkout)?;
            conn.batch_execute(&query).map_err(AsyncError::Error)
        })
        .await
        .expect("task has panicked")
    }
}

#[async_trait]
pub trait AsyncConnection<Conn>: AsyncSimpleConnection<Conn>
where
    Conn: 'static + Connection,
{
    async fn run<R, Func>(&self, f: Func) -> AsyncResult<R>
    where
        R: 'static + Send,
        Func: 'static + FnOnce(&Conn) -> QueryResult<R> + Send;

    async fn transaction<R, Func>(&self, f: Func) -> AsyncResult<R>
    where
        R: 'static + Send,
        Func: 'static + FnOnce(&Conn) -> QueryResult<R> + Send;
}

#[async_trait]
impl<Conn, M> AsyncConnection<Conn> for Pool<M>
where
    Conn: 'static + Connection,
    M: ManageConnection<Connection = Conn>,
{
    #[inline]
    async fn run<R, Func>(&self, f: Func) -> AsyncResult<R>
    where
        R: 'static + Send,
        Func: 'static + FnOnce(&Conn) -> QueryResult<R> + Send,
    {
        let self_ = self.clone();
        task::spawn_blocking(move || {
            let conn = self_.get().map_err(AsyncError::Checkout)?;
            f(&*conn).map_err(AsyncError::Error)
        })
        .await
        .expect("task has panicked")
    }

    #[inline]
    async fn transaction<R, Func>(&self, f: Func) -> AsyncResult<R>
    where
        R: 'static + Send,
        Func: 'static + FnOnce(&Conn) -> QueryResult<R> + Send,
    {
        let self_ = self.clone();
        task::spawn_blocking(move || {
            let conn = self_.get().map_err(AsyncError::Checkout)?;
            conn.transaction(|| f(&*conn)).map_err(AsyncError::Error)
        })
        .await
        .expect("task has panicked")
    }
}

#[async_trait]
pub trait AsyncRunQueryDsl<Conn, AsyncConn>
where
    Conn: 'static + Connection,
{
    async fn execute_async(self, asc: &AsyncConn) -> AsyncResult<usize>
    where
        Self: ExecuteDsl<Conn>;

    async fn load_async<U>(self, asc: &AsyncConn) -> AsyncResult<Vec<U>>
    where
        U: 'static + Send,
        Self: LoadQuery<Conn, U>;

    async fn get_result_async<U>(self, asc: &AsyncConn) -> AsyncResult<U>
    where
        U: 'static + Send,
        Self: LoadQuery<Conn, U>;

    async fn get_results_async<U>(self, asc: &AsyncConn) -> AsyncResult<Vec<U>>
    where
        U: 'static + Send,
        Self: LoadQuery<Conn, U>;

    async fn first_async<U>(self, asc: &AsyncConn) -> AsyncResult<U>
    where
        U: 'static + Send,
        Self: LimitDsl,
        Limit<Self>: LoadQuery<Conn, U>;
}

#[async_trait]
impl<T, Conn, M> AsyncRunQueryDsl<Conn, Pool<M>> for T
where
    T: 'static + Send + RunQueryDsl<Conn>,
    Conn: 'static + Connection,
    M: ManageConnection<Connection = Conn>,
{
    async fn execute_async(self, asc: &Pool<M>) -> AsyncResult<usize>
    where
        Self: ExecuteDsl<Conn>,
    {
        asc.run(|conn| self.execute(&*conn)).await
    }

    async fn load_async<U>(self, asc: &Pool<M>) -> AsyncResult<Vec<U>>
    where
        U: 'static + Send,
        Self: LoadQuery<Conn, U>,
    {
        asc.run(|conn| self.load(&*conn)).await
    }

    async fn get_result_async<U>(self, asc: &Pool<M>) -> AsyncResult<U>
    where
        U: 'static + Send,
        Self: LoadQuery<Conn, U>,
    {
        asc.run(|conn| self.get_result(&*conn)).await
    }

    async fn get_results_async<U>(self, asc: &Pool<M>) -> AsyncResult<Vec<U>>
    where
        U: 'static + Send,
        Self: LoadQuery<Conn, U>,
    {
        asc.run(|conn| self.get_results(&*conn)).await
    }

    async fn first_async<U>(self, asc: &Pool<M>) -> AsyncResult<U>
    where
        U: 'static + Send,
        Self: LimitDsl,
        Limit<Self>: LoadQuery<Conn, U>,
    {
        asc.run(|conn| self.first(&*conn)).await
    }
}
