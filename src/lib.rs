use diesel::{
    dsl::Limit,
    query_dsl::{
        methods::{ExecuteDsl, LimitDsl, LoadQuery},
        RunQueryDsl,
    },
    r2d2::{ConnectionManager, Pool},
    result::QueryResult,
    Connection,
};
use futures::future::BoxFuture;
use std::{error::Error as StdError, fmt};
use tokio_executor::blocking;

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

// TODO: Forward displays
impl fmt::Display for AsyncError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

// TODO: Forward causes
impl StdError for AsyncError {}

pub trait AsyncConnection<Conn>
where
    Conn: 'static + Connection,
{
    fn run<R, Func>(&self, f: Func) -> BoxFuture<AsyncResult<R>>
    where
        R: 'static + Send,
        Func: 'static + FnOnce(&Conn) -> QueryResult<R> + Send;

    fn transaction<R, Func>(&self, f: Func) -> BoxFuture<AsyncResult<R>>
    where
        R: 'static + Send,
        Func: 'static + FnOnce(&Conn) -> QueryResult<R> + Send;
}

impl<Conn> AsyncConnection<Conn> for Pool<ConnectionManager<Conn>>
where
    Conn: 'static + Connection,
{
    #[inline]
    fn run<R, Func>(&self, f: Func) -> BoxFuture<AsyncResult<R>>
    where
        R: 'static + Send,
        Func: 'static + FnOnce(&Conn) -> QueryResult<R> + Send,
    {
        let self_ = self.clone();
        Box::pin(blocking::run(move || {
            let conn = self_.get().map_err(AsyncError::Checkout)?;
            f(&*conn).map_err(AsyncError::Error)
        }))
    }

    #[inline]
    fn transaction<R, Func>(&self, f: Func) -> BoxFuture<AsyncResult<R>>
    where
        R: 'static + Send,
        Func: 'static + FnOnce(&Conn) -> QueryResult<R> + Send,
    {
        let self_ = self.clone();
        Box::pin(blocking::run(move || {
            let conn = self_.get().map_err(AsyncError::Checkout)?;
            conn.transaction(|| f(&*conn)).map_err(AsyncError::Error)
        }))
    }
}

pub trait AsyncRunQueryDsl<Conn, AsyncConn>
where
    Conn: 'static + Connection,
{
    fn execute_async(self, asc: &AsyncConn) -> BoxFuture<AsyncResult<usize>>
    where
        Self: ExecuteDsl<Conn>;

    fn load_async<U>(self, asc: &AsyncConn) -> BoxFuture<AsyncResult<Vec<U>>>
    where
        U: 'static + Send,
        Self: LoadQuery<Conn, U>;

    fn get_result_async<U>(self, asc: &AsyncConn) -> BoxFuture<AsyncResult<U>>
    where
        U: 'static + Send,
        Self: LoadQuery<Conn, U>;

    fn get_results_async<U>(self, asc: &AsyncConn) -> BoxFuture<AsyncResult<Vec<U>>>
    where
        U: 'static + Send,
        Self: LoadQuery<Conn, U>;

    fn first_async<U>(self, asc: &AsyncConn) -> BoxFuture<AsyncResult<U>>
    where
        U: 'static + Send,
        Self: LimitDsl,
        Limit<Self>: LoadQuery<Conn, U>;
}

impl<T, Conn> AsyncRunQueryDsl<Conn, Pool<ConnectionManager<Conn>>> for T
where
    T: 'static + Send + RunQueryDsl<Conn>,
    Conn: 'static + Connection,
{
    fn execute_async(self, asc: &Pool<ConnectionManager<Conn>>) -> BoxFuture<AsyncResult<usize>>
    where
        Self: ExecuteDsl<Conn>,
    {
        asc.run(|conn| self.execute(&*conn))
    }

    fn load_async<U>(self, asc: &Pool<ConnectionManager<Conn>>) -> BoxFuture<AsyncResult<Vec<U>>>
    where
        U: 'static + Send,
        Self: LoadQuery<Conn, U>,
    {
        asc.run(|conn| self.load(&*conn))
    }

    fn get_result_async<U>(self, asc: &Pool<ConnectionManager<Conn>>) -> BoxFuture<AsyncResult<U>>
    where
        U: 'static + Send,
        Self: LoadQuery<Conn, U>,
    {
        asc.run(|conn| self.get_result(&*conn))
    }

    fn get_results_async<U>(
        self,
        asc: &Pool<ConnectionManager<Conn>>,
    ) -> BoxFuture<AsyncResult<Vec<U>>>
    where
        U: 'static + Send,
        Self: LoadQuery<Conn, U>,
    {
        asc.run(|conn| self.get_results(&*conn))
    }

    fn first_async<U>(self, asc: &Pool<ConnectionManager<Conn>>) -> BoxFuture<AsyncResult<U>>
    where
        U: 'static + Send,
        Self: LimitDsl,
        Limit<Self>: LoadQuery<Conn, U>,
    {
        asc.run(|conn| self.first(&*conn))
    }
}
