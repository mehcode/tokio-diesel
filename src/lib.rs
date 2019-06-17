#![feature(async_await)]

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
use futures::{
    future::{poll_fn, BoxFuture},
    FutureExt, TryFutureExt,
};
use std::{error::Error as StdError, fmt};
use tokio_threadpool::BlockingError;

pub type AsyncResult<R> = Result<R, AsyncError>;

#[derive(Debug)]
pub enum AsyncError {
    // Attempt to run an async operation while not in a
    // tokio worker pool
    NotInPool(BlockingError),

    // Failed to checkout a connection
    Checkout(r2d2::Error),

    // The query failed in some way
    Error(diesel::result::Error),
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
        FutureExt::boxed(blocking(move || {
            let conn = self.get().map_err(AsyncError::Checkout)?;
            f(&*conn).map_err(AsyncError::Error)
        }))
    }

    #[inline]
    fn transaction<R, Func>(&self, f: Func) -> BoxFuture<AsyncResult<R>>
    where
        R: 'static + Send,
        Func: 'static + FnOnce(&Conn) -> QueryResult<R> + Send,
    {
        FutureExt::boxed(blocking(move || {
            let conn = self.get().map_err(AsyncError::Checkout)?;
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

// Convert a `Poll` from futures v0.1 to a `Poll` from futures v0.3
// https://github.com/rust-lang-nursery/futures-rs/blob/526259e3e25e65cc32653f6a8e0244db2faccb2e/futures-util/src/compat/compat01as03.rs#L135
fn poll_01_to_03<T, E>(x: Result<futures01::Async<T>, E>) -> futures::task::Poll<Result<T, E>> {
    match x? {
        futures01::Async::Ready(t) => futures::task::Poll::Ready(Ok(t)),
        futures01::Async::NotReady => futures::task::Poll::Pending,
    }
}

// Run a closure with blocking IO
async fn blocking<R, F>(f: F) -> AsyncResult<R>
where
    R: Send,
    F: FnOnce() -> AsyncResult<R>,
{
    let mut f = Some(f);
    Ok(poll_fn(move |_| {
        poll_01_to_03(tokio_threadpool::blocking(|| {
            (f.take().expect("call FnOnce more than once"))()
        }))
    })
    .map_err(AsyncError::NotInPool)
    .unwrap_or_else(|err| Err(err))
    .await?)
}
