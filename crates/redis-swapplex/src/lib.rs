//! Redis multiplexing with reconnection notifications and MGET auto-batching. Connection configuration is provided by [env-url](https://crates.io/crates/env-url).
//!
//! Why use this instead of [redis::aio::ConnectionManager](https://docs.rs/redis/latest/redis/aio/struct.ConnectionManager.html)?
//! - Error-free reconnection behavior: when a command would otherwise fail as a consequence of the connection being dropped, this library will immediately reconnect and retry when able without producing an otherwise avoidable IoError and with subsequent reconnections debounced 1500ms
//! - ENV configuration simplifies kubernetes usage
//! - Reconnects can be observed allowing for Redis [server-assisted client-side caching](https://redis.io/docs/manual/client-side-caching/) using client tracking redirection
//! - Integrated MGET auto-batching (up to 180x more performant than GET)
//!
//! Composible connection urls are provided by environment variables using [env-url](https://crates.io/crates/env-url) with the `REDIS` prefix:
//!
//! ```text
//! REDIS_URL=redis://127.0.0.1:6379
//! # Override env mapping for easy kubernetes config
//! REDIS_HOST_ENV=MONOLITH_STAGE_REDIS_MASTER_PORT_6379_TCP_ADDR
//! REDIS_PORT_ENV=MONOLITH_STAGE_REDIS_MASTER_SERVICE_PORT_REDIS
//! ```
//!
//! ```rust
//! use redis::{AsyncCommands, RedisResult};
//! use redis_swapplex::get_connection;
//!
//! async fn get_value(key: &str) -> RedisResult<String> {
//!   let mut conn = get_connection();
//!   conn.get(key).await
//! }
//! ```

#![cfg_attr(not(feature = "boxed"), feature(type_alias_impl_trait))]
#![allow(rustdoc::private_intra_doc_links)]
#[doc(hidden)]
pub extern crate arc_swap;
extern crate self as redis_swapplex;

mod bytes;

pub use bytes::IntoBytes;

use arc_swap::{ArcSwapAny, ArcSwapOption, Cache};
pub use derive_redis_swapplex::ConnectionManagerContext;
use env_url::*;
use futures_util::{future::FutureExt, stream::unfold, Stream};
use once_cell::sync::Lazy;
use redis::{
  aio::{ConnectionLike, MultiplexedConnection},
  Client, Cmd, ErrorKind, Pipeline, RedisError, RedisFuture, RedisResult, Value,
};
use stack_queue::{
  assignment::{CompletionReceipt, PendingAssignment, UnboundedRange},
  local_queue, BackgroundQueue, TaskQueue,
};
use std::{
  cell::RefCell,
  iter,
  marker::PhantomData,
  ops::Deref,
  ptr::addr_of,
  sync::Arc,
  task::Poll,
  thread::LocalKey,
  time::{Duration, SystemTime},
};
use tokio::sync::Notify;

/// Trait for defining redis client creation and db selection
pub trait ConnectionInfo: Send + Sync + Sized {
  fn new(client: RedisResult<Client>, db_index: i64) -> Self;
  fn parse_index(url: &Url) -> Option<i64> {
    let mut segments = url.path_segments()?;
    let db_index: i64 = segments.next()?.parse().ok()?;

    Some(db_index)
  }

  fn from_url(url: &Url) -> Self {
    let db_index = <Self as ConnectionInfo>::parse_index(url).unwrap_or(0);
    let client = redis::Client::open(url.as_str());

    <Self as ConnectionInfo>::new(client, db_index)
  }

  fn get_db(&self) -> i64;
  fn client(&self) -> &RedisResult<Client>;
}

#[derive(EnvURL, ConnectionManagerContext)]
#[env_url(env_prefix = "REDIS", default = "redis://127.0.0.1:6379")]
/// Default env-configured Redis connection manager
pub struct EnvConnection;

#[doc(hidden)]
pub struct RedisDB<T: Send + Sync + Sized> {
  client: RedisResult<Client>,
  db_index: i64,
  _marker: PhantomData<fn() -> T>,
}

impl<T> RedisDB<T>
where
  T: Send + Sync + 'static + Sized,
{
  pub fn new(client: RedisResult<Client>, db_index: i64) -> Self {
    RedisDB {
      client,
      db_index,
      _marker: PhantomData,
    }
  }
}

impl<T> ConnectionInfo for RedisDB<T>
where
  T: ServiceURL + Send + Sync + 'static + Sized,
{
  fn new(client: RedisResult<Client>, db_index: i64) -> Self {
    RedisDB::new(client, db_index)
  }

  fn get_db(&self) -> i64 {
    self.db_index
  }

  fn client(&self) -> &RedisResult<Client> {
    &self.client
  }
}

impl<T> Default for RedisDB<T>
where
  T: ServiceURL + Send + Sync + 'static + Sized,
  Self: ConnectionInfo,
{
  fn default() -> Self {
    match <T as ServiceURL>::service_url() {
      Ok(url) => <Self as ConnectionInfo>::from_url(&url),
      Err(_) => {
        let client = Err(RedisError::from((
          ErrorKind::InvalidClientConfig,
          "Invalid Redis connection URL",
        )));

        Self {
          client,
          db_index: 0,
          _marker: PhantomData,
        }
      }
    }
  }
}

#[doc(hidden)]
pub enum ConnectionState {
  Connecting,
  ClientError(ErrorKind),
  ConnectionError(ErrorKind, SystemTime),
  Connected(MultiplexedConnection),
}

#[doc(hidden)]
pub struct ConnectionManager<T: ConnectionInfo> {
  state: ArcSwapOption<ConnectionState>,
  notify: Notify,
  connection_info: Lazy<T>,
}

impl<T> ConnectionManager<T>
where
  T: ConnectionInfo,
{
  pub const fn new(connection_info: fn() -> T) -> ConnectionManager<T> {
    ConnectionManager {
      state: ArcSwapOption::const_empty(),
      notify: Notify::const_new(),
      connection_info: Lazy::new(connection_info),
    }
  }

  fn store_and_notify(&self, state: Option<Arc<ConnectionState>>) {
    self.state.store(state);
    self.notify.notify_waiters();
  }

  pub fn client(&self) -> &RedisResult<Client> {
    self.connection_info.client()
  }

  pub fn get_db(&self) -> i64 {
    self.connection_info.get_db()
  }
}

impl<T> Deref for ConnectionManager<T>
where
  T: ConnectionInfo,
{
  type Target = ArcSwapAny<Option<Arc<ConnectionState>>>;

  fn deref(&self) -> &Self::Target {
    &self.state
  }
}

#[derive(PartialEq)]
struct ConnectionAddr(*const MultiplexedConnection);

impl PartialEq<Option<ConnectionAddr>> for ConnectionAddr {
  fn eq(&self, other: &Option<ConnectionAddr>) -> bool {
    if let Some(addr) = other {
      self.0 == addr.0
    } else {
      false
    }
  }
}

unsafe impl Send for ConnectionAddr {}
unsafe impl Sync for ConnectionAddr {}

pub trait ConnectionManagerContext: Send + Sync + 'static + Sized {
  type ConnectionInfo: ConnectionInfo;

  fn get_connection() -> ManagedConnection<Self> {
    ManagedConnection::new()
  }

  fn connection_manager() -> &'static ConnectionManager<Self::ConnectionInfo>;

  fn client() -> &'static RedisResult<Client> {
    Self::connection_manager().client()
  }

  fn get_db() -> i64 {
    Self::connection_manager().get_db()
  }

  fn state_cache() -> &'static LocalKey<
    RefCell<Cache<&'static ArcSwapOption<ConnectionState>, Option<Arc<ConnectionState>>>>,
  >;

  fn with_state<T>(with_fn: fn(&Option<Arc<ConnectionState>>) -> T) -> T {
    <Self as ConnectionManagerContext>::state_cache()
      .with(|cache| with_fn(cache.borrow_mut().load()))
  }
}

impl<T> RedisDB<T>
where
  T: ConnectionManagerContext,
{
  async fn get_multiplexed_connection() -> RedisResult<(MultiplexedConnection, ConnectionAddr)> {
    let connection = T::with_state(|connection_state| match connection_state.as_deref() {
      None => {
        Self::establish_connection(None);
        None
      }
      Some(ConnectionState::Connecting) => None,
      Some(ConnectionState::ClientError(kind)) => Some(Err(RedisError::from((
        kind.to_owned(),
        "Invalid Redis connection URL",
      )))),
      Some(ConnectionState::ConnectionError(
        ErrorKind::IoError | ErrorKind::ClusterDown | ErrorKind::BusyLoadingError,
        time,
      )) if SystemTime::now()
        .duration_since(*time)
        .unwrap()
        .gt(&Duration::from_millis(1500)) =>
      {
        Self::establish_connection(None);
        None
      }
      Some(ConnectionState::ConnectionError(kind, _)) => Some(Err(RedisError::from((
        kind.to_owned(),
        "Unable to establish Redis connection",
      )))),
      Some(ConnectionState::Connected(connection)) => {
        let conn_addr = ConnectionAddr(addr_of!(*connection));
        Some(Ok((connection.clone(), conn_addr)))
      }
    });

    match connection {
      Some(connection) => connection,
      None => {
        T::connection_manager().notify.notified().await;

        T::with_state(|connection_state| match connection_state.as_deref() {
          None => unreachable!(),
          Some(ConnectionState::Connecting) => unreachable!(),
          Some(ConnectionState::ClientError(kind)) => Err(RedisError::from((
            kind.to_owned(),
            "Invalid Redis connection URL",
          ))),
          Some(ConnectionState::ConnectionError(kind, _timestamp)) => Err(RedisError::from((
            kind.to_owned(),
            "Unable to establish Redis connection",
          ))),
          Some(ConnectionState::Connected(connection)) => {
            let conn_addr = ConnectionAddr(addr_of!(*connection));
            Ok((connection.clone(), conn_addr))
          }
        })
      }
    }
  }

  fn establish_connection(conn_addr: Option<ConnectionAddr>) {
    let state = T::connection_manager().state.load();

    let should_connect = match state.as_deref() {
      None => true,
      Some(ConnectionState::Connecting) => false,
      // Never reconnect if there's been a client error; treat as poisoned
      Some(ConnectionState::ClientError(_)) => false,
      Some(ConnectionState::ConnectionError(
        ErrorKind::AuthenticationFailed | ErrorKind::InvalidClientConfig,
        _,
      )) => false,
      Some(ConnectionState::ConnectionError(_, time))
        if SystemTime::now()
          .duration_since(*time)
          .unwrap()
          .gt(&Duration::from_millis(1500)) =>
      {
        true
      }
      Some(ConnectionState::ConnectionError(_, _)) => false,
      Some(ConnectionState::Connected(connection)) => {
        if let Some(conn_addr) = conn_addr {
          let current_addr = ConnectionAddr(addr_of!(*connection));

          // Only reconnect if conn_addr hasn't changed
          conn_addr.eq(&current_addr)
        } else {
          false
        }
      }
    };

    if should_connect {
      let prev = T::connection_manager()
        .state
        .compare_and_swap(&state, Some(Arc::new(ConnectionState::Connecting)));

      if match (prev.as_ref(), state.as_ref()) {
        (None, None) => true,
        (Some(prev), Some(state)) => Arc::ptr_eq(prev, state),
        _ => false,
      } {
        tokio::task::spawn(async move {
          match T::client() {
            Ok(client) => match client.get_multiplexed_tokio_connection().await {
              Ok(conn) => {
                T::connection_manager()
                  .store_and_notify(Some(Arc::new(ConnectionState::Connected(conn))));
              }
              Err(err) => T::connection_manager().store_and_notify(Some(Arc::new(
                ConnectionState::ConnectionError(err.kind(), SystemTime::now()),
              ))),
            },
            Err(err) => T::connection_manager()
              .store_and_notify(Some(Arc::new(ConnectionState::ClientError(err.kind())))),
          }
        });
      }
    }
  }

  pub async fn on_connected() -> RedisResult<()> {
    loop {
      T::connection_manager().notify.notified().await;

      let poll = T::with_state(|connection_state| match connection_state.as_deref() {
        Some(ConnectionState::ClientError(kind)) => Poll::Ready(Err(RedisError::from((
          kind.to_owned(),
          "Invalid Redis connection URL",
        )))),
        Some(ConnectionState::ConnectionError(
          ErrorKind::BusyLoadingError | ErrorKind::ClusterDown | ErrorKind::IoError,
          _,
        )) => Poll::Pending,
        Some(ConnectionState::ConnectionError(kind, _)) => Poll::Ready(Err(RedisError::from((
          kind.to_owned(),
          "Unable to establish Redis connection",
        )))),
        Some(ConnectionState::Connected(_)) => Poll::Ready(Ok(())),
        _ => Poll::Pending,
      });

      match poll {
        Poll::Pending => continue,
        Poll::Ready(result) => return result,
      }
    }
  }
}

/// A multiplexed connection utilizing the respective connection manager
pub struct ManagedConnection<T: ConnectionManagerContext> {
  _marker: PhantomData<T>,
}

impl<T> ManagedConnection<T>
where
  T: ConnectionManagerContext,
{
  pub fn new() -> Self {
    ManagedConnection {
      _marker: PhantomData,
    }
  }
}

impl<T> Default for ManagedConnection<T>
where
  T: ConnectionManagerContext,
{
  fn default() -> Self {
    ManagedConnection::new()
  }
}

impl<T> ConnectionLike for ManagedConnection<T>
where
  T: ConnectionManagerContext,
{
  fn req_packed_command<'a>(&'a mut self, cmd: &'a Cmd) -> RedisFuture<'a, Value> {
    (async move {
      loop {
        let (mut conn, addr) = <RedisDB<T>>::get_multiplexed_connection().await?;

        match conn.req_packed_command(cmd).await {
          Ok(result) => break Ok(result),
          Err(err) => {
            if err.is_connection_dropped() {
              <RedisDB<T>>::establish_connection(Some(addr));
              continue;
            }

            break Err(err);
          }
        }
      }
    })
    .boxed()
  }

  fn req_packed_commands<'a>(
    &'a mut self,
    cmd: &'a Pipeline,
    offset: usize,
    count: usize,
  ) -> RedisFuture<'a, Vec<Value>> {
    (async move {
      loop {
        let (mut conn, addr) = <RedisDB<T>>::get_multiplexed_connection().await?;

        match conn.req_packed_commands(cmd, offset, count).await {
          Ok(result) => break Ok(result),
          Err(err) => {
            if err.is_connection_dropped() {
              <RedisDB<T>>::establish_connection(Some(addr));
              continue;
            }

            break Err(err);
          }
        }
      }
    })
    .boxed()
  }

  fn get_db(&self) -> i64 {
    T::get_db()
  }
}

/// Get a managed multiplexed connection for the default env-configured Redis database
pub fn get_connection() -> ManagedConnection<EnvConnection> {
  EnvConnection::get_connection()
}

/// Notify the next time a connection is established
pub async fn on_connected<T>() -> RedisResult<()>
where
  T: ConnectionManagerContext,
{
  <RedisDB<T>>::on_connected().await
}

fn connection_addr<T>() -> Option<ConnectionAddr>
where
  T: ConnectionManagerContext,
{
  T::with_state(|connect_state| {
    if let Some(ConnectionState::Connected(connection)) = connect_state.as_deref() {
      let conn_addr = ConnectionAddr(addr_of!(*connection));

      Some(conn_addr)
    } else {
      None
    }
  })
}

/// A stream notifying whenever the current or a new connection is connected; useful for client tracking redirection
pub fn connection_stream<T>() -> impl Stream<Item = ()>
where
  T: ConnectionManagerContext,
{
  unfold(None, |conn_addr| async move {
    loop {
      if let Some(current_addr) = connection_addr::<T>() {
        if current_addr.ne(&conn_addr) {
          break Some(((), Some(current_addr)));
        }
      }

      T::connection_manager().notify.notified().await
    }
  })
}

/// Get the value of a key using auto-batched MGET commands
pub async fn get<K: IntoBytes>(key: K) -> Result<Option<Vec<u8>>, ErrorKind> {
  struct MGetQueue;

  #[local_queue(buffer_size = 2048)]
  impl TaskQueue for MGetQueue {
    type Task = Vec<u8>;
    type Value = Result<Option<Vec<u8>>, ErrorKind>;

    async fn batch_process<const N: usize>(
      batch: PendingAssignment<'async_trait, Self, N>,
    ) -> CompletionReceipt<Self> {
      let mut conn = get_connection();
      let assignment = batch.into_assignment();
      let (front, back) = assignment.as_slices();

      let data: Result<Vec<Option<Vec<u8>>>, RedisError> = redis::cmd("MGET")
        .arg(front)
        .arg(back)
        .query_async(&mut conn)
        .await;

      match data {
        Ok(data) => assignment.resolve_with_iter(data.into_iter().map(Result::Ok)),
        Err(err) => assignment.resolve_with_iter(iter::repeat(Result::Err(err.kind()))),
      }
    }
  }

  MGetQueue::auto_batch(key.into_bytes()).await
}

/// Set the value of a key using auto-batched MSET commands. This runs in the background with Redis errors ignored.
///
/// # Panics
///
/// Panics if called from **outside** of the Tokio runtime.
///
pub fn set<K: IntoBytes, V: IntoBytes>(key: K, value: V) {
  struct MSetQueue;

  #[local_queue(buffer_size = 2048)]
  impl BackgroundQueue for MSetQueue {
    type Task = [Vec<u8>; 2];

    async fn batch_process<const N: usize>(batch: UnboundedRange<'async_trait, [Vec<u8>; 2], N>) {
      let mut conn = get_connection();
      let assignment = batch.into_bounded();

      let mut cmd = redis::cmd("MSET");

      for kv in assignment.into_iter() {
        cmd.arg(&kv);
      }

      let _: Result<(), RedisError> = cmd.query_async(&mut conn).await;
    }
  }

  MSetQueue::auto_batch([key.into_bytes(), value.into_bytes()]);
}

#[cfg(test)]
#[ctor::ctor]
fn setup_test_env() {
  std::env::set_var("REDIS_URL", "redis://127.0.0.1:6379");
}
#[cfg(all(test))]
mod tests {
  use std::collections::HashSet;

  use futures_util::StreamExt;
  use redis::AsyncCommands;

  use super::*;

  #[tokio::test]
  async fn reconnects_on_error() -> RedisResult<()> {
    let conn_stream = connection_stream::<EnvConnection>();

    tokio::pin!(conn_stream);

    let mut conn = get_connection();

    let mut pipe = redis::pipe();

    pipe
      .atomic()
      .del("test::stream")
      .xgroup_create_mkstream("test::stream", "rustc", "0");

    let _: (i64, String) = pipe.query_async(&mut conn).await?;

    conn_stream.next().await;

    let _: () = redis::cmd("QUIT").query_async(&mut conn).await?;

    let result: RedisResult<String> = conn
      .xgroup_create_mkstream("test::stream", "rustc", "0")
      .await;

    match result {
      Err(err) if err.kind().eq(&ErrorKind::ExtensionError) => {
        assert_eq!(err.code(), Some("BUSYGROUP"));
      }
      _ => panic!("Expected BUSYGROUP error"),
    };

    conn_stream.next().await;

    conn.del("test::stream").await?;

    Ok(())
  }

  #[tokio::test]
  async fn reconnects_immediately() -> RedisResult<()> {
    let mut conn = get_connection();

    let mut client_list: HashSet<i32> = HashSet::new();

    for _ in 0..10 {
      let (client_id, _): (i32, String) = redis::pipe()
        .cmd("CLIENT")
        .arg("ID")
        .cmd("QUIT")
        .query_async(&mut conn)
        .await?;

      client_list.insert(client_id);
    }

    assert_eq!(client_list.len(), 10);

    Ok(())
  }

  #[ignore = "use `cargo test -- --ignored` to test in isolation"]
  #[tokio::test]
  async fn handles_shutdown() -> RedisResult<()> {
    let mut conn = get_connection();

    match redis::cmd("SHUTDOWN").query_async(&mut conn).await {
      Ok(()) => panic!("Redis shutdown should result in IoError"),
      Err(err) if err.kind().eq(&ErrorKind::IoError) => Ok(()),
      Err(err) => Err(err),
    }?;

    match redis::cmd("CLIENT").arg("ID").query_async(&mut conn).await {
      Ok(()) => panic!("Redis server should still be offline"),
      Err(err) if err.kind().eq(&ErrorKind::IoError) => Ok(()),
      Err(err) => Err(err),
    }?;

    tokio::time::sleep(Duration::from_millis(1400)).await;

    match redis::cmd("CLIENT").arg("ID").query_async(&mut conn).await {
      Ok(()) => panic!("Redis server should be online, but we shouldn't be able to reconnect yet"),
      Err(err) if err.kind().eq(&ErrorKind::IoError) => Ok(()),
      Err(err) => Err(err),
    }?;

    tokio::time::sleep(Duration::from_millis(100)).await;

    redis::cmd("CLIENT")
      .arg("ID")
      .query_async(&mut conn)
      .await?;

    Ok(())
  }
}
