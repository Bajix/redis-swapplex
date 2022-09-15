//! Atomic state-transition based Redis multiplexing with reconnection notifications. Connection configuration is provided by [env-url](https://crates.io/crates/env-url).
//!
//! Why use this instead of [redis::aio::ConnectionManager](https://docs.rs/redis/latest/redis/aio/struct.ConnectionManager.html)?
//! - Error-free reconnection behavior: when a command would otherwise fail as a consequence of the connection being dropped, this library will immediately reconnect and retry when able without producing an otherwise avoidable IoError and with subsequent reconnections debounced 1500ms
//! - Less contention overhead: the usage of [arc_swap::cache::Cache](https://docs.rs/arc-swap/latest/arc_swap/cache/struct.Cache.html) results in a 10-25x speed up of cached connection acquisition.
//! - ENV configuration makes life easier and simplifies kubernetes usage
//! - Reconnects can be observed, thus allowing for Redis [server-assisted client-side caching](https://redis.io/docs/manual/client-side-caching/) using client tracking redirection
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

#![allow(rustdoc::private_intra_doc_links)]
#[doc(hidden)]
pub extern crate arc_swap;
extern crate self as redis_swapplex;

use arc_swap::{ArcSwap, ArcSwapAny, Cache};
pub use derive_redis_swapplex::ConnectionManagerContext;
use env_url::*;
use futures_util::{future::FutureExt, stream::unfold, Stream};
use once_cell::sync::Lazy;
use redis::{
  aio::{ConnectionLike, MultiplexedConnection},
  Client, Cmd, ErrorKind, Pipeline, RedisError, RedisFuture, RedisResult, Value,
};
use std::{
  cell::RefCell,
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
  Idle,
  Connecting,
  ClientError(ErrorKind),
  ConnectionError(ErrorKind, SystemTime),
  Connected(MultiplexedConnection),
}

#[doc(hidden)]
pub struct ConnectionManager<T: ConnectionInfo> {
  state: Lazy<ArcSwap<ConnectionState>>,
  notify: Notify,
  connection_info: Lazy<T>,
}

impl<T> ConnectionManager<T>
where
  T: ConnectionInfo,
{
  pub const fn new(connection_info: fn() -> T) -> ConnectionManager<T> {
    ConnectionManager {
      state: Lazy::new(|| ArcSwap::from(Arc::new(ConnectionState::Idle))),
      notify: Notify::const_new(),
      connection_info: Lazy::new(connection_info),
    }
  }

  fn store_and_notify<S: Into<Arc<ConnectionState>>>(&self, state: S) {
    self.state.store(state.into());
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
  type Target = ArcSwapAny<Arc<ConnectionState>>;

  fn deref(&self) -> &Self::Target {
    self.state.deref()
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

  fn state_cache(
  ) -> &'static LocalKey<RefCell<Cache<&'static ArcSwap<ConnectionState>, Arc<ConnectionState>>>>;

  fn with_state<T>(with_fn: fn(&ConnectionState) -> T) -> T {
    <Self as ConnectionManagerContext>::state_cache()
      .with(|cache| with_fn(cache.borrow_mut().load()))
  }
}

impl<T> RedisDB<T>
where
  T: ConnectionManagerContext,
{
  async fn get_multiplexed_connection() -> RedisResult<(MultiplexedConnection, ConnectionAddr)> {
    let connection = T::with_state(|connection_state| match connection_state {
      ConnectionState::Idle => {
        Self::establish_connection(None);
        None
      }
      ConnectionState::Connecting => None,
      ConnectionState::ClientError(kind) => Some(Err(RedisError::from((
        kind.to_owned(),
        "Invalid Redis connection URL",
      )))),
      ConnectionState::ConnectionError(
        ErrorKind::IoError | ErrorKind::ClusterDown | ErrorKind::BusyLoadingError,
        time,
      ) if SystemTime::now()
        .duration_since(*time)
        .unwrap()
        .gt(&Duration::from_millis(1500)) =>
      {
        Self::establish_connection(None);
        None
      }
      ConnectionState::ConnectionError(kind, _) => Some(Err(RedisError::from((
        kind.to_owned(),
        "Unable to establish Redis connection",
      )))),
      ConnectionState::Connected(connection) => {
        let conn_addr = ConnectionAddr(addr_of!(*connection));
        Some(Ok((connection.clone(), conn_addr)))
      }
    });

    match connection {
      Some(connection) => connection,
      None => {
        T::connection_manager().notify.notified().await;

        T::with_state(|connection_state| match connection_state {
          ConnectionState::Idle => unreachable!(),
          ConnectionState::Connecting => unreachable!(),
          ConnectionState::ClientError(kind) => Err(RedisError::from((
            kind.to_owned(),
            "Invalid Redis connection URL",
          ))),
          ConnectionState::ConnectionError(kind, _timestamp) => Err(RedisError::from((
            kind.to_owned(),
            "Unable to establish Redis connection",
          ))),
          ConnectionState::Connected(connection) => {
            let conn_addr = ConnectionAddr(addr_of!(*connection));
            Ok((connection.clone(), conn_addr))
          }
        })
      }
    }
  }

  fn establish_connection(conn_addr: Option<ConnectionAddr>) {
    let state = T::connection_manager().state.load();

    let should_connect = match state.as_ref() {
      ConnectionState::Idle => true,
      ConnectionState::Connecting => false,
      // Never reconnect if there's been a client error; treat as poisoned
      ConnectionState::ClientError(_) => false,
      ConnectionState::ConnectionError(
        ErrorKind::AuthenticationFailed | ErrorKind::InvalidClientConfig,
        _,
      ) => false,
      ConnectionState::ConnectionError(_, time)
        if SystemTime::now()
          .duration_since(*time)
          .unwrap()
          .gt(&Duration::from_millis(1500)) =>
      {
        true
      }
      ConnectionState::ConnectionError(_, _) => false,
      ConnectionState::Connected(connection) => {
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
        .compare_and_swap(&state, Arc::new(ConnectionState::Connecting));

      if Arc::ptr_eq(&prev, &state) {
        tokio::task::spawn(async move {
          match T::client() {
            Ok(client) => match client.get_multiplexed_tokio_connection().await {
              Ok(conn) => {
                T::connection_manager().store_and_notify(ConnectionState::Connected(conn));
              }
              Err(err) => T::connection_manager().store_and_notify(
                ConnectionState::ConnectionError(err.kind(), SystemTime::now()),
              ),
            },
            Err(err) => {
              T::connection_manager().store_and_notify(ConnectionState::ClientError(err.kind()))
            }
          }
        });
      }
    }
  }

  pub async fn on_connected() -> RedisResult<()> {
    loop {
      T::connection_manager().notify.notified().await;

      let poll = T::with_state(|connection_state| match connection_state {
        ConnectionState::ClientError(kind) => Poll::Ready(Err(RedisError::from((
          kind.to_owned(),
          "Invalid Redis connection URL",
        )))),
        ConnectionState::ConnectionError(
          ErrorKind::BusyLoadingError | ErrorKind::ClusterDown | ErrorKind::IoError,
          _,
        ) => Poll::Pending,
        ConnectionState::ConnectionError(kind, _) => Poll::Ready(Err(RedisError::from((
          kind.to_owned(),
          "Unable to establish Redis connection",
        )))),
        ConnectionState::Connected(_) => Poll::Ready(Ok(())),
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
    if let ConnectionState::Connected(connection) = connect_state {
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
