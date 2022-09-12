//! Atomic state-transition based Redis multiplexing with reconnection notifications. Connection configuration is provided by [env-url](https://crates.io/crates/env-url).
//!
//!
//! ```text
//! REDIS_URL=redis://127.0.0.1:6379
//! # Override env mapping for easy kubernetes config
//! REDIS_HOST_ENV=MONOLITH_STAGE_REDIS_MASTER_PORT_6379_TCP_ADDR
//! REDIS_PORT_ENV=MONOLITH_STAGE_REDIS_MASTER_SERVICE_PORT_REDIS
//! ```
//!

#![allow(rustdoc::private_intra_doc_links)]
#[doc(hidden)]
pub extern crate arc_swap;
extern crate self as redis_swapplex;

use arc_swap::{ArcSwap, ArcSwapAny, Cache};
pub use derive_redis_swapplex::ConnectionManagerContext;
use env_url::*;
use futures_util::future::FutureExt;
use once_cell::sync::Lazy;
use redis::{
  aio::{ConnectionLike, MultiplexedConnection},
  Client, Cmd, ErrorKind, Pipeline, RedisError, RedisFuture, RedisResult, Value,
};
use std::{
  cell::RefCell, marker::PhantomData, ops::Deref, ptr::addr_of, sync::Arc, task::Poll,
  thread::LocalKey,
};
use tokio::sync::Notify;

pub trait ConnectionInfo: Send + Sync + 'static + Sized {
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
/// Default env-configured Redis connection
pub struct EnvConnection;

pub struct RedisDB<T: Send + Sync + 'static + Sized> {
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
pub enum ConnectionState {
  Idle,
  Connecting,
  ClientError(ErrorKind),
  ConnectionError(ErrorKind),
  Connected(MultiplexedConnection),
}

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

struct ConnectionAddr(*const MultiplexedConnection);

impl PartialEq<*const MultiplexedConnection> for ConnectionAddr {
  fn eq(&self, other: &*const MultiplexedConnection) -> bool {
    self.0 == *other
  }
}

impl From<*const MultiplexedConnection> for ConnectionAddr {
  fn from(ptr: *const MultiplexedConnection) -> Self {
    ConnectionAddr(ptr)
  }
}

unsafe impl Send for ConnectionAddr {}
unsafe impl Sync for ConnectionAddr {}

pub trait ConnectionManagerContext {
  type ConnectionInfo: ConnectionInfo;
  fn connection_manager() -> &'static ConnectionManager<Self::ConnectionInfo>;

  fn state_cache(
  ) -> &'static LocalKey<RefCell<Cache<&'static ArcSwap<ConnectionState>, Arc<ConnectionState>>>>;

  fn with_state<T>(with_fn: fn(&ConnectionState) -> T) -> T {
    <Self as ConnectionManagerContext>::state_cache()
      .with(|cache| with_fn(cache.borrow_mut().load()))
  }
}

impl<T> RedisDB<T>
where
  T: ConnectionManagerContext + Send + Sync + 'static + Sized,
{
  pub fn get_connection() -> ManagedConnection<T> {
    ManagedConnection::new()
  }

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
      ConnectionState::ConnectionError(ErrorKind::IoError) => {
        Self::establish_connection(None);
        None
      }
      ConnectionState::ConnectionError(kind) => Some(Err(RedisError::from((
        kind.to_owned(),
        "Unable to establish Redis connection",
      )))),
      ConnectionState::Connected(connection) => {
        let conn_addr = addr_of!(connection) as *const MultiplexedConnection;
        Some(Ok((connection.clone(), conn_addr.into())))
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
          ConnectionState::ConnectionError(kind) => Err(RedisError::from((
            kind.to_owned(),
            "Unable to establish Redis connection",
          ))),
          ConnectionState::Connected(connection) => {
            let conn_addr = addr_of!(connection) as *const MultiplexedConnection;
            Ok((connection.clone(), conn_addr.into()))
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
      ConnectionState::ConnectionError(_) => true,
      ConnectionState::Connected(connection) => {
        if let Some(conn_addr) = conn_addr {
          let current_addr = addr_of!(connection) as *const MultiplexedConnection;

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
          match T::connection_manager().connection_info.client() {
            Ok(client) => match client.get_multiplexed_tokio_connection().await {
              Ok(conn) => {
                T::connection_manager().store_and_notify(ConnectionState::Connected(conn));
              }
              Err(err) => T::connection_manager()
                .store_and_notify(ConnectionState::ConnectionError(err.kind())),
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
        ConnectionState::ConnectionError(kind) if kind.ne(&ErrorKind::IoError) => Poll::Ready(Err(
          RedisError::from((kind.to_owned(), "Unable to establish Redis connection")),
        )),
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
  T: ConnectionManagerContext + Send + Sync + 'static + Sized,
{
  fn req_packed_command<'a>(&'a mut self, cmd: &'a Cmd) -> RedisFuture<'a, Value> {
    (async move {
      let (mut conn, addr) = <RedisDB<T>>::get_multiplexed_connection().await?;

      match conn.req_packed_command(cmd).await {
        Ok(result) => Ok(result),
        Err(err) => {
          if err.is_connection_dropped() {
            <RedisDB<T>>::establish_connection(Some(addr));
          }

          Err(err)
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
      let (mut conn, addr) = <RedisDB<T>>::get_multiplexed_connection().await?;

      match conn.req_packed_commands(cmd, offset, count).await {
        Ok(result) => Ok(result),
        Err(err) => {
          if err.is_connection_dropped() {
            <RedisDB<T>>::establish_connection(Some(addr));
          }

          Err(err)
        }
      }
    })
    .boxed()
  }

  fn get_db(&self) -> i64 {
    T::connection_manager().connection_info.get_db()
  }
}

/// Get a managed multiplexed connection for the default env-configured Redis database
pub fn get_connection() -> ManagedConnection<EnvConnection> {
  <RedisDB<EnvConnection>>::get_connection()
}

/// Notify on next connection. Useful for connection lifecycle behaviors
pub async fn on_connected<T>() -> RedisResult<()>
where
  T: ConnectionManagerContext,
{
  <RedisDB<T>>::on_connected().await
}

#[cfg(test)]
#[ctor::ctor]
fn setup_test_env() {
  std::env::set_var("REDIS_URL", "redis://127.0.0.1:6379");
}
#[cfg(all(test))]
mod tests {
  use redis::AsyncCommands;

  use super::*;

  #[tokio::test]
  async fn reconnects_on_error() -> RedisResult<()> {
    let (tx, mut rx) = tokio::sync::oneshot::channel();

    tokio::task::spawn(async move {
      if let Ok(()) = on_connected::<EnvConnection>().await {
        tx.send(true).ok();
      }
    });

    let mut conn = get_connection();

    let mut pipe = redis::pipe();

    pipe
      .atomic()
      .del("test::stream")
      .xgroup_create_mkstream("test::stream", "rustc", "0");

    let _: (i64, String) = pipe.query_async(&mut conn).await?;

    let result: RedisResult<String> = conn
      .xgroup_create_mkstream("test::stream", "rustc", "0")
      .await;

    match result {
      Err(err) if err.kind().eq(&ErrorKind::ExtensionError) => {
        assert_eq!(err.code(), Some("BUSYGROUP"));
      }
      _ => panic!("Expected BUSYGROUP error"),
    };

    conn.del("test::stream").await?;

    rx.try_recv().expect("on_connected not called");
    Ok(())
  }
}
