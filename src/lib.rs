use arc_swap::ArcSwap;
use env_url::*;
use futures_util::future::FutureExt;
use once_cell::sync::Lazy;
use redis::{
  aio::{ConnectionLike, MultiplexedConnection},
  Client, Cmd, ErrorKind, Pipeline, RedisError, RedisFuture, RedisResult, Value,
};
use std::sync::Arc;
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
  fn client<'a>(&'a self) -> &'a RedisResult<Client>;
}

#[derive(EnvURL)]
#[env_url(env_prefix = "REDIS", default = "redis://127.0.0.1:6379")]
pub struct RedisEnvDB {
  client: RedisResult<Client>,
  db_index: i64,
}

impl ConnectionInfo for RedisEnvDB {
  fn new(client: RedisResult<Client>, db_index: i64) -> Self {
    RedisEnvDB { client, db_index }
  }
  fn get_db(&self) -> i64 {
    self.db_index
  }

  fn client<'a>(&'a self) -> &'a RedisResult<Client> {
    &self.client
  }
}

impl Default for RedisEnvDB {
  fn default() -> Self {
    match RedisEnvDB::service_url() {
      Ok(url) => RedisEnvDB::from_url(&url),
      Err(_) => {
        let client = Err(RedisError::from((
          ErrorKind::InvalidClientConfig,
          "Invalid Redis connection URL",
        )));

        RedisEnvDB {
          client,
          db_index: 0,
        }
      }
    }
  }
}

enum ConnectionState {
  Idle,
  Connecting,
  ClientError(ErrorKind),
  ConnectionError(ErrorKind),
  Connected(MultiplexedConnection),
}

pub struct ConnectionManager<T: ConnectionInfo> {
  state: ArcSwap<ConnectionState>,
  notify: Notify,
  connection_info: T,
}

impl<T> ConnectionManager<T>
where
  T: ConnectionInfo,
{
  pub fn new(connection_info: T) -> Arc<ConnectionManager<T>> {
    Arc::new(ConnectionManager {
      state: ArcSwap::from(Arc::new(ConnectionState::Idle)),
      notify: Notify::new(),
      connection_info,
    })
  }

  pub fn get_connection(self: &Arc<Self>) -> ManagedConnection<T> {
    ManagedConnection(self.clone())
  }

  pub async fn on_connected(self: &Arc<Self>) -> RedisResult<()> {
    loop {
      self.notify.notified().await;

      let state = self.state.load_full();

      match state.as_ref() {
        ConnectionState::ClientError(kind) => {
          break Err::<(), _>(RedisError::from((
            kind.to_owned(),
            "Invalid Redis connection URL",
          )));
        }
        ConnectionState::ConnectionError(kind) => {
          if kind.ne(&ErrorKind::IoError) {
            break Err::<(), _>(RedisError::from((
              kind.to_owned(),
              "Unable to establish Redis connection",
            )));
          }
        }
        ConnectionState::Connected(_) => break Ok(()),
        _ => continue,
      }
    }
  }

  async fn get_multiplexed_connection(self: &Arc<Self>) -> RedisResult<Arc<ConnectionState>> {
    let mut i = 0;

    loop {
      let state = self.state.load_full();

      match state.as_ref() {
        ConnectionState::Idle => {
          self.clone().establish_connection(&state);
        }
        ConnectionState::Connecting => {
          self.notify.notified().await;
        }
        ConnectionState::ClientError(kind) => {
          break Err::<Arc<ConnectionState>, _>(RedisError::from((
            kind.to_owned(),
            "Invalid Redis connection URL",
          )))
        }
        ConnectionState::ConnectionError(kind) => {
          if kind.eq(&ErrorKind::IoError) && i.eq(&0) {
            self.clone().establish_connection(&state);
          } else {
            break Err::<Arc<ConnectionState>, _>(RedisError::from((
              kind.to_owned(),
              "Unable to establish Redis connection",
            )));
          }
        }
        ConnectionState::Connected(_) => break Ok(state),
      }

      i = i + 1;
    }
  }

  fn store_and_notify(self: &Arc<Self>, state: ConnectionState) {
    self.state.store(Arc::new(state));
    self.notify.notify_waiters();
  }

  fn establish_connection(self: Arc<Self>, current: &Arc<ConnectionState>) {
    let prev = self
      .state
      .compare_and_swap(current, Arc::new(ConnectionState::Connecting));

    if Arc::ptr_eq(&prev, current) {
      tokio::task::spawn(async move {
        match self.connection_info.client() {
          Ok(client) => {
            if let Err(err) = self.start_new_connection(client).await {
              self.store_and_notify(ConnectionState::ConnectionError(err.kind()))
            }
          }
          Err(err) => self.store_and_notify(ConnectionState::ClientError(err.kind())),
        }
      });
    }
  }

  async fn start_new_connection(self: &Arc<Self>, client: &Client) -> RedisResult<()> {
    let conn = client.get_multiplexed_tokio_connection().await?;

    self.store_and_notify(ConnectionState::Connected(conn));

    Ok(())
  }
}

pub struct ManagedConnection<T: ConnectionInfo>(Arc<ConnectionManager<T>>);

impl<T> ConnectionLike for ManagedConnection<T>
where
  T: ConnectionInfo,
{
  fn req_packed_command<'a>(&'a mut self, cmd: &'a Cmd) -> RedisFuture<'a, Value> {
    (async move {
      let state = self.0.get_multiplexed_connection().await?;

      let mut conn = match &*state {
        ConnectionState::Connected(conn) => conn.to_owned(),
        _ => unreachable!(),
      };

      match conn.req_packed_command(cmd).await {
        Ok(result) => Ok(result),
        Err(err) => {
          if err.is_connection_dropped() {
            self.0.clone().establish_connection(&state);
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
      let state = self.0.get_multiplexed_connection().await?;

      let mut conn = match &*state {
        ConnectionState::Connected(conn) => conn.to_owned(),
        _ => unreachable!(),
      };

      match conn.req_packed_commands(cmd, offset, count).await {
        Ok(result) => Ok(result),
        Err(err) => {
          if err.is_connection_dropped() {
            self.0.clone().establish_connection(&state);
          }

          Err(err)
        }
      }
    })
    .boxed()
  }

  fn get_db(&self) -> i64 {
    self.0.connection_info.get_db()
  }
}

static CONNECTION_MANAGER: Lazy<Arc<ConnectionManager<RedisEnvDB>>> = Lazy::new(|| {
  let connection_info = RedisEnvDB::default();

  ConnectionManager::new(connection_info)
});

/// Get a managed multiplexed connection for the default env-configured Redis database
pub fn get_connection() -> ManagedConnection<RedisEnvDB> {
  (*CONNECTION_MANAGER).get_connection()
}

/// Notify on next connection. Useful for life-cycle based behaviors
pub async fn on_connected() -> RedisResult<()> {
  (*CONNECTION_MANAGER).on_connected().await
}
