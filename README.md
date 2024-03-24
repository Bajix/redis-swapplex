# redis-swapplex

![License](https://img.shields.io/badge/license-MIT-green.svg)
[![Cargo](https://img.shields.io/crates/v/redis-swapplex.svg)](https://crates.io/crates/redis-swapplex)
[![Documentation](https://docs.rs/redis-swapplex/badge.svg)](https://docs.rs/redis-swapplex)

Redis multiplexing with reconnection notifications and MGET auto-batching

Why use this instead of [redis::aio::ConnectionManager](https://docs.rs/redis/latest/redis/aio/struct.ConnectionManager.html)?
- Error-free reconnection behavior: when a command would otherwise fail as a consequence of the connection being dropped, this library will immediately reconnect and retry when able without producing an otherwise avoidable IoError and with subsequent reconnections debounced 1500ms
- ENV configuration simplifies kubernetes usage
- Reconnects can be observed allowing for Redis [server-assisted client-side caching](https://redis.io/docs/manual/client-side-caching/) using client tracking redirection
- Integrated MGET auto-batching

## ENV Configuration

Composible connection urls are provided by environment variables using [env-url](https://crates.io/crates/env-url) with the `REDIS` prefix:

```
REDIS_URL=redis://127.0.0.1:6379
# Override env mapping for easy kubernetes config
REDIS_HOST_ENV=MONOLITH_STAGE_REDIS_MASTER_PORT_6379_TCP_ADDR
REDIS_PORT_ENV=MONOLITH_STAGE_REDIS_MASTER_SERVICE_PORT_REDIS
```

## Example

```rust
use redis::{AsyncCommands, RedisResult};
use redis_swapplex::get_connection;

async fn get_value(key: &str) -> RedisResult<String> {
  let mut conn = get_connection();
  conn.get(key).await
}
```

## Runtime Configuration (optional)

For best performance, use the Tokio runtime as configured via the [tokio::main](https://docs.rs/tokio/latest/tokio/attr.main.html) or [tokio::test](https://docs.rs/tokio/latest/tokio/attr.test.html) macro with the `crate` attribute set to `async_local` while the `barrier-protected-runtime` feature is enabled on [`async-local`](https://crates.io/crates/async-local). Doing so configures the Tokio runtime with a barrier that rendezvous runtime worker threads during shutdown in a way that ensures tasks never outlive thread local data owned by runtime worker threads and obviates the need for [Box::leak](https://doc.rust-lang.org/std/boxed/struct.Box.html#method.leak) as a means of lifetime extension.
