# redis-swapplex

![License](https://img.shields.io/badge/license-MIT-green.svg)
[![Cargo](https://img.shields.io/crates/v/redis-swapplex.svg)](https://crates.io/crates/redis-swapplex)
[![Documentation](https://docs.rs/redis-swapplex/badge.svg)](https://docs.rs/redis-swapplex)

Atomic state-transition based Redis multiplexing with reconnection notifications. Connection configuration is provided by [env-url](https://crates.io/crates/env-url).

Why use this instead of [redis::aio::ConnectionManager](https://docs.rs/redis/latest/redis/aio/struct.ConnectionManager.html)?
- Error-free reconnection behavior: when a command would otherwise fail as a consequence of the connection being dropped, this library will immediately reconnect and retry when able without producing an otherwise avoidable IoError and with subsequent reconnections debounced 1500ms
- Less contention overhead: the usage of [arc_swap::cache::Cache](https://docs.rs/arc-swap/latest/arc_swap/cache/struct.Cache.html) results in a 10-25x speed up of cached connection acquisition.
- ENV configuration makes life easier and simplifies kubernetes usage
- Reconnects can be observed, thus allowing for Redis [server-assisted client-side caching](https://redis.io/docs/manual/client-side-caching/) using client tracking redirection

````
REDIS_URL=redis://127.0.0.1:6379
# Override env mapping for easy kubernetes config
REDIS_HOST_ENV=MONOLITH_STAGE_REDIS_MASTER_PORT_6379_TCP_ADDR
REDIS_PORT_ENV=MONOLITH_STAGE_REDIS_MASTER_SERVICE_PORT_REDIS
```

```rust
use redis::{AsyncCommands, RedisResult};
use redis_swapplex::get_connection;

async fn get_value(key: &str) -> RedisResult<String> {
  let mut conn = get_connection();
  conn.get(key).await
}
```
