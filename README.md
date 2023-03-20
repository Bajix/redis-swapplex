# redis-swapplex

![License](https://img.shields.io/badge/license-MIT-green.svg)
[![Cargo](https://img.shields.io/crates/v/redis-swapplex.svg)](https://crates.io/crates/redis-swapplex)
[![Documentation](https://docs.rs/redis-swapplex/badge.svg)](https://docs.rs/redis-swapplex)

Redis multiplexing with reconnection notifications and MGET auto-batching. Connection configuration is provided by [env-url](https://crates.io/crates/env-url).

Why use this instead of [redis::aio::ConnectionManager](https://docs.rs/redis/latest/redis/aio/struct.ConnectionManager.html)?
- Error-free reconnection behavior: when a command would otherwise fail as a consequence of the connection being dropped, this library will immediately reconnect and retry when able without producing an otherwise avoidable IoError and with subsequent reconnections debounced 1500ms
- ENV configuration simplifies kubernetes usage
- Reconnects can be observed allowing for Redis [server-assisted client-side caching](https://redis.io/docs/manual/client-side-caching/) using client tracking redirection
- Integrated MGET auto-batching (up to 180x more performant than GET)

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

## Runtime Configuration

By utilizing a barrier to guard thread local data destruction until runtime threads rendezvous during shutdown, it becomes possible to create thread-safe pointers to thread-local data owned by runtime worker threads. In order for [async-local](https://docs.rs/async-local) to protect thread local data within an async context, the provided barrier-protected Tokio Runtime must be used to ensure tasks never outlive thread local data owned by worker threads. By default, this crate makes no assumptions about the runtime used, and comes with the `leaky-context` feature flag enabled which prevents [Context<T>](https://docs.rs/async-local/latest/async_local/struct.Context.html) from ever deallocating by using [Box::leak](https://doc.rust-lang.org/std/boxed/struct.Box.html#method.leak); to avoid this extra indirection, disable `leaky-context` and configure the runtime using the [tokio::main](https://docs.rs/tokio/latest/tokio/attr.main.html) or [tokio::test](https://docs.rs/tokio/latest/tokio/attr.test.html) macro with the `crate` attribute set to `async_local` with only the `barrier-protected-runtime` feature flag set on [`async-local`](https://docs.rs/async-local).

## Stable Usage

This crate conditionally makes use of the nightly only feature [type_alias_impl_trait](https://rust-lang.github.io/rfcs/2515-type_alias_impl_trait.html) to allow async fns in traits to be unboxed. To compile on `stable` the `boxed` feature flag can be used to downgrade [async_t::async_trait](https://docs.rs/async_t/latest/async_t/attr.async_trait.html) to [async_trait::async_trait](https://docs.rs/async-trait/latest/async_trait).
