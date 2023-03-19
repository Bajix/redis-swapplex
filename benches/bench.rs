use criterion::{criterion_group, criterion_main, Criterion};
use fred::prelude::*;
use redis::{aio::ConnectionManager, AsyncCommands};
use redis_swapplex::{ConnectionManagerContext, EnvConnection};
use tokio::runtime::Builder;

fn bench_redis(c: &mut Criterion) {
  let rt = Builder::new_multi_thread()
    .enable_io()
    .build()
    .expect("Unable to create Tokio runtime");

  let conn_manager = rt.block_on(async {
    // connection established on creation
    let client = EnvConnection::client()
      .as_ref()
      .expect("Unable to get redis client");

    ConnectionManager::new(client.to_owned())
      .await
      .expect("Unable to establish Redis connection")
  });

  rt.block_on(async {
    // connection established on first use
    let _: () = EnvConnection::get_connection()
      .set("test", "test")
      .await
      .expect("Unable to establish Redis connection");
  });

  c.benchmark_group("Multiplexed Redis GET");

  c.bench_function("redis::aio::ConnectionManager", |b| {
    b.to_async(&rt).iter(|| async {
      let mut conn = conn_manager.clone();
      let _: () = conn.get("test").await.unwrap();
    })
  });

  c.bench_function("redis_swapplex::EnvConnection", |b| {
    b.to_async(&rt).iter(|| async {
      let mut conn = EnvConnection::get_connection();
      let _: () = conn.get("test").await.unwrap();
    })
  });

  c.benchmark_group("Redis GET");

  let client = rt.block_on(async {
    let config = RedisConfig::default();
    let policy = ReconnectPolicy::default();
    let client = RedisClient::new(config);

    // connect to the server, returning a handle to the task that drives the connection
    let _ = client.connect(Some(policy));
    let _ = client
      .wait_for_connect()
      .await
      .expect("Unable to establish Redis connection");

    client
  });

  c.bench_function("fred::clients::redis::RedisClient", |b| {
    b.to_async(&rt).iter(|| async {
      let _: () = client.get("test").await.unwrap();
    });
  });
}

criterion_group!(benches, bench_redis);
criterion_main!(benches);
