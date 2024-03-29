use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use fred::prelude::*;
use futures_util::{stream::FuturesUnordered, StreamExt};
use redis::{aio::ConnectionManager, AsyncCommands};
use redis_swapplex::{get, ConnectionManagerContext, EnvConnection};
use tokio::runtime::Builder;

fn bench_redis(c: &mut Criterion) {
  let rt = Builder::new_current_thread()
    .enable_all()
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
    let mset = (0..512)
      .into_iter()
      .map(|i| (i.to_string(), i.to_string()))
      .chain(std::iter::once(("test".to_string(), "test".to_string())))
      .collect::<Vec<_>>();
    // connection established on first use
    let _: () = EnvConnection::get_connection()
      .mset(mset.as_slice())
      .await
      .expect("Unable to establish Redis connection");
  });

  let mut bench_multiplexed = c.benchmark_group("Multiplexed GET");

  bench_multiplexed.bench_function("redis::aio::ConnectionManager", |b| {
    b.to_async(&rt).iter(|| async {
      let mut conn = conn_manager.clone();
      let _: () = conn.get("test").await.unwrap();
    })
  });

  bench_multiplexed.bench_function("redis_swapplex::EnvConnection", |b| {
    b.to_async(&rt).iter(|| async {
      let mut conn = EnvConnection::get_connection();
      let _: () = conn.get("test").await.unwrap();
    })
  });

  bench_multiplexed.bench_function("redis_swapplex::get", |b| {
    b.to_async(&rt).iter(|| async {
      get("test").await.unwrap();
    });
  });

  for n in 4..10 {
    let batch_size: u64 = 1 << n;

    bench_multiplexed.bench_with_input(
      BenchmarkId::new("redis::aio::ConnectionManager", batch_size),
      &batch_size,
      |b, batch_size| {
        b.to_async(&rt).iter(|| async {
          let tasks: FuturesUnordered<_> = (0..*batch_size)
            .map(|i| {
              let mut conn = conn_manager.clone();

              async move { conn.get::<'_, _, Vec<u8>>(i).await }
            })
            .collect();

          tasks.collect::<Vec<_>>().await;
        })
      },
    );

    bench_multiplexed.bench_with_input(
      BenchmarkId::new("redis_swapplex::get", batch_size),
      &batch_size,
      |b, batch_size| {
        b.to_async(&rt).iter(|| async {
          let tasks: FuturesUnordered<_> = (0..*batch_size).map(|i| get(i)).collect();

          tasks.collect::<Vec<_>>().await;
        })
      },
    );
  }

  bench_multiplexed.finish();

  let mut bench_regular = c.benchmark_group("GET");

  let client = rt.block_on(async {
    let config = RedisConfig::default();
    let client = RedisClient::new(config, None, None, None);

    // connect to the server, returning a handle to the task that drives the connection
    let _ = client.connect();
    let _ = client
      .wait_for_connect()
      .await
      .expect("Unable to establish Redis connection");

    client
  });

  bench_regular.bench_function("fred::clients::redis::RedisClient", |b| {
    b.to_async(&rt).iter(|| async {
      let _: () = client.get("test").await.unwrap();
    });
  });

  bench_regular.finish();
}

criterion_group!(benches, bench_redis);
criterion_main!(benches);
