# redis-swapplex

![License](https://img.shields.io/badge/license-MIT-green.svg)
[![Cargo](https://img.shields.io/crates/v/redis-swapplex.svg)](https://crates.io/crates/redis-swapplex)
[![Documentation](https://docs.rs/redis-swapplex/badge.svg)](https://docs.rs/redis-swapplex)

Atomic state-transition based Redis multiplexing with reconnection notifications. Connection configuration is provided by [env-url](https://crates.io/crates/env-url).


````
REDIS_URL=redis://127.0.0.1:6379
# Override env mapping for easy kubernetes config
REDIS_HOST_ENV=MONOLITH_STAGE_REDIS_MASTER_PORT_6379_TCP_ADDR
REDIS_PORT_ENV=MONOLITH_STAGE_REDIS_MASTER_SERVICE_PORT_REDIS
```
