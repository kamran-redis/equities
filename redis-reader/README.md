# Redis Equities Reader

Small benchmark/demo for loading synthetic equities time-series data into Redis and reading it back with Java.

There are two storage formats:

- `str.py` writes each key as a Redis string using `APPEND`. Each point is packed as 12 bytes: little-endian `int64` timestamp plus `float32` value.
- `ts.py` writes each key as a RedisTimeSeries series using `TS.CREATE` and `TS.MADD`.

Both Java readers generate the same key names locally, so they do not need to scan Redis.

## Requirements

- Redis on `localhost:6379`
- Redis Stack or RedisTimeSeries module for `ts.py` / `TsReader`
- Python 3
- Java 17
- Maven

## Build

Run from this directory:

```bash
mvn package
```

This creates:

```text
target/redis-reader-1.0-SNAPSHOT.jar
```

## Load And Read Redis Strings

Run from this `redis-reader` directory. The Python script is one directory up.

```bash
python3 ../str.py | redis-cli --pipe
```

Then read the generated string keys:

```bash
java \
  -Dredis.host=localhost \
  -Dredis.port=6379 \
  -Dreader.threads=1 \
  -Dreader.pipelineBatch=500 \
  -Dreader.printPoints=false \
  -DMAX_KEYS=0 \
  -cp target/redis-reader-1.0-SNAPSHOT.jar \
  com.redis.equities.RedisReader
```

## Load And Read RedisTimeSeries

Run from this `redis-reader` directory:

```bash
python3 ../ts.py | redis-cli --pipe
```

Then read the generated time-series keys:

```bash
java \
  -Dredis.host=localhost \
  -Dredis.port=6379 \
  -Dreader.threads=1 \
  -Dreader.pipelineBatch=500 \
  -Dreader.printPoints=false \
  -DMAX_KEYS=0 \
  -cp target/redis-reader-1.0-SNAPSHOT.jar \
  com.redis.equities.TsReader
```

## Configuration

The loaders and readers must use matching dataset settings.

Common settings:

```bash
NUM_SYMBOLS=5000
NUM_RISKS=1
MAX_KEYS=0
```

Examples:

```bash
NUM_SYMBOLS=100 NUM_RISKS=1 python3 ../str.py | redis-cli --pipe
java -DNUM_SYMBOLS=100 -DNUM_RISKS=1 -DMAX_KEYS=0 -cp target/redis-reader-1.0-SNAPSHOT.jar com.redis.equities.RedisReader
```

Reader JVM properties:

- `redis.host`: Redis host, default `localhost`
- `redis.port`: Redis port, default `6379`
- `reader.threads`: number of reader threads, default is available CPUs
- `reader.pipelineBatch`: number of Redis commands per pipeline batch, default `500`
- `reader.printPoints`: set to `true` to print every point, default `false`
- `MAX_KEYS`: `0` reads all generated keys; any positive value limits the number of keys read

## Key Format

Keys are generated as:

```text
{SYM0001}:1D:0.01
{SYM0002}:1D:0.01
...
```

The `{SYMxxxx}` hash tag keeps all keys for a symbol in the same Redis Cluster slot.
