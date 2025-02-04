[![progress-banner](https://backend.codecrafters.io/progress/redis/ecc5ac1f-3828-4f3a-9eac-4a6707d87a5d)](https://app.codecrafters.io/users/codecrafters-bot?r=2qF)

My approach to the ['Build Your Own Redis'](https://app.codecrafters.io/courses/redis/) challenge.

## Features  

- **Basic Commands**: 
  - `GET`
  - `SET` (with TTL)
  - `PING`
  - `ECHO`  
- **Concurrency**: Supports multiple concurrent clients  
- **Persistence**: Loads RDB files and serves the content
- **Replication**:  
  - Performs replication handshake  
  - Propagates commands to replicas  
  - Supports the `WAIT` command  
- **Streams**:
  - Create streams
  - Support `XREAD` and `XRANGE` commands
  - Support blocking reads (`XREAD block`)
  - Support different type of queries (with `+`, `-`, `$`, partially/fully create IDs)
- **Transactions**:
  - `INCR` command
  - `MULTI` and `EXEC` commands
  - Multiple concurrent transactions with failures within transactions 

## RUN

- **Build**:
  - `go build -o redis app/*.go`
- **Basic execution**:
  - `./redis --port 5678`
- **Parse RDB**:
  - `./redis --dir ./redis-data/ --dbfilename dump.rdb --port 5678`
- **Run as replica**:
  - `./redis --port 1234 --replicaof "localhost 6379"`
  
After the server is running, you can connect to it via any Redis client, Including `redis-cli`;
Ex: `redis-cli xread block 10000 streams orange 0-110`.

**NOTE**: The current version fully supports RESP V2.

