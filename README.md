# Concurrent In-Memory Key-Value Store (C++)

A Redis-like in-memory key-value store implemented in modern C++, designed to handle concurrent requests efficiently using a thread pool and sharded storage.

## What It Does

- Supports basic key-value operations: GET, SET, DEL
- Processes requests in parallel using a fixed-size thread pool
- Uses a blocking queue for task scheduling
- Distributes data across multiple shards to reduce contention
- Ensures thread-safe access to shared data

## Key Skills Demonstrated

- Multithreading and synchronization in C++
- Thread pool and producer–consumer pattern
- Mutexes, condition variables, and read–write locks
- Sharding for scalable concurrent access
- Clean shutdown of worker threads
