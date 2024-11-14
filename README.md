# Cuckoo Filter

[![CI build status](https://github.com/farhadi/cuckoo_filter/workflows/CI/badge.svg)](https://github.com/farhadi/cuckoo_filter/actions?query=workflow%3ACI)
[![codecov](https://codecov.io/gh/farhadi/cuckoo_filter/branch/main/graph/badge.svg)](https://codecov.io/gh/farhadi/cuckoo_filter)
[![Hex docs](http://img.shields.io/badge/hex.pm-docs-green.svg?style=flat)](https://hexdocs.pm/cuckoo_filter)
[![Hex Version](http://img.shields.io/hexpm/v/cuckoo_filter.svg?style=flat)](https://hex.pm/packages/cuckoo_filter)
[![License](http://img.shields.io/hexpm/l/cuckoo_filter.svg?style=flat)](https://github.com/farhadi/cuckoo_filter/blob/master/LICENSE)

A high-performance, concurrent, and mutable [Cuckoo Filter](https://www.cs.cmu.edu/~dga/papers/cuckoo-conext2014.pdf)
implemented using [atomics](https://erlang.org/doc/man/atomics.html) for Erlang and Elixir.

## Introduction

A **Cuckoo Filter** is a space-efficient probabilistic data structure for approximate
set membership queries. It enables constant-time checks to determine if an element
is in a set, using only a few bits per element. This efficiency comes with a trade-off:
a low rate of false positives may occur, but false negatives are guaranteed not to happen.

Compared to a **Bloom Filter**, a Cuckoo Filter offers a more efficient use of space and
supports the deletion of inserted elements. However, as the filter's load factor increases,
insertion operations may become slower and could fail once the filter is nearly full.

## Implementation Details

In this implementation, filter data is stored in an `atomics` array, a fixed-size, mutable
array of 64-bit integers. Using atomics enables fast, concurrent access to the filter
for both reading and writing operations.

The first two integers in the atomics array act as a lock to prevent race conditions
during relocatiion of elements.

The third integer acts as an atomic counter to track the number of elements in the filter.

### Fingerprint Constraints

To ensure atomic updates for fingerprints, this implementation only supports fingerprint
sizes of **4, 8, 16, 32, and 64 bits**—allowing multiple fingerprints to fit within a single
64-bit atomic integer.

### Insertion

Each element in a Cuckoo Filter can be placed in one of two possible buckets. If an empty
slot is available in either bucket, it is updated atomically. However, if both buckets
are full, elements need to be relocated to make room for the new one. In such cases,
atomic updates for all entries are not feasible, and insertions cannot be concurrent.
To manage this, a [spin lock](https://github.com/farhadi/spinlock) (using the first two
integers in the atomics array) prevents race conditions during these operations.

To maintain availability for lookups during element relocation, this implementation uses
an eviction cache. When relocating elements, the filter temporarily stores a sequence of
evictions. Once an empty slot is identified, changes are applied in reverse order,
ensuring that elements remain accessible for lookups throughout the relocation process.
Unlike many traditional Cuckoo Filter implementations, where inserting a new element
when the filter is full may lead to the removal of a random existing element,
this eviction cache technique helps avoid such unintended removals.

The eviction cache also provides early detection of loops, helping prevent excessive
evictions.

### Deletion

Deletion operations also utilize a lock to avoid race conditions when elements are
being relocated.

## Configurations

You can customize the fingerprint size, bucket size, hash function, and the maximum
number of evictions when creating a new filter.

By default, this implementation uses `erlang:phash2` for hashing. If a 32bit hash is
insufficient, [XXH3](https://github.com/farhadi/xxh3) hash functions are applied,
which require adding `xxh3` to your project dependencies manually.

When using a custom hash function, ensure that the hash output length meets or exceeds
the sum of the fingerprint and bucket sizes.

## Usage

In Erlang

```erlang
Filter = cuckoo_filter:new(1000),
ok = cuckoo_filter:add(Filter, 1),
true = cuckoo_filter:contains(Filter, 1),
false = cuckoo_filter:contains(Filter, 2),
ok = cuckoo_filter:delete(Filter, 1),
{error, not_found} = cuckoo_filter:delete(Filter, 1).
```

In Elixir

```elixir
filter = :cuckoo_filter.new(1000)
:ok = :cuckoo_filter.add(filter, 1)
true = :cuckoo_filter.contains(filter, 1)
false = :cuckoo_filter.contains(filter, 2)
:ok = :cuckoo_filter.delete(filter, 1)
{:error, :not_found} = :cuckoo_filter.delete(filter, 1)
```

For more details, see the [Hex documentation](https://hexdocs.pm/cuckoo_filter).

## License

Copyright 2021, Ali Farhadi <a.farhadi@gmail.com>.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.