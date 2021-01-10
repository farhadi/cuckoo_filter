# Cuckoo Filter

[![CI build status](https://github.com/farhadi/cuckoo_filter/workflows/CI/badge.svg)](https://github.com/farhadi/cuckoo_filter/actions?query=workflow%3ACI)
[![codecov](https://codecov.io/gh/farhadi/cuckoo_filter/branch/main/graph/badge.svg)](https://codecov.io/gh/farhadi/cuckoo_filter)

A high-performance, concurrent, and mutable [Cuckoo Filter](https://www.cs.cmu.edu/~dga/papers/cuckoo-conext2014.pdf)
implemented using [atomics](https://erlang.org/doc/man/atomics.html) for Erlang and Elixir.

## Introduction

A Cuckoo Filter is a space-efficient probabilistic data structure for approximated
set-membership queries. It can be used to test whether an element is a member of a set in
constant time and requires only a few bits per element. The trade-off is that a low rate
of false positives is possible, but false negatives are not.

Cuckoo Filter is considered a better alternative to Bloom Filter with lower space
overhead and with support for deletion of inserted elements.

Insertion of elements in a cuckoo filter becomes slowers when the load factor is high and
might fail when capacity is nearly full.

## Implementation

In this implementation, filter data is stored in an atomics array, which is a fixed-size
mutable array of 64 bits integers. By using atomics we can have fast and concurrent
access to the filter for both reads and writes.

To be able to update fingerprints atomically, this implementation only allows fingerprint
sizes of 4, 8, 16, and 32 bits, so that multiple fingerprints can fit in a single 64 bits
atomic integer.

In a Cuckoo Filter for each element, there are two buckets where it can be inserted. To
insert an element when there is an empty slot available in one of the two buckets, we
can atomically update that empty entry, but when there is no slot available in the two
buckets we have to relocate exiting elements to make room for the new one. In this case,
since multiple entries need to be updated, we no longer can do insert operation
atomically, and such inserts can not be done concurrently. In such cases, to prevent
race conditions we use the first integer in the atomics array as a mutex.

Deletes are also done with a lock to prevent race conditions when an insert is relocating
elements.

When relocating existing elements, it is also possible that a concurrent lookup of an
evicted element fails. To prevent this, instead of immediately updating an evicted
element, we keep a sequence of evictions in memory, and once we find an empty slot we
start applying the changes in the sequence from the end to the beginning. In other words,
to relocate an element, we first insert it in its new place, then we remove it from its
old place, making sure the element is always available to lookups.

In most cuckoo filter implementations, when an insert fails, the element is actually
added to the filter, but some other random element gets removed, but in this
implementation, by using this eviction cache technique we can also avoid the removal
of a random element when an insertion fails.

Another benefit of keeping the chain of evictions in memory is the early detection of
loops before reaching the maximum number of evictions.

The second integer in the array is used as an atomic counter to store the number of
elements inserted in the filter. 

For hashing, 64-bit variant of xxHash is used.

Fingerprint size, bucket size, and the maximum number of evictions are configurable when
creating a new filter.

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

For more details, see the module documentation.

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