-module(cuckoo_filter).

-export([
    new/1, new/2, new/3,
    add/2, add/3,
    contains/2,
    delete/2, delete/3,
    size/1,
    capacity/1,
    export/1,
    import/2
]).

-include("cuckoo_filter.hrl").

%% Default configurations
-define(DEFAULT_FINGERPRINT_SIZE, 16).
-define(DEFAULT_BUCKET_SIZE, 4).
-define(DEFAULT_EVICTIONS, 100).

new(Capacity) ->
    new(Capacity, []).

new(Capacity, Data) when is_binary(Data) ->
    new(Capacity, Data, []);
new(Capacity, Opts) ->
    is_integer(Capacity) andalso Capacity > 0 orelse error(badarg),
    BucketSize = proplists:get_value(bucket_size, Opts, ?DEFAULT_BUCKET_SIZE),
    is_integer(BucketSize) andalso BucketSize > 0 orelse error(badarg),
    FingerprintSize = proplists:get_value(fingerprint_size, Opts, ?DEFAULT_FINGERPRINT_SIZE),
    lists:member(FingerprintSize, [4, 8, 16, 32]) orelse error(badarg),
    MaxEvictions = proplists:get_value(max_evictions, Opts, ?DEFAULT_EVICTIONS),
    is_integer(MaxEvictions) andalso MaxEvictions >= 0 orelse error(badarg),
    NumBuckets = 1 bsl ceil(math:log2(Capacity / BucketSize)),
    AtomicsSize = ceil(NumBuckets * BucketSize * FingerprintSize / 64) + 2,
    #cuckoo_filter{
        buckets = atomics:new(AtomicsSize, [{signed, false}]),
        num_buckets = NumBuckets,
        bucket_size = BucketSize,
        fingerprint_size = FingerprintSize,
        max_evictions = MaxEvictions
    }.

new(Capacity, Data, Opts) when is_binary(Data) ->
    Filter = new(Capacity, Opts),
    case import(Filter, Data) of
        ok ->
            Filter;
        {error, Error} ->
            {error, Error}
    end.

import(#cuckoo_filter{buckets = Buckets} = Filter, Data) when is_binary(Data) ->
    ByteSize = (maps:get(size, atomics:info(Buckets)) - 1) * 8,
    case byte_size(Data) of
        ByteSize ->
            ok = write_lock(Filter, infinity),
            import(Buckets, Data, 2),
            release_write_lock(Filter);
        _ ->
            {error, invalid_data_size}
    end.

import(_Buckets, <<>>, _Index) ->
    ok;
import(Buckets, <<Atomic:64/big-unsigned-integer, Data/binary>>, Index) ->
    atomics:put(Buckets, Index, Atomic),
    import(Buckets, Data, Index + 1).

export(#cuckoo_filter{buckets = Buckets} = Filter) ->
    ok = write_lock(Filter, infinity),
    AtomicsSize = maps:get(size, atomics:info(Buckets)),
    Result = <<
        <<(atomics:get(Buckets, I)):64/big-unsigned-integer>>
        || I <- lists:seq(2, AtomicsSize)
    >>,
    release_write_lock(Filter),
    Result.

add(Filter, Data) ->
    add(Filter, Data, infinity).

add(
    #cuckoo_filter{fingerprint_size = FingerprintSize, num_buckets = NumBuckets} = Filter,
    Data,
    LockTimeout
) ->
    {Index, Fingerprint} = index_and_fingerprint(Data, FingerprintSize, NumBuckets),
    case insert_at_index(Filter, Index, Fingerprint) of
        ok ->
            ok;
        {error, full} ->
            AltIndex = alt_index(Index, Fingerprint, NumBuckets),
            case insert_at_index(Filter, AltIndex, Fingerprint) of
                ok ->
                    ok;
                {error, full} ->
                    RandIndex = element(rand:uniform(2), {Index, AltIndex}),
                    force_insert(Filter, RandIndex, Fingerprint, LockTimeout)
            end
    end.

contains(
    #cuckoo_filter{fingerprint_size = FingerprintSize, num_buckets = NumBuckets} = Filter,
    Data
) ->
    {Index, Fingerprint} = index_and_fingerprint(Data, FingerprintSize, NumBuckets),
    lookup_index(Filter, Index, Fingerprint).

delete(Filter, Data) ->
    delete(Filter, Data, infinity).

delete(
    #cuckoo_filter{fingerprint_size = FingerprintSize, num_buckets = NumBuckets} =
        Filter,
    Data,
    LockTimeout
) ->
    case write_lock(Filter, LockTimeout) of
        ok ->
            {Index, Fingerprint} = index_and_fingerprint(Data, FingerprintSize, NumBuckets),
            case delete_fingerprint(Filter, Fingerprint, Index) of
                ok ->
                    release_write_lock(Filter);
                {error, not_found} ->
                    AltIndex = alt_index(Index, Fingerprint, NumBuckets),
                    Result = delete_fingerprint(Filter, Fingerprint, AltIndex),
                    release_write_lock(Filter),
                    Result
            end;
        {error, timeout} ->
            {error, timeout}
    end.

capacity(#cuckoo_filter{bucket_size = BucketSize, num_buckets = NumBuckets}) ->
    NumBuckets * BucketSize.

size(#cuckoo_filter{buckets = Buckets}) ->
    atomics:get(Buckets, 2).

lookup_index(Filter, Index, Fingerprint) ->
    Bucket = read_bucket(Index, Filter),
    case lists:member(Fingerprint, Bucket) of
        true ->
            true;
        false ->
            lookup_alt_index(Filter, Index, Fingerprint, Bucket)
    end.

lookup_alt_index(#cuckoo_filter{num_buckets = NumBuckets} = Filter, Index, Fingerprint, Bucket) ->
    AltIndex = alt_index(Index, Fingerprint, NumBuckets),
    AltBucket = read_bucket(AltIndex, Filter),
    case lists:member(Fingerprint, AltBucket) of
        true ->
            true;
        false ->
            case read_bucket(Index, Filter) of
                Bucket -> false;
                _ -> lookup_index(Filter, Index, Fingerprint)
            end
    end.

delete_fingerprint(Filter, Fingerprint, Index) ->
    Bucket = read_bucket(Index, Filter),
    case find_in_bucket(Bucket, Fingerprint) of
        {ok, SubIndex} ->
            ok = update_in_bucket(Filter, Index, SubIndex, Fingerprint, 0);
        {error, not_found} ->
            {error, not_found}
    end.

hash(Data) when is_binary(Data) ->
    xxhash:hash64(Data);
hash(Data) ->
    hash(term_to_binary(Data)).

fingerprint(Hash, FingerprintSize) ->
    case Hash band (1 bsl FingerprintSize - 1) of
        0 ->
            1;
        Fingerprint ->
            Fingerprint
    end.

index_and_fingerprint(Data, FingerprintSize, NumBuckets) ->
    Hash = hash(Data),
    Fingerprint = fingerprint(Hash, FingerprintSize),
    Index = (Hash bsr FingerprintSize) rem NumBuckets,
    {Index, Fingerprint}.

alt_index(Index, Fingerprint, NumBuckets) ->
    Index bxor hash(Fingerprint) rem NumBuckets.

atomic_index(BitIndex) ->
    BitIndex div 64 + 3.

insert_at_index(Filter, Index, Fingerprint) ->
    Bucket = read_bucket(Index, Filter),
    case find_in_bucket(Bucket, 0) of
        {ok, SubIndex} ->
            case update_in_bucket(Filter, Index, SubIndex, 0, Fingerprint) of
                ok ->
                    ok;
                {error, outdated} ->
                    insert_at_index(Filter, Index, Fingerprint)
            end;
        {error, not_found} ->
            {error, full}
    end.

write_lock(#cuckoo_filter{buckets = Buckets}, infinity) ->
    write_lock(Buckets, infinity);
write_lock(#cuckoo_filter{buckets = Buckets}, Timeout) ->
    write_lock(Buckets, erlang:monotonic_time(microsecond) + Timeout * 1000);
write_lock(Buckets, Timeout) ->
    case atomics:compare_exchange(Buckets, 1, 0, 1) of
        ok ->
            ok;
        1 ->
            case erlang:monotonic_time(microsecond) > Timeout of
                true ->
                    {error, timeout};
                false ->
                    write_lock(Buckets, Timeout)
            end
    end.

release_write_lock(#cuckoo_filter{buckets = Buckets}) ->
    atomics:put(Buckets, 1, 0).

force_insert(Filter, Index, Fingerprint, LockTimeout) ->
    case write_lock(Filter, LockTimeout) of
        ok ->
            Result =
                force_insert(Filter, Index, Fingerprint, #{}, [], Filter#cuckoo_filter.bucket_size),
            release_write_lock(Filter),
            Result;
        {error, timeout} ->
            {error, timeout}
    end.

force_insert(_Filter, _Index, _Fingerprint, _Evictions, _EvictionsList, 0) ->
    {error, full};
force_insert(
    #cuckoo_filter{max_evictions = MaxEvictions},
    _Index,
    _Fingerprint,
    Evictions,
    _EvictionsList,
    _Retry
) when map_size(Evictions) == MaxEvictions ->
    {error, full};
force_insert(
    #cuckoo_filter{bucket_size = BucketSize, num_buckets = NumBuckets} = Filter,
    Index,
    Fingerprint,
    Evictions,
    EvictionsList,
    Retry
) ->
    Bucket = read_bucket(Index, Filter),
    SubIndex = rand:uniform(BucketSize) - 1,
    case lists:nth(SubIndex + 1, Bucket) of
        0 ->
            case insert_at_index(Filter, Index, Fingerprint) of
                ok ->
                    persist_evictions(Filter, Evictions, EvictionsList, Fingerprint);
                {error, full} ->
                    force_insert(Filter, Index, Fingerprint, Evictions, EvictionsList, BucketSize)
            end;
        Evicted ->
            AltIndex = alt_index(Index, Evicted, NumBuckets),
            Key = {Index, SubIndex},
            if
                is_map_key(Key, Evictions) ->
                    force_insert(Filter, Index, Fingerprint, Evictions, EvictionsList, Retry - 1);
                true ->
                    case insert_at_index(Filter, AltIndex, Evicted) of
                        ok ->
                            persist_evictions(
                                Filter,
                                Evictions#{Key => Fingerprint},
                                [Key | EvictionsList],
                                Evicted
                            ),
                            ok;
                        {error, full} ->
                            force_insert(
                                Filter,
                                AltIndex,
                                Evicted,
                                Evictions#{Key => Fingerprint},
                                [Key | EvictionsList],
                                BucketSize
                            )
                    end
            end
    end.

persist_evictions(_Filter, _Evictions, [], _Evicted) ->
    ok;
persist_evictions(
    Filter,
    Evictions,
    [Key = {Index, SubIndex} | EvictionsList],
    Evicted
) ->
    Fingerprint = maps:get(Key, Evictions),
    ok = update_in_bucket(Filter, Index, SubIndex, Evicted, Fingerprint),
    persist_evictions(Filter, Evictions, EvictionsList, Fingerprint).

find_in_bucket(Bucket, Fingerprint) ->
    find_in_bucket(Bucket, Fingerprint, 0).

find_in_bucket([], _Fingerprint, _Index) ->
    {error, not_found};
find_in_bucket([Fingerprint | _Bucket], Fingerprint, Index) ->
    {ok, Index};
find_in_bucket([_ | Bucket], Fingerprint, Index) ->
    find_in_bucket(Bucket, Fingerprint, Index + 1).

read_bucket(
    Index,
    #cuckoo_filter{
        buckets = Buckets,
        bucket_size = BucketSize,
        fingerprint_size = FingerprintSize
    }
) ->
    BucketBitSize = BucketSize * FingerprintSize,
    BitIndex = Index * BucketBitSize,
    AtomicIndex = atomic_index(BitIndex),
    SkipBits = BitIndex rem 64,
    EndIndex = atomic_index(BitIndex + BucketBitSize - 1),
    <<_:SkipBits, Bucket:BucketBitSize/bitstring, _/bitstring>> = <<
        <<(atomics:get(Buckets, I)):64/big-unsigned-integer>>
        || I <- lists:seq(AtomicIndex, EndIndex)
    >>,
    [F || <<F:FingerprintSize/big-unsigned-integer>> <= Bucket].

update_in_bucket(
    #cuckoo_filter{
        buckets = Buckets,
        bucket_size = BucketSize,
        fingerprint_size = FingerprintSize
    } = Filter,
    Index,
    SubIndex,
    OldValue,
    Value
) ->
    BitIndex = Index * BucketSize * FingerprintSize + SubIndex * FingerprintSize,
    AtomicIndex = atomic_index(BitIndex),
    SkipBits = BitIndex rem 64,
    AtomicValue = atomics:get(Buckets, AtomicIndex),
    case <<AtomicValue:64/big-unsigned-integer>> of
        <<Prefix:SkipBits/bitstring, OldValue:FingerprintSize/big-unsigned-integer,
            Suffix/bitstring>> ->
            <<UpdatedAtomic:64/big-unsigned-integer>> =
                <<Prefix/bitstring, Value:FingerprintSize/big-unsigned-integer, Suffix/bitstring>>,
            case atomics:compare_exchange(Buckets, AtomicIndex, AtomicValue, UpdatedAtomic) of
                ok ->
                    case {OldValue, Value} of
                        {0, _} -> atomics:add(Buckets, 2, 1);
                        {_, 0} -> atomics:sub(Buckets, 2, 1);
                        {_, _} -> ok
                    end;
                _ ->
                    update_in_bucket(Filter, Index, SubIndex, OldValue, Value)
            end;
        _ ->
            {error, outdated}
    end.
