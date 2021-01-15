%%%-------------------------------------------------------------------
%% @doc High-performance, concurrent, and mutable Cuckoo Filter
%% implemented using atomics for Erlang and Elixir.
%% @end
%%%-------------------------------------------------------------------

-module(cuckoo_filter).

-export([
    new/1, new/2,
    add/2, add/3,
    contains/2,
    delete/2, delete/3,
    capacity/1,
    size/1,
    export/1,
    import/2
]).

-include("cuckoo_filter.hrl").

-type cuckoo_filter() :: #cuckoo_filter{}.

-export_type([cuckoo_filter/0]).

-type options() :: [option()].
-type option() ::
    {fingerprint_size, 4 | 8 | 16 | 32 | 64}
    | {bucket_size, pos_integer()}
    | {max_evictions, non_neg_integer()}
    | {hash_function, fun((binary()) -> non_neg_integer())}.

%% Default configurations
-define(DEFAULT_FINGERPRINT_SIZE, 16).
-define(DEFAULT_BUCKET_SIZE, 4).
-define(DEFAULT_EVICTIONS, 100).

%% @equiv new(Capacity, [])
-spec new(pos_integer()) -> cuckoo_filter().
new(Capacity) ->
    new(Capacity, []).

%% @doc Creates a new cuckoo filter with the given capacity and options
%%
%% Note that the actual capacity might be higher than the given capacity,
%% because internally number of buckets in a cuckoo filter must be a power of 2.
%%
%% Possible options are:
%% <ul>
%% <li>`{fingerprint_size, FingerprintSize}'
%% <p>FingerprintSize can be one of 4, 8, 16, 32, and 64 bits. Default fingerprint
%% size is 16 bits.</p>
%% </li>
%% <li>`{bucket_size, BucketSize}'</li>
%% <p>BucketSize must be a non negative integer, and the default value is 4.
%% Higher bucket sizes can reduce insert time considerably since it reduces the number
%% of relocations of existing fingerprints in occupied buckets, but it increases the
%% lookup time, and false positive rate.</p>
%% <li>`{max_evictions, MaxEvictions}'</li>
%% <p> MaxEvictions indicates the maximum number of relocation attemps before giving up
%% when inserting a new element.</p>
%% <li>`{hash_function, HashFunction}'</li>
%% <p> You can specify a custom hash function that accepts a binary as argument and returns
%% hash value as an integer.</p>
%% </ul>
-spec new(pos_integer(), options()) -> cuckoo_filter().
new(Capacity, Opts) ->
    is_integer(Capacity) andalso Capacity > 0 orelse error(badarg),
    BucketSize = proplists:get_value(bucket_size, Opts, ?DEFAULT_BUCKET_SIZE),
    is_integer(BucketSize) andalso BucketSize > 0 orelse error(badarg),
    FingerprintSize = proplists:get_value(fingerprint_size, Opts, ?DEFAULT_FINGERPRINT_SIZE),
    lists:member(FingerprintSize, [4, 8, 16, 32, 64]) orelse error(badarg),
    MaxEvictions = proplists:get_value(max_evictions, Opts, ?DEFAULT_EVICTIONS),
    is_integer(MaxEvictions) andalso MaxEvictions >= 0 orelse error(badarg),
    HashFunction = proplists:get_value(
        hash_function,
        Opts,
        default_hash_function(BucketSize + FingerprintSize)
    ),
    NumBuckets = 1 bsl ceil(math:log2(ceil(Capacity / BucketSize))),
    AtomicsSize = ceil(NumBuckets * BucketSize * FingerprintSize / 64) + 2,
    #cuckoo_filter{
        buckets = atomics:new(AtomicsSize, [{signed, false}]),
        num_buckets = NumBuckets,
        bucket_size = BucketSize,
        fingerprint_size = FingerprintSize,
        max_evictions = MaxEvictions,
        hash_function = HashFunction
    }.

%% @equiv add(Filter, Data, infinity)
-spec add(cuckoo_filter(), term()) -> ok | {error, not_enough_space}.
add(Filter, Data) ->
    add(Filter, Data, infinity).

%% @doc Adds data to the filter.
%%
%% Returns `ok' if the insertion was successful, but could return
%% `{error, not_enough_space}', when the filter is nearing its capacity.
%%
%% When `LockTimeout' is given, it could return `{error, timeout}', if it
%% can not acquire the lock within `LockTimeout' milliseconds.
-spec add(cuckoo_filter(), term(), timeout()) -> ok | {error, not_enough_space | timeout}.
add(
    Filter = #cuckoo_filter{
        fingerprint_size = FingerprintSize,
        num_buckets = NumBuckets,
        hash_function = HashFunction
    },
    Data,
    LockTimeout
) ->
    {Index, Fingerprint} = index_and_fingerprint(Data, FingerprintSize, NumBuckets, HashFunction),
    case insert_at_index(Filter, Index, Fingerprint) of
        ok ->
            ok;
        {error, full} ->
            AltIndex = alt_index(Index, Fingerprint, NumBuckets, HashFunction),
            case insert_at_index(Filter, AltIndex, Fingerprint) of
                ok ->
                    ok;
                {error, full} ->
                    RandIndex = element(rand:uniform(2), {Index, AltIndex}),
                    force_insert(Filter, RandIndex, Fingerprint, LockTimeout)
            end
    end.

%% @doc Checks if data is in the filter.
-spec contains(cuckoo_filter(), term()) -> boolean().
contains(
    Filter = #cuckoo_filter{
        fingerprint_size = FingerprintSize,
        num_buckets = NumBuckets,
        hash_function = HashFunction
    },
    Data
) ->
    {Index, Fingerprint} = index_and_fingerprint(Data, FingerprintSize, NumBuckets, HashFunction),
    lookup_index(Filter, Index, Fingerprint).

%% @equiv delete(Filter, Data, infinity)
-spec delete(cuckoo_filter(), term()) -> ok | {error, not_found}.
delete(Filter, Data) ->
    delete(Filter, Data, infinity).

%% @doc Deletes data from the filter.
%%
%% Returns `ok' if the deletion was successful, and returns {error, not_found}
%% if the element could not be found in the filter.
%%
%% When `LockTimeout' is given, it could return `{error, timeout}', if it
%% can not acquire the lock within `LockTimeout' milliseconds.
%%
%% <b>Note:</b> A cuckoo filter can only delete items that are known to be
%% inserted before. Deleting of non inserted items might lead to deletion of
%% another random element.
-spec delete(cuckoo_filter(), term(), timeout()) -> ok | {error, not_found | timeout}.
delete(
    Filter = #cuckoo_filter{
        fingerprint_size = FingerprintSize,
        num_buckets = NumBuckets,
        hash_function = HashFunction
    },
    Data,
    LockTimeout
) ->
    case write_lock(Filter, LockTimeout) of
        ok ->
            {Index, Fingerprint} = index_and_fingerprint(
                Data,
                FingerprintSize,
                NumBuckets,
                HashFunction
            ),
            case delete_fingerprint(Filter, Fingerprint, Index) of
                ok ->
                    release_write_lock(Filter);
                {error, not_found} ->
                    AltIndex = alt_index(Index, Fingerprint, NumBuckets, HashFunction),
                    Result = delete_fingerprint(Filter, Fingerprint, AltIndex),
                    release_write_lock(Filter),
                    Result
            end;
        {error, timeout} ->
            {error, timeout}
    end.

%% @doc Returns the maximum capacity of the filter.
-spec capacity(cuckoo_filter()) -> pos_integer().
capacity(#cuckoo_filter{bucket_size = BucketSize, num_buckets = NumBuckets}) ->
    NumBuckets * BucketSize.

%% @doc Returns number of items in the filter.
-spec size(cuckoo_filter()) -> non_neg_integer().
size(#cuckoo_filter{buckets = Buckets}) ->
    atomics:get(Buckets, 2).

%% @doc Returns all buckets in the filter as a binary.
%%
%% Returned binary can be used to reconstruct the filter again, using
%% {@link import/2} function.
-spec export(cuckoo_filter()) -> binary().
export(Filter = #cuckoo_filter{buckets = Buckets}) ->
    ok = write_lock(Filter, infinity),
    AtomicsSize = maps:get(size, atomics:info(Buckets)),
    Result = <<
        <<(atomics:get(Buckets, I)):64/big-unsigned-integer>>
        || I <- lists:seq(2, AtomicsSize)
    >>,
    release_write_lock(Filter),
    Result.

%% @doc Imports filter data from a binary created using {@link export/1}.
%%
%% Returns ok if the import was successful, but could return {ok, invalid_data_size}
%% if the size of the given binary does not match the size of the filter.
-spec import(cuckoo_filter(), binary()) -> ok | {error, invalid_data_size}.
import(Filter = #cuckoo_filter{buckets = Buckets}, Data) when is_binary(Data) ->
    ByteSize = (maps:get(size, atomics:info(Buckets)) - 1) * 8,
    case byte_size(Data) of
        ByteSize ->
            ok = write_lock(Filter, infinity),
            import(Buckets, Data, 2),
            release_write_lock(Filter);
        _ ->
            {error, invalid_data_size}
    end.

%%%-------------------------------------------------------------------
%% Internal functions
%%%-------------------------------------------------------------------

default_hash_function(Size) when Size > 64 ->
    fun xxh3:hash128/1;
default_hash_function(_Size) ->
    fun xxh3:hash64/1.

import(_Buckets, <<>>, _Index) ->
    ok;
import(Buckets, <<Atomic:64/big-unsigned-integer, Data/binary>>, Index) ->
    atomics:put(Buckets, Index, Atomic),
    import(Buckets, Data, Index + 1).

lookup_index(Filter, Index, Fingerprint) ->
    Bucket = read_bucket(Index, Filter),
    case lists:member(Fingerprint, Bucket) of
        true ->
            true;
        false ->
            lookup_alt_index(Filter, Index, Fingerprint, Bucket)
    end.

lookup_alt_index(
    Filter = #cuckoo_filter{num_buckets = NumBuckets, hash_function = HashFunction},
    Index,
    Fingerprint,
    Bucket
) ->
    AltIndex = alt_index(Index, Fingerprint, NumBuckets, HashFunction),
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

fingerprint(Hash, FingerprintSize) ->
    case Hash band (1 bsl FingerprintSize - 1) of
        0 ->
            1;
        Fingerprint ->
            Fingerprint
    end.

index_and_fingerprint(Data, FingerprintSize, NumBuckets, HashFunction) when is_binary(Data) ->
    Hash = HashFunction(Data),
    Fingerprint = fingerprint(Hash, FingerprintSize),
    Index = (Hash bsr FingerprintSize) rem NumBuckets,
    {Index, Fingerprint};
index_and_fingerprint(Data, FingerprintSize, NumBuckets, HashFunction) ->
    index_and_fingerprint(term_to_binary(Data), FingerprintSize, NumBuckets, HashFunction).

alt_index(Index, Fingerprint, NumBuckets, HashFunction) ->
    Index bxor HashFunction(binary:encode_unsigned(Fingerprint)) rem NumBuckets.

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
    {error, not_enough_space};
force_insert(
    #cuckoo_filter{max_evictions = MaxEvictions},
    _Index,
    _Fingerprint,
    Evictions,
    _EvictionsList,
    _Retry
) when map_size(Evictions) == MaxEvictions ->
    {error, not_enough_space};
force_insert(
    Filter = #cuckoo_filter{
        bucket_size = BucketSize,
        num_buckets = NumBuckets,
        hash_function = HashFunction
    },
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
            AltIndex = alt_index(Index, Evicted, NumBuckets, HashFunction),
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
                            );
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
    Filter = #cuckoo_filter{
        buckets = Buckets,
        bucket_size = BucketSize,
        fingerprint_size = FingerprintSize
    },
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
