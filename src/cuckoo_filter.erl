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
    hash/2,
    add_hash/2, add_hash/3,
    contains_hash/2,
    contains_fingerprint/3,
    delete_hash/2, delete_hash/3,
    capacity/1,
    size/1,
    whereis/1,
    export/1,
    import/2
]).

-include("cuckoo_filter.hrl").

-type cuckoo_filter() :: #cuckoo_filter{}.
-type hash() :: non_neg_integer().

-export_type([cuckoo_filter/0, hash/0]).

-type filter_name() :: term().
-type fingerprint() :: pos_integer().
-type index() :: non_neg_integer().
-type options() :: [option()].
-type option() ::
    {name, filter_name()}
    | {fingerprint_size, 4 | 8 | 16 | 32 | 64}
    | {bucket_size, pos_integer()}
    | {max_evictions, non_neg_integer()}
    | {hash_function, fun((binary()) -> hash())}.

%% Default configurations
-define(DEFAULT_FINGERPRINT_SIZE, 16).
-define(DEFAULT_BUCKET_SIZE, 4).
-define(DEFAULT_EVICTIONS, 100).

-define(FILTER(FilterName), persistent_term:get({?MODULE, FilterName})).

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
%% <li>`{name, Name}'
%% <p>If you give it a name, created filter instance will be stored in
%% persistent_term, and later you can access the filter by its name.</p>
%% </li>
%% <li>`{fingerprint_size, FingerprintSize}'
%% <p>FingerprintSize can be one of 4, 8, 16, 32, and 64 bits. Default fingerprint
%% size is 16 bits.</p>
%% </li>
%% <li>`{bucket_size, BucketSize}'
%% <p>BucketSize must be a non negative integer, and the default value is 4.
%% Higher bucket sizes can reduce insert time considerably since it reduces the number
%% of relocations of existing fingerprints in occupied buckets, but it increases the
%% lookup time, and false positive rate.</p>
%% </li>
%% <li>`{max_evictions, MaxEvictions}'
%% <p> MaxEvictions indicates the maximum number of relocation attemps before giving up
%% when inserting a new element.</p>
%% </li>
%% <li>`{hash_function, HashFunction}'
%% <p> You can specify a custom hash function that accepts a binary as argument
%% and returns hash value as an integer. By default xxh3 hash functions are used,
%% and you need to manually add `xxh3' to the list of your project dependencies.</p>
%% </li>
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
    MaxHash = NumBuckets bsl FingerprintSize - 1,
    AtomicsSize = ceil(NumBuckets * BucketSize * FingerprintSize / 64) + 2,
    Filter = #cuckoo_filter{
        buckets = atomics:new(AtomicsSize, [{signed, false}]),
        num_buckets = NumBuckets,
        max_hash = MaxHash,
        bucket_size = BucketSize,
        fingerprint_size = FingerprintSize,
        max_evictions = MaxEvictions,
        hash_function = HashFunction
    },
    case proplists:get_value(name, Opts) of
        undefined ->
            Filter;
        FilterName ->
            persistent_term:put({?MODULE, FilterName}, Filter),
            Filter
    end.

%% @equiv add(Filter, Element, infinity)
-spec add(cuckoo_filter() | filter_name(), term()) -> ok | {error, not_enough_space}.
add(Filter = #cuckoo_filter{}, Element) ->
    add_hash(Filter, hash(Filter, Element), infinity);
add(FilterName, Element) ->
    add(?FILTER(FilterName), Element).

%% @equiv add_hash(Filter, Element, infinity)
-spec add_hash(cuckoo_filter() | filter_name(), hash()) -> ok | {error, not_enough_space}.
add_hash(Filter = #cuckoo_filter{}, Hash) ->
    add_hash(Filter, Hash, infinity);
add_hash(FilterName, Hash) ->
    add_hash(?FILTER(FilterName), Hash, infinity).

%% @doc Adds an element to a filter.
%%
%% Returns `ok' if the insertion was successful, but could return
%% `{error, not_enough_space}', when the filter is nearing its capacity.
%%
%% When `LockTimeout' is given, it could return `{error, timeout}', if it
%% can not acquire the lock within `LockTimeout' milliseconds.
%%
%% If `force' is given as the 3rd argument, and there is no room for the element
%% to be inserted, another random element is removed, and the removed element
%% is returned as `{ok, {Index, Fingerprint}}'. In this case, elements are not
%% relocated, and no lock is acquired.
%%
%% Forced insertion can only be used with `max_evictions' set to 0.
-spec add
    (cuckoo_filter() | filter_name(), term(), timeout()) ->
        ok | {error, not_enough_space | timeout};
    (cuckoo_filter() | filter_name(), term(), force) ->
        ok | {ok, Evicted :: {index(), fingerprint()}}.
add(Filter = #cuckoo_filter{}, Element, LockTimeout) ->
    add_hash(Filter, hash(Filter, Element), LockTimeout);
add(FilterName, Element, LockTimeout) ->
    add(?FILTER(FilterName), Element, LockTimeout).

%% @doc Adds an element to a filter by its hash.
%%
%% Same as {@link add/3} except that it accepts the hash of the element instead
%% of the element.
-spec add_hash
    (cuckoo_filter() | filter_name(), hash(), timeout()) ->
        ok | {error, not_enough_space | timeout};
    (cuckoo_filter() | filter_name(), hash(), force) ->
        ok | {ok, Evicted :: {index(), fingerprint()}}.
add_hash(
    Filter = #cuckoo_filter{
        fingerprint_size = FingerprintSize,
        num_buckets = NumBuckets,
        hash_function = HashFunction
    },
    Hash,
    LockTimeout
) ->
    {Index, Fingerprint} = index_and_fingerprint(Hash, FingerprintSize),
    case insert_at_index(Filter, Index, Fingerprint) of
        ok ->
            ok;
        {error, full} ->
            AltIndex = alt_index(Index, Fingerprint, NumBuckets, HashFunction),
            case insert_at_index(Filter, AltIndex, Fingerprint) of
                ok ->
                    ok;
                {error, full} ->
                    RState = rand:mwc59_seed(),
                    Rand = rand:mwc59_value32(RState) bsr 31 + 1,
                    RandIndex = element(Rand, {Index, AltIndex}),
                    try_insert(Filter, RandIndex, Fingerprint, RState, LockTimeout)
            end
    end;
add_hash(FilterName, Hash, LockTimeout) ->
    add_hash(?FILTER(FilterName), Hash, LockTimeout).

%% @doc Checks if an element is in a filter.
-spec contains(cuckoo_filter() | filter_name(), term()) -> boolean().
contains(Filter = #cuckoo_filter{}, Element) ->
    contains_hash(Filter, hash(Filter, Element));
contains(FilterName, Element) ->
    contains(?FILTER(FilterName), Element).

%% @doc Checks if an element is in a filter by its hash.
-spec contains_hash(cuckoo_filter() | filter_name(), hash()) -> boolean().
contains_hash(Filter = #cuckoo_filter{fingerprint_size = FingerprintSize}, Hash) ->
    {Index, Fingerprint} = index_and_fingerprint(Hash, FingerprintSize),
    contains_fingerprint(Filter, Index, Fingerprint);
contains_hash(FilterName, Hash) ->
    contains_hash(?FILTER(FilterName), Hash).

%% @doc Checks whether a filter contains a fingerprint at the given index or its alternative index.
-spec contains_fingerprint(cuckoo_filter() | filter_name(), index(), fingerprint()) -> boolean().
contains_fingerprint(Filter = #cuckoo_filter{max_evictions = 0}, Index, Fingerprint) ->
    contains_fingerprint(Filter, Index, undefined, Fingerprint, 1);
contains_fingerprint(Filter = #cuckoo_filter{}, Index, Fingerprint) ->
    contains_fingerprint(Filter, Index, undefined, Fingerprint, 2);
contains_fingerprint(FilterName, Index, Fingerprint) ->
    contains_fingerprint(?FILTER(FilterName), Index, Fingerprint).

%% @equiv delete(Filter, Element, infinity)
-spec delete(cuckoo_filter() | filter_name(), term()) -> ok | {error, not_found}.
delete(Filter = #cuckoo_filter{}, Element) ->
    delete_hash(Filter, hash(Filter, Element), infinity);
delete(FilterName, Element) ->
    delete(?FILTER(FilterName), Element).

%% @equiv delete_hash(Filter, Element, infinity)
-spec delete_hash(cuckoo_filter() | filter_name(), hash()) -> ok | {error, not_found}.
delete_hash(Filter = #cuckoo_filter{}, Hash) ->
    delete_hash(Filter, Hash, infinity);
delete_hash(FilterName, Hash) ->
    delete_hash(?FILTER(FilterName), Hash, infinity).

%% @doc Deletes an element from a filter.
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
-spec delete(cuckoo_filter() | filter_name(), term(), timeout()) ->
    ok | {error, not_found | timeout}.
delete(Filter = #cuckoo_filter{}, Element, LockTimeout) ->
    delete_hash(Filter, hash(Filter, Element), LockTimeout);
delete(FilterName, Element, LockTimeout) ->
    delete(?FILTER(FilterName), Element, LockTimeout).

%% @doc Deletes an element from a filter by its hash.
%%
%% Same as {@link delete/3} except that it uses the hash of the element instead
%% of the element.
-spec delete_hash(cuckoo_filter() | filter_name(), hash(), timeout()) ->
    ok | {error, not_found | timeout}.
delete_hash(
    Filter = #cuckoo_filter{
        fingerprint_size = FingerprintSize,
        num_buckets = NumBuckets,
        hash_function = HashFunction
    },
    Hash,
    LockTimeout
) ->
    {Index, Fingerprint} = index_and_fingerprint(Hash, FingerprintSize),
    case write_lock(Filter, LockTimeout) of
        ok ->
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
    end;
delete_hash(FilterName, Hash, LockTimeout) ->
    delete_hash(?FILTER(FilterName), Hash, LockTimeout).

%% @doc Returns the hash value of an element using the hash function of the filter.
-spec hash(cuckoo_filter() | filter_name(), term()) -> hash().
hash(
    #cuckoo_filter{
        max_hash = MaxHash,
        hash_function = HashFunction
    },
    Element
) ->
    HashFunction(Element) band MaxHash;
hash(FilterName, Element) ->
    hash(?FILTER(FilterName), Element).

%% @doc Returns the maximum capacity of a filter.
-spec capacity(cuckoo_filter() | filter_name()) -> pos_integer().
capacity(#cuckoo_filter{bucket_size = BucketSize, num_buckets = NumBuckets}) ->
    NumBuckets * BucketSize;
capacity(FilterName) ->
    capacity(?FILTER(FilterName)).

%% @doc Returns number of items in a filter.
-spec size(cuckoo_filter() | filter_name()) -> non_neg_integer().
size(#cuckoo_filter{buckets = Buckets}) ->
    atomics:get(Buckets, 2);
size(FilterName) ->
    ?MODULE:size(?FILTER(FilterName)).

%% @doc Retrieves a cuckoo_filter from persistent_term by its name.
-spec whereis(filter_name()) -> cuckoo_filter().
whereis(FilterName) ->
    ?FILTER(FilterName).

%% @doc Exports a filter as a binary.
%%
%% Returned binary can be used to reconstruct the filter again, using
%% {@link import/2} function.
-spec export(cuckoo_filter() | filter_name()) -> binary().
export(Filter = #cuckoo_filter{buckets = Buckets}) ->
    ok = write_lock(Filter, infinity),
    AtomicsSize = maps:get(size, atomics:info(Buckets)),
    Result = <<
        <<(atomics:get(Buckets, I)):64/big-unsigned-integer>>
     || I <- lists:seq(2, AtomicsSize)
    >>,
    release_write_lock(Filter),
    Result;
export(FilterName) ->
    export(?FILTER(FilterName)).

%% @doc Imports filter data from a binary created using {@link export/1}.
%%
%% Returns ok if the import was successful, but could return {ok, invalid_data_size}
%% if the size of the given binary does not match the size of the filter.
-spec import(cuckoo_filter() | filter_name(), binary()) -> ok | {error, invalid_data_size}.
import(Filter = #cuckoo_filter{buckets = Buckets}, Data) when is_binary(Data) ->
    ByteSize = (maps:get(size, atomics:info(Buckets)) - 1) * 8,
    case byte_size(Data) of
        ByteSize ->
            ok = write_lock(Filter, infinity),
            import(Buckets, Data, 2),
            release_write_lock(Filter);
        _ ->
            {error, invalid_data_size}
    end;
import(FilterName, Data) ->
    import(?FILTER(FilterName), Data).

%%%-------------------------------------------------------------------
%% Internal functions
%%%-------------------------------------------------------------------

default_hash_function(Size) when Size > 64 ->
    fun(Element) -> xxh3:hash128(term_to_binary(Element)) end;
default_hash_function(Size) when Size > 32 ->
    fun(Element) -> xxh3:hash64(term_to_binary(Element)) end;
default_hash_function(Size) when Size > 27 ->
    fun(Element) -> erlang:phash2(Element, 4294967296) end;
default_hash_function(_Size) ->
    fun erlang:phash2/1.

import(_Buckets, <<>>, _Index) ->
    ok;
import(Buckets, <<Atomic:64/big-unsigned-integer, Data/binary>>, Index) ->
    atomics:put(Buckets, Index, Atomic),
    import(Buckets, Data, Index + 1).

contains_fingerprint(
    Filter = #cuckoo_filter{num_buckets = NumBuckets, hash_function = HashFunction},
    undefined,
    Index,
    Fingerprint,
    Retry
) ->
    AltIndex = alt_index(Index, Fingerprint, NumBuckets, HashFunction),
    contains_fingerprint(Filter, AltIndex, Index, Fingerprint, Retry);
contains_fingerprint(Filter, Index, _AltIndex, Fingerprint, 0) ->
    Bucket = read_bucket(Index, Filter),
    lists:member(Fingerprint, Bucket);
contains_fingerprint(Filter, Index, AltIndex, Fingerprint, Retry) ->
    Bucket = read_bucket(Index, Filter),
    case lists:member(Fingerprint, Bucket) of
        true ->
            true;
        false ->
            contains_fingerprint(Filter, AltIndex, Index, Fingerprint, Retry - 1)
    end.

delete_fingerprint(Filter, Fingerprint, Index) ->
    Bucket = read_bucket(Index, Filter),
    case find_in_bucket(Bucket, Fingerprint) of
        {ok, SubIndex} ->
            case update_in_bucket(Filter, Index, SubIndex, Fingerprint, 0) of
                ok -> ok;
                {error, outdated} -> delete_fingerprint(Filter, Fingerprint, Index)
            end;
        {error, not_found} ->
            {error, not_found}
    end.

index_and_fingerprint(Hash, FingerprintSize) ->
    Fingerprint = Hash rem (1 bsl FingerprintSize - 1) + 1,
    Index = Hash bsr FingerprintSize,
    {Index, Fingerprint}.

alt_index(Index, Fingerprint, NumBuckets, HashFunction) ->
    Index bxor HashFunction(Fingerprint) rem NumBuckets.

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

write_lock(#cuckoo_filter{max_evictions = 0}, _) ->
    ok;
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

release_write_lock(#cuckoo_filter{max_evictions = 0}) ->
    ok;
release_write_lock(#cuckoo_filter{buckets = Buckets}) ->
    atomics:put(Buckets, 1, 0).

try_insert(Filter = #cuckoo_filter{bucket_size = BucketSize}, Index, Fingerprint, RState, force) ->
    Filter#cuckoo_filter.max_evictions == 0 orelse error(badarg),
    Bucket = read_bucket(Index, Filter),
    UpdatedRState = rand:mwc59(RState),
    SubIndex = (rand:mwc59_value32(UpdatedRState) * BucketSize) bsr 32,
    case lists:nth(SubIndex + 1, Bucket) of
        0 ->
            case update_in_bucket(Filter, Index, SubIndex, 0, Fingerprint) of
                ok -> ok;
                {error, outdated} -> try_insert(Filter, Index, Fingerprint, UpdatedRState, force)
            end;
        Fingerprint ->
            {ok, {Index, Fingerprint}};
        Evicted ->
            case update_in_bucket(Filter, Index, SubIndex, Evicted, Fingerprint) of
                ok -> {ok, {Index, Evicted}};
                {error, outdated} -> try_insert(Filter, Index, Fingerprint, UpdatedRState, force)
            end
    end;
try_insert(Filter, Index, Fingerprint, RState, LockTimeout) ->
    case write_lock(Filter, LockTimeout) of
        ok ->
            Result =
                try_insert(
                    Filter,
                    Index,
                    Fingerprint,
                    RState,
                    #{},
                    [],
                    Filter#cuckoo_filter.bucket_size
                ),
            release_write_lock(Filter),
            Result;
        {error, timeout} ->
            {error, timeout}
    end.

try_insert(_Filter, _Index, _Fingerprint, _RState, _Evictions, _EvictionsList, 0) ->
    {error, not_enough_space};
try_insert(
    #cuckoo_filter{max_evictions = MaxEvictions},
    _Index,
    _Fingerprint,
    _RState,
    Evictions,
    _EvictionsList,
    _Retry
) when map_size(Evictions) > MaxEvictions ->
    {error, not_enough_space};
try_insert(
    Filter = #cuckoo_filter{
        bucket_size = BucketSize,
        num_buckets = NumBuckets,
        hash_function = HashFunction
    },
    Index,
    Fingerprint,
    RState,
    Evictions,
    EvictionsList,
    Retry
) ->
    Bucket = read_bucket(Index, Filter),
    case find_in_bucket(Bucket, 0) of
        {ok, SubIndex} ->
            case update_in_bucket(Filter, Index, SubIndex, 0, Fingerprint) of
                ok ->
                    persist_evictions(Filter, Evictions, EvictionsList, Fingerprint);
                {error, outdated} ->
                    try_insert(Filter, Index, Fingerprint, RState, Evictions, EvictionsList, Retry)
            end;
        {error, not_found} ->
            UpdatedRState = rand:mwc59(RState),
            SubIndex = (rand:mwc59_value32(UpdatedRState) * BucketSize) bsr 32,
            Evicted = lists:nth(SubIndex + 1, Bucket),
            Key = {Index, SubIndex},
            if
                Fingerprint == Evicted orelse is_map_key(Key, Evictions) ->
                    try_insert(
                        Filter,
                        Index,
                        Fingerprint,
                        UpdatedRState,
                        Evictions,
                        EvictionsList,
                        Retry - 1
                    );
                true ->
                    AltIndex = alt_index(Index, Evicted, NumBuckets, HashFunction),
                    try_insert(
                        Filter,
                        AltIndex,
                        Evicted,
                        UpdatedRState,
                        Evictions#{Key => Fingerprint},
                        [Key | EvictionsList],
                        BucketSize
                    )
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
