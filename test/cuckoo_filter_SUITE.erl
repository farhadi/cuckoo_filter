-module(cuckoo_filter_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-include_lib("cuckoo_filter.hrl").

all() ->
    [
        new_badargs,
        new,
        new_capacity,
        new_with_hash128,
        new_with_args,
        named_filter,
        add_contains_size,
        add_delete_contains_hash,
        adding_to_a_full_filter,
        adding_to_a_full_filter_by_force,
        delete,
        max_evictions,
        fingerprint_size,
        import_export,
        concurrent_add,
        concurrent_delete,
        concurrent_add_delete,
        concurrent_add_delete_with_0_evictions,
        concurrent_add_delete_forced,
        concurrent_add_same_item,
        lock_timeout
    ].

random_items(N) ->
    [rand:uniform() || _ <- lists:seq(1, N)].

new_badargs(_Config) ->
    ?assertError(badarg, cuckoo_filter:new(0)),
    ?assertError(badarg, cuckoo_filter:new(8, [{bucket_size, 0}])),
    ?assertError(badarg, cuckoo_filter:new(8, [{fingerprint_size, 5}])),
    ?assertError(badarg, cuckoo_filter:new(8, [{max_evictions, -1}])).

new(_Config) ->
    Capacity = rand:uniform(1000),
    Filter = cuckoo_filter:new(Capacity),
    RealCapacity = cuckoo_filter:capacity(Filter),
    NumBuckets = RealCapacity div 4,
    MaxHash = NumBuckets bsl 16 - 1,
    HashFunction = fun xxh3:hash64/1,
    ?assert(RealCapacity >= Capacity),
    ?assertMatch(
        #cuckoo_filter{
            bucket_size = 4,
            num_buckets = NumBuckets,
            max_hash = MaxHash,
            fingerprint_size = 16,
            max_evictions = 100,
            hash_function = HashFunction
        },
        Filter
    ).

new_capacity(_Config) ->
    lists:foreach(
        fun(Capacity) ->
            Filter = cuckoo_filter:new(Capacity),
            RealCapacity = cuckoo_filter:capacity(Filter),
            ?assert(RealCapacity >= Capacity)
        end,
        lists:seq(1, 10000)
    ).

new_with_hash128(_Config) ->
    Capacity = rand:uniform(1000),
    Filter = cuckoo_filter:new(Capacity, [{fingerprint_size, 64}]),
    RealCapacity = cuckoo_filter:capacity(Filter),
    NumBuckets = RealCapacity div 4,
    MaxHash = NumBuckets bsl 64 - 1,
    HashFunction = fun xxh3:hash128/1,
    ?assert(RealCapacity >= Capacity),
    ?assertMatch(
        #cuckoo_filter{
            bucket_size = 4,
            num_buckets = NumBuckets,
            max_hash = MaxHash,
            fingerprint_size = 64,
            max_evictions = 100,
            hash_function = HashFunction
        },
        Filter
    ).

new_with_args(_Config) ->
    Capacity = rand:uniform(1000),
    FingerprintSize = lists:nth(rand:uniform(4), [4, 8, 16, 32]),
    BucketSize = rand:uniform(16),
    MaxEvictions = rand:uniform(1000),
    HashFunction = fun erlang:phash2/1,
    Filter = cuckoo_filter:new(Capacity, [
        {bucket_size, BucketSize},
        {fingerprint_size, FingerprintSize},
        {max_evictions, MaxEvictions},
        {hash_function, HashFunction},
        {name, my_filter}
    ]),
    RealCapacity = cuckoo_filter:capacity(Filter),
    NumBuckets = RealCapacity div BucketSize,
    MaxHash = NumBuckets bsl FingerprintSize - 1,
    ?assertEqual(cuckoo_filter:whereis(my_filter), Filter),
    ?assert(RealCapacity >= Capacity),
    ?assertMatch(
        #cuckoo_filter{
            bucket_size = BucketSize,
            num_buckets = NumBuckets,
            max_hash = MaxHash,
            fingerprint_size = FingerprintSize,
            max_evictions = MaxEvictions,
            hash_function = HashFunction
        },
        Filter
    ).

named_filter(_Config) ->
    Name = named_filter,
    Capacity = rand:uniform(1000),
    Filter = cuckoo_filter:new(Capacity, [{name, Name}]),
    Element = an_element,
    ?assertEqual(cuckoo_filter:whereis(Name), Filter),
    ?assertEqual(ok, cuckoo_filter:add(Name, Element)),
    ?assertEqual(ok, cuckoo_filter:add(Name, Element, infinity)),
    ?assert(cuckoo_filter:contains(Name, Element)),
    ?assertEqual(ok, cuckoo_filter:delete(Name, Element)),
    ?assertEqual(ok, cuckoo_filter:delete(Name, Element, infinity)),
    Hash = cuckoo_filter:hash(Name, Element),
    ?assertEqual(ok, cuckoo_filter:add_hash(Name, Hash)),
    ?assertEqual(ok, cuckoo_filter:add_hash(Name, Hash, infinity)),
    ?assert(cuckoo_filter:contains_hash(Name, Hash)),
    ?assertEqual(ok, cuckoo_filter:delete_hash(Name, Hash)),
    ?assertEqual(ok, cuckoo_filter:delete_hash(Name, Hash, infinity)),
    ?assertEqual(0, cuckoo_filter:size(Name)),
    ?assert(cuckoo_filter:capacity(Name) >= Capacity),
    cuckoo_filter:new(Capacity, [{name, Name}, {max_evictions, 0}]),
    [cuckoo_filter:add(Name, Element, force) || _ <- lists:seq(1, 100)],
    {ok, {Index, Fingerprint}} = cuckoo_filter:add(Name, Element, force),
    ?assert(cuckoo_filter:contains_fingerprint(Name, Index, Fingerprint)),
    Data = cuckoo_filter:export(Name),
    ?assertEqual(ok, cuckoo_filter:import(Name, Data)).

add_contains_size(_Config) ->
    FingerprintSize = lists:nth(rand:uniform(4), [4, 8, 16, 32]),
    BucketSize = 1 + rand:uniform(10),
    MaxEvictions = 10 + rand:uniform(1000),
    Filter = cuckoo_filter:new(100 + rand:uniform(10000), [
        {bucket_size, BucketSize},
        {fingerprint_size, FingerprintSize},
        {max_evictions, MaxEvictions}
    ]),
    Capacity = cuckoo_filter:capacity(Filter),
    Items = random_items(Capacity div 4),
    ?assert(lists:all(fun(I) -> cuckoo_filter:add(Filter, I) == ok end, Items)),
    ?assert(lists:all(fun(I) -> cuckoo_filter:contains(Filter, I) end, Items)),
    ?assertEqual(cuckoo_filter:size(Filter), length(Items)).

add_delete_contains_hash(_Config) ->
    FingerprintSize = lists:nth(rand:uniform(4), [4, 8, 16, 32]),
    BucketSize = 1 + rand:uniform(10),
    MaxEvictions = 10 + rand:uniform(1000),
    Filter = cuckoo_filter:new(100 + rand:uniform(10000), [
        {bucket_size, BucketSize},
        {fingerprint_size, FingerprintSize},
        {max_evictions, MaxEvictions}
    ]),
    Capacity = cuckoo_filter:capacity(Filter),
    Items = random_items(Capacity div 4),
    ?assert(
        lists:all(
            fun(I) -> cuckoo_filter:add_hash(Filter, cuckoo_filter:hash(Filter, I)) == ok end,
            Items
        )
    ),
    ?assert(
        lists:all(
            fun(I) -> cuckoo_filter:contains_hash(Filter, cuckoo_filter:hash(Filter, I)) end,
            Items
        )
    ),
    ?assert(lists:all(fun(I) -> cuckoo_filter:contains(Filter, I) end, Items)),
    ?assert(
        lists:all(
            fun(I) -> cuckoo_filter:delete_hash(Filter, cuckoo_filter:hash(Filter, I)) == ok end,
            Items
        )
    ),
    ?assert(lists:all(fun(I) -> not cuckoo_filter:contains(Filter, I) end, Items)),
    ?assert(
        lists:all(
            fun(I) ->
                cuckoo_filter:delete_hash(Filter, cuckoo_filter:hash(Filter, I)) ==
                    {error, not_found}
            end,
            Items
        )
    ).

adding_to_a_full_filter(_Config) ->
    Filter = cuckoo_filter:new(rand:uniform(1000)),
    Capacity = cuckoo_filter:capacity(Filter),
    Items = random_items(Capacity + 1),
    ?assert(
        lists:any(fun(I) -> cuckoo_filter:add(Filter, I) == {error, not_enough_space} end, Items)
    ),
    ?assert(lists:any(fun(I) -> not cuckoo_filter:contains(Filter, I) end, Items)),
    ?assert(cuckoo_filter:size(Filter) < length(Items)).

adding_to_a_full_filter_by_force(_Config) ->
    Filter = cuckoo_filter:new(rand:uniform(1000), [{max_evictions, 0}]),
    Capacity = cuckoo_filter:capacity(Filter),
    Items = random_items(Capacity * 10),
    lists:foreach(fun(I) -> cuckoo_filter:add(Filter, I) end, Items),
    ?assertEqual(cuckoo_filter:size(Filter), Capacity),
    {ok, {Index, Fingerprint}} = cuckoo_filter:add(Filter, extra, force),
    ?assert(not cuckoo_filter:contains_fingerprint(Filter, Index, Fingerprint)),
    ?assert(cuckoo_filter:contains(Filter, extra)).

delete(_Config) ->
    Filter = cuckoo_filter:new(rand:uniform(1000)),
    Capacity = cuckoo_filter:capacity(Filter),
    Items = random_items(Capacity),
    Added = [I || I <- Items, cuckoo_filter:add(Filter, I) == ok],
    ?assert(lists:all(fun(I) -> cuckoo_filter:delete(Filter, I) == ok end, Added)),
    ?assert(lists:all(fun(I) -> not cuckoo_filter:contains(Filter, I) end, Items)),
    ?assert(lists:all(fun(I) -> cuckoo_filter:delete(Filter, I) == {error, not_found} end, Items)),
    ?assertEqual(cuckoo_filter:size(Filter), 0).

max_evictions(_Config) ->
    Filter1 = cuckoo_filter:new(1000 + rand:uniform(10000), [{max_evictions, 1000}]),
    Capacity = cuckoo_filter:capacity(Filter1),
    Filter2 = cuckoo_filter:new(Capacity, [{max_evictions, 10}]),
    Items = random_items(Capacity),
    lists:foreach(fun(I) -> cuckoo_filter:add(Filter1, I) end, Items),
    lists:foreach(fun(I) -> cuckoo_filter:add(Filter2, I) end, Items),
    ?assert(cuckoo_filter:size(Filter1) > cuckoo_filter:size(Filter2)).

fingerprint_size(_Config) ->
    Filter1 = cuckoo_filter:new(100 + rand:uniform(1000), [{fingerprint_size, 8}]),
    Capacity = cuckoo_filter:capacity(Filter1),
    Filter2 = cuckoo_filter:new(Capacity, [{fingerprint_size, 16}]),
    Items = random_items(Capacity),
    lists:foreach(fun(I) -> cuckoo_filter:add(Filter1, I) end, Items),
    lists:foreach(fun(I) -> cuckoo_filter:add(Filter2, I) end, Items),
    NonExistingItems = random_items(Capacity * 100),
    FalsePositives1 = length([I || I <- NonExistingItems, cuckoo_filter:contains(Filter1, I)]),
    FalsePositives2 = length([I || I <- NonExistingItems, cuckoo_filter:contains(Filter2, I)]),
    ?assert(FalsePositives1 > FalsePositives2).

import_export(_Config) ->
    Filter1 = cuckoo_filter:new(rand:uniform(1000)),
    Capacity = cuckoo_filter:capacity(Filter1),
    Filter2 = cuckoo_filter:new(Capacity),
    Items = random_items(Capacity div 2),
    lists:foreach(fun(I) -> cuckoo_filter:add(Filter1, I) end, Items),
    Exported = cuckoo_filter:export(Filter1),
    ?assertEqual(byte_size(Exported), Capacity * 2 + 8),
    cuckoo_filter:import(Filter2, Exported),
    ?assert(lists:all(fun(I) -> cuckoo_filter:contains(Filter2, I) end, Items)),
    Filter3 = cuckoo_filter:new(Capacity * 2),
    ?assertEqual({error, invalid_data_size}, cuckoo_filter:import(Filter3, Exported)).

concurrent_add(_Config) ->
    Filter = cuckoo_filter:new(100 + rand:uniform(1000)),
    Capacity = cuckoo_filter:capacity(Filter),
    ItemsGroup = [random_items(Capacity div 8) || _ <- lists:seq(1, 8)],
    Parent = self(),
    [
        spawn(fun() -> Parent ! [I || I <- Items, cuckoo_filter:add(Filter, I) == ok] end)
     || Items <- ItemsGroup
    ],
    [
        receive
            Added ->
                ?assert(lists:all(fun(I) -> cuckoo_filter:contains(Filter, I) end, Added))
        end
     || _ <- ItemsGroup
    ].

concurrent_delete(_Config) ->
    Filter = cuckoo_filter:new(100 + rand:uniform(1000)),
    Capacity = cuckoo_filter:capacity(Filter),
    Parent = self(),
    Items = random_items(Capacity),
    Added = [I || I <- Items, cuckoo_filter:add(Filter, I) == ok],
    {Added1, Added2} = lists:split(length(Added) div 2, Added),
    spawn(fun() -> Parent ! length([I || I <- Added1, cuckoo_filter:delete(Filter, I) == ok]) end),
    Deleted2 = length([I || I <- Added2, cuckoo_filter:delete(Filter, I) == ok]),
    receive
        Deleted1 ->
            ?assertEqual(Deleted1 + Deleted2, length(Added))
    end.

concurrent_add_delete(_Config) ->
    Capacity = 1024,
    Filter = cuckoo_filter:new(Capacity),
    Items = random_items(Capacity),
    ExtraItems = random_items(Capacity * 10),
    Added = lists:takewhile(fun(I) -> cuckoo_filter:add(Filter, I) == ok end, Items),
    {Pid, Ref} = spawn_monitor(fun F() ->
        receive
            exit ->
                ok;
            I ->
                cuckoo_filter:delete(Filter, I),
                F()
        end
    end),
    spawn(fun() ->
        [Pid ! I || I <- ExtraItems, cuckoo_filter:add(Filter, I) == ok],
        Pid ! exit
    end),
    ?assertEqual([], [
        I
     || I <- Added, _ <- lists:seq(1, 250), not cuckoo_filter:contains(Filter, I)
    ]),
    receive
        {'DOWN', Ref, process, Pid, normal} -> ok
    end,
    ?assertEqual(cuckoo_filter:size(Filter), length(Added)).

concurrent_add_delete_with_0_evictions(_Config) ->
    Capacity = 1024,
    Filter = cuckoo_filter:new(Capacity, [{max_evictions, 0}]),
    Items = random_items(Capacity),
    ExtraItems = random_items(Capacity * 10),
    Added = lists:takewhile(fun(I) -> cuckoo_filter:add(Filter, I) == ok end, Items),
    {Pid, Ref} = spawn_monitor(fun F() ->
        receive
            exit ->
                ok;
            I ->
                cuckoo_filter:delete(Filter, I),
                F()
        end
    end),
    spawn(fun() ->
        [Pid ! I || I <- ExtraItems, cuckoo_filter:add(Filter, I) == ok],
        Pid ! exit
    end),
    ?assertEqual([], [
        I
     || I <- Added, _ <- lists:seq(1, 250), not cuckoo_filter:contains(Filter, I)
    ]),
    receive
        {'DOWN', Ref, process, Pid, normal} -> ok
    end,
    ?assertEqual(cuckoo_filter:size(Filter), length(Added)).

concurrent_add_delete_forced(_Config) ->
    Capacity = 128,
    Filter = cuckoo_filter:new(Capacity, [{max_evictions, 0}, {fingerprint_size, 8}]),
    Items = random_items(Capacity * 100),
    Parent = self(),
    [
        spawn(fun() ->
            Parent ! length([I || I <- Items, cuckoo_filter:add(Filter, I, force) == ok])
        end)
     || _ <- lists:seq(1, 10)
    ],
    [
        spawn(fun() ->
            Parent ! -length([I || I <- Items, cuckoo_filter:delete(Filter, I) == ok])
        end)
     || _ <- lists:seq(1, 10)
    ],
    AddsAndRemoves = [
        receive
            N -> N
        end
     || _ <- lists:seq(1, 20)
    ],
    ?assertEqual(lists:sum(AddsAndRemoves), cuckoo_filter:size(Filter)).

concurrent_add_same_item(_Config) ->
    Filter = cuckoo_filter:new(100 + rand:uniform(1000)),
    Capacity = cuckoo_filter:capacity(Filter),
    Parent = self(),
    Items = lists:duplicate(Capacity * 10, 0),
    spawn(fun() -> Parent ! length([I || I <- Items, cuckoo_filter:add(Filter, I) == ok]) end),
    spawn(fun() -> Parent ! length([I || I <- Items, cuckoo_filter:add(Filter, I) == ok]) end),
    Deleted = length([I || I <- Items, cuckoo_filter:delete(Filter, I) == ok]),
    receive
        Added1 -> Added1
    end,
    receive
        Added2 -> Added2
    end,
    ?assertEqual(Deleted + cuckoo_filter:size(Filter), Added1 + Added2).

lock_timeout(_Config) ->
    Filter = cuckoo_filter:new(100 + rand:uniform(1000)),
    Capacity = cuckoo_filter:capacity(Filter),
    Items1 = random_items(Capacity),
    Items2 = random_items(Capacity),
    {Pid, Ref} = spawn_monitor(fun() -> [I || I <- Items1, cuckoo_filter:add(Filter, I) == ok] end),
    ?assert(lists:any(fun(I) -> cuckoo_filter:add(Filter, I, 0) == {error, timeout} end, Items2)),
    ?assert(
        lists:any(fun(I) -> cuckoo_filter:delete(Filter, I, 0) == {error, timeout} end, Items2)
    ),
    receive
        {'DOWN', Ref, process, Pid, normal} -> ok
    end.
