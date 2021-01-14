-record(cuckoo_filter, {
    buckets :: atomics:atomics_ref(),
    num_buckets :: pos_integer(),
    bucket_size :: pos_integer(),
    fingerprint_size :: 4 | 8 | 16 | 32 | 64,
    max_evictions :: non_neg_integer(),
    hash_function :: fun((binary()) -> non_neg_integer())
}).
