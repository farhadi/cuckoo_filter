-record(cuckoo_filter, {
    buckets :: reference(),
    num_buckets :: non_neg_integer(),
    bucket_size :: non_neg_integer(),
    fingerprint_size :: non_neg_integer(),
    max_evictions :: non_neg_integer()
}).
