select * from PROJECT_t.DATASET_t.TABLE_t where {{ var("key_a") }} = 0 and {{ var("key_b") }} = 0

-- if the table already exists and `--full-refresh` is
-- not set, then only add new records. otherwise, select
-- all records.