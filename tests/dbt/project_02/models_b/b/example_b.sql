select * from PROJECT_a.DATASET_b.TABLE_c where {{ var("key_a") }} = 0 and {{ var("key_b") }} = 0

-- if the table already exists and `--full-refresh` is
-- not set, then only add new records. otherwise, select
-- all records.