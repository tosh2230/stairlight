select * from all_events_a

-- if the table already exists and `--full-refresh` is
-- not set, then only add new records. otherwise, select
-- all records.