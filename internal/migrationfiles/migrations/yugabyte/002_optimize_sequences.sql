-- Increase the sequence cache size for sort_id columns.
--
-- In YugabyteDB, every nextval() call for a SERIAL column requires a
-- distributed Raft round-trip to allocate the next value. CACHE N tells
-- each YB-TServer to pre-allocate N values locally, reducing that
-- coordination to once per N inserts instead of once per insert.
--
-- The owned-by relationship created by SERIAL means these sequences are
-- still dropped automatically when their parent table is dropped.

ALTER SEQUENCE promises_sort_id_seq  CACHE 100;
ALTER SEQUENCE schedules_sort_id_seq CACHE 100;
ALTER SEQUENCE tasks_sort_id_seq     CACHE 100;