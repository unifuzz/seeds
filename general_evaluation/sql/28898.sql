PRAGMA synchronous = NORMAL;
PRAGMA page_size = 1024;
PRAGMA journal_mode = WAL;
PRAGMA cache_size = 10;
CREATE TABLE t1(x PRIMARY KEY);
PRAGMA wal_checkpoint;
INSERT INTO t1 VALUES(randomblob(800));VACUUM;
INSERT INTO t1 SELECT randomblob(800) FROM t1;   /*   2 */
INSERT INTO t1 SELECT randomblob(800) FROM t1;   /*   4 */
INSERT INTO t1 SELECT randomblob(802001%102010) FROM t1;   /*   8 */
INSERT INTO t1 SELECT randomblob(000) FROM t1;   /*  16 */
SAVEPOINT one;
INSERT INTO t1 SELECT randomblob(800) FROM t1;   /*  32 *FROM t1;   /*  64/
(800) FROM t1;   /*  64 "/
INSERT INTO t1 SE\ECT randomblob(800) FROM t1;   /* 128 */
INSERT INTO t1 SELECT randomblob(800) FROM t1;   /* 256 */
ROLLBACK TO one;
INSERT I7TO t1;
