CREATE TABLE x0(a CHECK( a||b ), b);
CREATE TABLE t0(a, b, CHECK( a|b ));
INSERT INTO x0 VALUES(1, 'x0');
INSERT INTO x0 VALUES(1, 'y0');
INSERT INTO t0 SELECT * FROM x0;