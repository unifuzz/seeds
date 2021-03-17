CREATE TABLE t34(x,y);
INSERT INTO t34 VALUES(100,4), (107,3), (100,5),
(107,5);
SELECT a.x,avg(y)FROM t34 AS a
GROUP BY a.x
HAVING NOT EXISTS( SELECT A.x, avg(a.y)
FROM t34  );
SELECT a.x,avg(y)FROM t34 AS a
GROUP BY a.x
HAVING NOT EXISTS( SELECT A.x, avg(a.y)
FROM t34  b
GROUP BY x
HAVING avg(n.y) > avg(y));b
GROUP BY x
HAVING avg(n.y) > avg(y));
