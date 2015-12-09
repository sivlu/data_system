-- Needs test25.dsl to have been executed first.
-- Testing for correctness - simple hashjoin between two tables. Columns other than those on which we join
-- are filtered upon. We aggregate the results. 
-- 
-- Query in SQL:
-- SELECT max(tbl4.col4 + tbl5.col5)
-- FROM tbl4, tbl5
-- WHERE tbl4.col1 = tbl5.col1
-- AND tbl4.col2 >= 10000 AND tbl4.col2 < 14000
-- AND tbl5.col3 >= 15000 AND tbl5.col3 < 62000;
s1=select(db1.tbl4.col2,10000,14000)
s2=select(db1.tbl5.col3,15000,62000)
f1=fetch(db1.tbl4.col1,s1)
f2=fetch(db1.tbl5.col1,s2)
r1,r2=hashjoin(f1,s1,f2,s2)
f1=fetch(db1.tbl4.col4,r1)
f2=fetch(db1.tbl5.col5,r2)
f3=add(f1,f2)
m1=max(f3)
tuple(m1)