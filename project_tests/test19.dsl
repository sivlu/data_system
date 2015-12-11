-- Needs test8.dsl and test9.dsl to have been executed first.
-- This query uses tbl2
--
-- tbl2 has a secondary b-tree tree index on col1 and col2, and a clustered index on col7 with the form of a sorted column
--
-- this is a test for correctness on a complex query
--
-- Query in SQL:
-- SELECT max(col1+col5) FROM tbl2 WHERE col1 >= 21000000 and col1 < 510000000 and col7 >= 61000000 and col7 < 1010000000 and col6 >= 410000000 and col6 < 8100000000
--
-- The exact DSL of this query is left for you to design. You can decide (and experiment) with the order of the select operators, which indexes to use and/or test alternative operators.
s1=select(db1.tbl2.col1,21000000,510000000)
s2=select(db1.tbl2.col7,61000000,1010000000,s1)
s3=select(db1.tbl2.col6,410000000,8100000000,s2)
f1=fetch(db1.tbl2.col1,s3)
f2=fetch(db1.tbl2.col5,s3)
f3=add(f1,f2)
m1=max(f3)
tuple(m1)