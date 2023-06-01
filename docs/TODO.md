
# 1

Value expression and result of subquery must be of the same type for quantified comparison: date vs interval day to second

    --java.lang.AssertionError: SELECT t0.c0 FROM t0 WHERE (((DATE '2002-03-30')-(INTERVAL '12' MONTH)) <> SOME (VALUES INTERVAL '08' DAY, INTERVAL '17' HOUR, INTERVAL '09' DAY, INTERVAL '59' MINUTE)) UNION ALL SELECT t0.c0 FROM t0 WHERE (NOT (((DATE '2002-03-30')-(INTERVAL '12' MONTH)) <> SOME (VALUES INTERVAL '08' DAY, INTERVAL '17' HOUR, INTERVAL '09' DAY, INTERVAL '59' MINUTE))) UNION ALL SELECT t0.c0 FROM t0 WHERE (((((DATE '2002-03-30')-(INTERVAL '12' MONTH)) <> SOME (VALUES INTERVAL '08' DAY, INTERVAL '17' HOUR, INTERVAL '09' DAY, INTERVAL '59' MINUTE))) IS NULL)


# 2 

--java.lang.AssertionError: SELECT t1.c0 FROM t1, t0 WHERE (t1.c0 IN (t1.c0, JSON '{"val":null}', JSON '{"val":null}', t1.c0, JSON '{"val":true}')) UNION ALL SELECT t1.c0 FROM t1, t0 WHERE (NOT (t1.c0 IN (t1.c0, JSON '{"val":null}', JSON '{"val":null}', t1.c0, JSON '{"val":true}'))) UNION ALL SELECT t1.c0 FROM t1, t0 WHERE (((t1.c0 IN (t1.c0, JSON '{"val":null}', JSON '{"val":null}', t1.c0, JSON '{"val":true}'))) IS NULL)
--	at sqlancer.ComparatorHelper.getResultSetFirstColumnAsString(ComparatorHelper.java:73)
--	at sqlancer.ComparatorHelper.getCombinedResultSet(ComparatorHelper.java:140)
--	at sqlancer.presto.test.PrestoQueryPartitioningWhereTester.check(PrestoQueryPartitioningWhereTester.java:40)
--	at sqlancer.ProviderAdapter.generateAndTestDatabase(ProviderAdapter.java:61)
--	at sqlancer.Main$DBMSExecutor.run(Main.java:364)
--	at sqlancer.Main$2.run(Main.java:559)
--	at sqlancer.Main$2.runThread(Main.java:541)
--	at sqlancer.Main$2.run(Main.java:532)
--	at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128)
--	at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628)
--	at java.base/java.lang.Thread.run(Thread.java:829)
--Caused by: java.lang.IllegalArgumentException
--	at sqlancer.common.query.ExpectedErrors.errorIsExpected(ExpectedErrors.java:68)
--	at sqlancer.common.query.SQLQueryAdapter.checkException(SQLQueryAdapter.java:111)
--	at sqlancer.common.query.SQLQueryAdapter.executeAndGet(SQLQueryAdapter.java:150)
--	at sqlancer.ComparatorHelper.getResultSetFirstColumnAsString(ComparatorHelper.java:55)
--	... 10 more
---- Time: 2023/05/22 13:54:13
-- Database: database59
-- Database version: 0.280-a95c1b4
-- seed value: 1684752947332

    DROP SCHEMA IF EXISTS database59;
    CREATE SCHEMA IF NOT EXISTS database59;
    USE database59;
    CREATE TABLE memory.database59.t0(c0 VARCHAR(218));
    CREATE TABLE memory.database59.t1(c0 JSON);
    INSERT INTO t0(c0) VALUES ('0Z*q'), ('/Jl!Rl\C');
    INSERT INTO t1(c0) VALUES (JSON '{"val":false}'), (JSON '{"employees":["John", "Anna", "Peter"]}');
    INSERT INTO t0(c0) VALUES (''), ('W?t');

    SELECT t1.c0 FROM t1, t0 WHERE (t1.c0 IN (t1.c0, JSON '{"val":null}', JSON '{"val":null}', t1.c0, JSON '{"val":true}')) UNION ALL SELECT t1.c0 FROM t1, t0 WHERE (NOT (t1.c0 IN (t1.c0, JSON '{"val":null}', JSON '{"val":null}', t1.c0, JSON '{"val":true}'))) UNION ALL SELECT t1.c0 FROM t1, t0 WHERE (((t1.c0 IN (t1.c0, JSON '{"val":null}', JSON '{"val":null}', t1.c0, JSON '{"val":true}'))) IS NULL)

# 3

