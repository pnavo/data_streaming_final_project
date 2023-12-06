-- ksql_scripts.sql --


/*ksql queries to create tables from stream for downstream analytics*/


-- CREATE TIP_PASSENGERS TABLE --
CREATE TABLE PASSENGERS_TIPS AS 
SELECT PASSENGER_COUNT, AVG(TIP_AMOUNT) "avg_tips"
FROM  TAXI_STREAMING_PROJECT 
GROUP BY PASSENGER_COUNT
EMIT CHANGES;


-- CREATE VENDOR_TIPS TABLE --
CREATE TABLE VENDOR_TIPS AS 
SELECT VENDORID, COUNT(*) "trips", AVG(TIP_AMOUNT) "avg_tips", SUM(TIP_AMOUNT) "total_tips"
FROM  TAXI_STREAMING_PROJECT 
GROUP BY VENDORID
EMIT CHANGES;


-- VENDOR_TIPS WITH TIMESTAMP --
SELECT *, FROM_UNIXTIME(UNIX_TIMESTAMP()) "update_time" from VENDOR_TIPS EMIT CHANGES;


-- CREATE AIRPORT_TIPS TABLE --
CREATE TABLE AIRPORT_TIPS AS 
SELECT AIRPORT_FEE, COUNT(*) "trips", AVG(TIP_AMOUNT) "avg_tips", SUM(TIP_AMOUNT) "total_tips"
FROM  TAXI_STREAMING_PROJECT 
GROUP BY AIRPORT_FEE
EMIT CHANGES;

SELECT * FROM AIRPORT_TIPS EMIT CHANGES;