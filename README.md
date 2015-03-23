# Transactional key-value store for YARN
Development of a YARN application for transactional key-value operations.

## How to use it

For now it only run properly in local HDFS/YARN.

1. HDFS and YARN must be running
2. If you run it for the first time, you must run `./configure.sh`
3. Start the YARN Application with `./start.sh`
4. Run example client application with `./start_client.sh` (It should print "zero" in the end) 
5. You can see the logs with `./printlogs.sh`
6. Stop the YARN Application with `./stop.sh` (Kill)

You can look at the example client source code in `src/ch/epfl/tkvs/userclient/UserClient.java`
