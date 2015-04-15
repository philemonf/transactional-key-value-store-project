# Transactional key-value store for YARN
Development of a YARN application for transactional key-value operations.

## How to use it

For now it only run properly in local HDFS/YARN.

1. HDFS and YARN must be running
2. Start the YARN Application with `./start.sh`. The Client REPL will start.
3. Run example client application with `:test`.
4. Stop the YARN Application with `:exit`.
5. View all logs with `./printlogs.sh`.

You can look at the example client source code in `test/ch/epfl/tkvs/userclient/UserClient.java`
