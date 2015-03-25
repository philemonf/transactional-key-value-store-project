# Transactional key-value store for YARN
Development of a YARN application for transactional key-value operations.

## How to use it

For now it only run properly in local HDFS/YARN.

1. HDFS and YARN must be running
2. Start the YARN Application with `./start.sh`. The Client REPL will start.
3. Run example client application with `:test` (It should print "zero" in the end) 
4. Stop the YARN Application with `:kill` (Kills for now)
5. (WIP) Viewing logs to be configured.

You can look at the example client source code in `test/ch/epfl/tkvs/userclient/UserClient.java`
