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

Note: For the first run, `./start.sh local` to configure the slaves file.

## Selecting the concurrency control
At the moment, the key-value store support 3 concurrency algorithm: 2PL, MVCC2PL and MVTO.
To select one of them, write `simple_2pl`, `mvcc2pl` or `mvto` in `./config/algorithm`.
By default (in absence of the file for instance), MVTO will be used.
