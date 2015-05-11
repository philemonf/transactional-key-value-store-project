# Transactional key-value store for YARN
Development of a YARN application for transactional key-value operations.

## How to use it

If you are running the , `./configure.sh local` to configure the slaves file. Otherwise, if you are running it on the cluster of the course, please run `./configure.sh N`  to run the system on `N` nodes.


1. HDFS and YARN must be running
2. Start the YARN Application with `./start.sh`. The Client REPL will start.
3. Try `:benchmark`, it's gonna be fun.
4. Stop the YARN Application with `:exit`.
5. View all logs with `./printlogs.sh`.

You can look at the example client source code in `test/ch/epfl/tkvs/userclient/UserClient.java`



## Selecting the concurrency control
At the moment, the key-value store support 3 concurrency algorithms: 2PL, MVCC2PL and MVTO.
To select one of them, write `simple_2pl`, `mvcc2pl` or `mvto` in `./config/algorithm`.
By default (in absence of the file for instance), MVTO will be used.
