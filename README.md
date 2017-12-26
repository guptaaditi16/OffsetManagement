# OffsetManagement
OffsetManagement project denotes a Fault-Tolerant Data Processing System built with the help of zookeeper data node. 
Initially zookeeper node's value is set to 0.Records are read and processed in the chunks of size 10. 
Every time all 10 records in a chunk get processed, we store the current record's offset into the zookeeper node. 
This helps the application to resume the processing from the same chunk number, where the master node of spark application got failed/crashed. 
 
