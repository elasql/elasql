# Configuration Instructions

ElaSQL provides some configurable settings. These settings can be found in properties file `elasql.properties` which is located in `src\main\resources\org\elasql`.

### Commonly Used Configurations

Here are some commonly used configurations:

ElaSQL has various types of distributed systems. You can select one of the systems by adjusting the following setting. Currently supported types are fully replicated distributed database (0), Calvin style partitioned DBMS (1) and T-Part style  partitioned DBMS (2).
```
org.elasql.server.Elasql.SERVICE_TYPE=1
```

To adjust the number of partitions in the system:

```
org.elasql.storage.metadata.PartitionMetaMgr.NUM_PARTITIONS=1
```

To adjust the number of requests in each batch of group communication:

```
org.elasql.remote.groupcomm.client.BatchSpcSender.BATCH_SIZE=1
```
