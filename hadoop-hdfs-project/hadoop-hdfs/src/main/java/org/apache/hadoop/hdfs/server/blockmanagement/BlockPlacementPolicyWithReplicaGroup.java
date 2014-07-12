1. DFSOutputStream specifies the current gourp ID of blocks been written. HDFS guarantees that replicas with the same group ID are placed on the same phy node.
2. DFSOutputStream writeChunk() API add a parameter replicaGroups[] to indicate the replicaGroup of each replica. This API also allows different blocks to have a different number of replicas
