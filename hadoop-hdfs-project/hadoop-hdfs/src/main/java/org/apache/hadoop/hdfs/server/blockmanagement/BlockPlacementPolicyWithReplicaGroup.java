package org.apache.hadoop.hdfs.server.blockmanagement;

import static org.apache.hadoop.util.Time.now;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.DFSUtil;
import org.apache.hadoop.hdfs.StorageType;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.namenode.FSClusterStats;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage.State;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;


/**
 *
 * 1. DFSOutputStream specifies the current gourp ID of blocks been 
 * written. HDFS guarantees that replicas with the same group ID 
 * are placed on the same phy node.
 *
 * 2. DFSOutputStream writeChunk() API add a parameter 
 * replicaGroups[] to indicate the replicaGroup of each replica. 
 * This API also allows different blocks to have a different number 
 * of replicas
 */
public class BlockPlacementPolicyWithReplicaGroup 
extends BlockPlacementPolicyDefault {

  // TODO: has to make this persistent
  // The mapping from replica group id to DataStorageGroup
  protected static TreeMap<String, DatanodeStorageInfo> rGroup2Dns; 

  @Override
  protected void initialize(Configuration conf, FSClusterStats stats,
                            NetworkTopolocy clusterMap) {
    super(conf, stats, clusterMap);
    synchronized (rGroup2Dns) {
      if (null == rGroup2Dns) {
        rGroup2Dns = new TreeMap<String, DatanodeStorageInfo> ();
      }
    }
  }

  @Override
  public DatanodeStorageInfo[] chooseTarget(String srcPath,
                                    int numOfReplicas,
                                    Node writer,
                                    List<DatanodeStorageInfo> chosen,
                                    boolean returnChosenNodes,
                                    Set<Node> excludedNodes,
                                    long blockSize,
                                    StorageType storageType){
    return null;
  }

  @Override
  public DatanodeStorageInfo[] chooseTarget(String srcPath,
                                    int numOfReplicas,
                                    Node write,
                                    Set<Node> excludeNodes,
                                    long blockSize,
                                    List<DatanodeDescriptor> favoredNodes,
                                    StorageType storageType) {
    return null;
  }

  @Override
  public BlockPlacementStatus verifyBlockPlacement(String srcPath,
                                                   LocatedBlock lBlk,
                                                   int numReplicas) {
    return null;
  }

  @Override
  public DatanodeDescriptor chooseReplicaToDelete(BlockCollection srcBC,
                      Block block,
                      short replicationFactor,
                      Collection<DatanoteDescriptor> existingReplicas,
                      Collection<DatanoteDescriptor> moreExistingReplicas) {
    throw new IllegalStateException("Shen Li: " 
        + "BlockPlacementPolicyWitReplicaGroup does not support "
        + "chooseReplicaToDelete yet!");
    return null;
  }

}
