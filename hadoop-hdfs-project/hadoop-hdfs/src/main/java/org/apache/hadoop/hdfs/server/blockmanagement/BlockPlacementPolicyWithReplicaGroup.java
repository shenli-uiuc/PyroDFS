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
import org.apache.hadoop.hdfs.DFSUtil;
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
 *
 * 3. replica group id format: pathAndFilenNme:internalReplicaGroupID
 *
 * 4. replica with internalReplicaGroupID == 0, has to be placed on
 * the writer locally. All other replicaGroups can be placed randomly.
 */
public class BlockPlacementPolicyWithReplicaGroup 
extends BlockPlacementPolicyDefault {
  // TODO: has to make this persistent, make it a file in the HDFS
  // every new replicaGroup --> DatanodeStorageInfo mapping will
  // be one log entry in that file.
  //
  // The mapping from replica group id to DataStorageGroup
  protected static TreeMap<String, DatanodeStorageInfo> rGroup2Dns = 
               new TreeMap<String, DatanodeStorageInfo> (); 

  @Override
  public void initialize(Configuration conf, FSClusterStats stats,
                         NetworkTopology clusterMap) {
    super.initialize(conf, stats, clusterMap);
    LOG.info("Shen Li: in initialize");
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
    LOG.info("Shen Li: in chooseTarget 1, fall back to default");
    return super.chooseTarget(srcPath, numOfReplicas, writer, chosen,
                              returnChosenNodes, excludedNodes,
                              blockSize, storageType);
  }

  @Override
  public DatanodeStorageInfo[] chooseTarget(String srcPath,
                                    int numOfReplicas,
                                    Node writer,
                                    Set<Node> excludeNodes,
                                    long blockSize,
                                    List<DatanodeDescriptor> favoredNodes,
                                    StorageType storageType) {
    LOG.info("Shen Li: in chooseTarget 2, fall back to default");
    return super.chooseTarget(srcPath, numOfReplicas, writer, excludeNodes,
                              blockSize, favoredNodes, storageType);
  }

  @Override
  public DatanodeStorageInfo[] chooseTarget(String srcPath,
                                    int numOfReplicas,
                                    Node writer,
                                    Set<Node> excludeNodes,
                                    long blockSize,
                                    List<DatanodeDescriptor> favoredNodes,
                                    StorageType storageType,
                                    List<String> replicaGroups) {
    LOG.info("Shen Li: in chooseTarget 3");
    if (null != replicaGroups && replicaGroups.size() > 0) {
      try {
        return chooseTarget(srcPath,  writer, excludeNodes, blockSize, 
                            storageType, replicaGroups);
      } catch (NotEnoughReplicasException ex) {
        LOG.info("Shen Li: in chooseTarget 3, not enough replica exception"
                 + ", fall back to default: " + ex.getMessage());
      }
    } else {
      LOG.info("Shen Li: in chooseTarget 3, null replicaGroups, "
               + "fall back to default");
    }
    
    return super.chooseTarget(srcPath, numOfReplicas, writer, excludeNodes,
                              blockSize, favoredNodes, storageType);
  }

  /**
   * Shen Li: implementation of replica group based block palcment,
   * the number of replicas is completely determined by the number
   * of elements in replicaGroups
   */
  private DatanodeStorageInfo[] chooseTarget(String srcPath,
                                    Node writer,
                                    Set<Node> excludeNodes,
                                    long blockSize,
                                    StorageType storageType,
                                    List<String> replicaGroups) 
      throws NotEnoughReplicasException {

    int chosenReplicaNum = 0;
    int[] result = 
      getMaxNodesPerRack(0, replicaGroups.size());
    int maxNodesPerRack = result[1];

    boolean avoidStaleNodes = (stats != null
            && stats.isAvoidingStaleDataNodesForWrite());
    final List<DatanodeStorageInfo> results = 
      new ArrayList<DatanodeStorageInfo>();
    for (String replicaGroup : replicaGroups) {
      try {
        if (null == replicaGroup) {
          continue;
        }
        DatanodeStorageInfo dnsi = rGroup2Dns.get(replicaGroup);
        if (null == dnsi) {
          // first time seen this replicaGroup, choose a DatanodeStorageInfo
          // for it.
          //
          // TODO: a replicaGroup should be a logical object which is 
          //       respected by the balancer
          long groupID = ReplicaGroupUtil.getReplicaGroupID(replicaGroup);
          
          if (0 == groupID) {
            // primary replication, store it on writer
            dnsi = 
              chooseLocalStorage(writer, excludeNodes, blockSize,
                                 maxNodesPerRack, results, avoidStaleNodes, 
                                 storageType);
          } else {
            // randomly choose a DatanodeStorageInfo, 
            // replica groups that are responsible for region server
            // split have to be mutual exclusive
            //
            // TODO 1. add excluded nodes for mutual-exclusive replica groups
            dnsi =chooseRandom(NodeBase.ROOT, excludeNodes, blockSize, 
                               maxNodesPerRack, results, avoidStaleNodes, 
                               storageType);
          }
          if (null == dnsi) {
            LOG.warn("Shen Li: could not find a target for replica group " 
                     + replicaGroup + " of file " + srcPath);
            continue;
          }
          addToExcludeIfNecessary(groupID, dnsi, excludeNodes);
          rGroup2Dns.put(replicaGroup, dnsi);
        } else {
          // got one existing replica group
          results.add(dnsi);
        }

        ++chosenReplicaNum;
      } catch (IllegalStateException e) {
        LOG.error("Shen Li: error decoding replica group id from "
            + replicaGroup + ": " + e.getMessage());
      }
    }

    if (chosenReplicaNum < replicaGroups.size()) {
      String details = "chosen: " + chosenReplicaNum
                       + ", required: " + replicaGroups.size() 
                       + ". Details: ";
      for (String replicaGroup : replicaGroups) {
        details += (replicaGroup + ", ");
      }
      throw new NotEnoughReplicasException(details);
    }

    return getPipeline(writer,
        results.toArray(new DatanodeStorageInfo[results.size()]));
  }

  /**
   * Shen Li: TODO implement
   */
  private void addToExcludeIfNecessary(long groupID, 
                 DatanodeStorageInfo dnsi, Set<Node> excludeNodes) {
    // TODO: move this to ReplicaGroupUtils.java
  }

  @Override
  public BlockPlacementStatus verifyBlockPlacement(String srcPath,
                                                   LocatedBlock lBlk,
                                                   int numReplicas) {
    LOG.info("Shen Li: in verifyBlockPlacement");
    return super.verifyBlockPlacement(srcPath, lBlk, numReplicas);
  }

  @Override
  public DatanodeDescriptor chooseReplicaToDelete(BlockCollection srcBC,
                      Block block,
                      short replicationFactor,
                      Collection<DatanodeDescriptor> existingReplicas,
                      Collection<DatanodeDescriptor> moreExistingReplicas) {
    /*
    throw new IllegalStateException("Shen Li: " 
        + "BlockPlacementPolicyWitReplicaGroup does not support "
        + "chooseReplicaToDelete yet!");
        */
    LOG.info("Shen Li: in chooseReplicaToDelete");
    return super.chooseReplicaToDelete(srcBC, block, replicationFactor,
                                       existingReplicas, moreExistingReplicas);
  }

}
