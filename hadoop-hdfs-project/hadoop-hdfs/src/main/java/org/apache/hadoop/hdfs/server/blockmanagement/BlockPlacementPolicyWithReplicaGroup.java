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
  private static ReplicaGroupManager rgManager =
             new ReplicaGroupManager();
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
                                    String replicaNamespace,
                                    List<String> replicaGroups) {
    // numOfReplicas is set for the file, which has to agree with 
    // the number of replicaGroups
    LOG.info("Shen Li: in chooseTarget 3");
    String strGroups = "";
    if (null != replicaGroups) {
      for (String replicaGroup : replicaGroups)
        strGroups += (replicaGroup + ", ");
    }
    LOG.info("Shen Li: replicaNamespace " + replicaNamespace
             + ", replicaGroups " + strGroups);
    if (null != excludeNodes) {
      String details = "";
      for (Node node: excludeNodes) {
        details += (node.getName() + ", ");
      }
      LOG.info("Shen Li: in chooseTarget 3, excludeNodes is not null: "
          + details);
    }
    if (null != replicaGroups && replicaGroups.size() > 0) {
      if (replicaGroups.size() > numOfReplicas) {
        throw new IllegalStateException("Shen Li: file replica number "
            + numOfReplicas + " is smaller than the number of "
            + "replicaGroups " + replicaGroups.size());
      }
      try {
        return chooseTarget(srcPath,  numOfReplicas, writer, excludeNodes, 
                            blockSize, 
                            storageType, replicaNamespace, replicaGroups);
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
   * of elements in replicaGroups.
   *
   * Try to place replica group with non-negtive ids into different
   * servers. randomly place group with negtive ids
   */
  private DatanodeStorageInfo[] chooseTarget(String srcPath,
                                    int numOfReplicas, // TODO: handle
                                    Node writer,
                                    Set<Node> dfsExcludeNodes,
                                    long blockSize,
                                    StorageType storageType,
                                    String replicaNamespace,
                                    List<String> replicaGroups) 
      throws NotEnoughReplicasException {
    // the caller guarantees that the replica number of the file
    // agrees with the size of replicaGroups
    //
    // TODO: for now excludeNodes is guaranteed to be null.
    //       handle not-null case in the future
    int chosenReplicaNum = 0;
    int[] result = 
      getMaxNodesPerRack(0, replicaGroups.size());
    int maxNodesPerRack = result[1];

    boolean avoidStaleNodes = (stats != null
            && stats.isAvoidingStaleDataNodesForWrite());
    final List<DatanodeStorageInfo> results = 
      new ArrayList<DatanodeStorageInfo>();
    Set<Node> excludeNodes = null;
    Set<Node> randomExcludeNodes = new TreeSet<Node> ();
    if (null == dfsExcludeNodes) {
      dfsExcludeNodes = new TreeSet<Node> ();
    }
    randomExcludeNodes.addAll(dfsExcludeNodes);
    for (String replicaGroup : replicaGroups) {
      try {
        if (null == replicaGroup) {
          continue;
        }
        LOG.info("Shen Li: before rgManager.get()");
        DatanodeStorageInfo dnsi = rgManager.get(replicaNamespace,
                                                 replicaGroup);
        if (null == dnsi) {
          LOG.info("Shen Li: replicaGroup " + replicaGroup 
              + " returns null dnsi");
          // first time seen this replicaGroup, choose a DatanodeStorageInfo
          // for it.
          //
          // TODO: a replicaGroup should be a logical object which is 
          //       respected by the balancer
          int groupType = rgManager.checkGroupType(replicaGroup);
         
          if (rgManager.PRIMARY_GROUP == groupType) {
            // primary replication, store it on writer
            dnsi = 
              chooseLocalStorage(writer, dfsExcludeNodes, blockSize,
                                 maxNodesPerRack, results, avoidStaleNodes, 
                                 storageType);
            // local node already added into dfsExcludeNodes 
            randomExcludeNodes.add(dnsi.getDatanodeDescriptor());
          } else if (rgManager.EXCLUSIVE_GROUP == groupType){
            excludeNodes = rgManager.getExcludeNodes(replicaNamespace);
            LOG.info("Shen Li: replicaGroup get exlucdeNodes "
                + excludeNodes);
            if (null == excludeNodes) {
              excludeNodes = new TreeSet<Node> ();
            }
            excludeNodes.addAll(dfsExcludeNodes);
            // randomly choose a DatanodeStorageInfo, 
            // replica groups that are responsible for region server
            // split have to be mutual exclusive
            dnsi = chooseRandom(NodeBase.ROOT, excludeNodes, blockSize, 
                                maxNodesPerRack, results, avoidStaleNodes, 
                                storageType);
            randomExcludeNodes.add(dnsi.getDatanodeDescriptor());
          } else {
            // do not care about RANDOM_GROUP
            dnsi = chooseRandom(NodeBase.ROOT, randomExcludeNodes, blockSize,
                                maxNodesPerRack, results, avoidStaleNodes,
                                storageType);
          }
          if (null == dnsi) {
            LOG.warn("Shen Li: could not find a target for replica group " 
                     + replicaGroup + " of file " + srcPath);
            continue;
          }
          rgManager.addDnsiIfNecessary(replicaNamespace, replicaGroup, dnsi);
        } else {
          // existing mapping 
          results.add(dnsi);
        }

        ++chosenReplicaNum;
      } catch (IllegalStateException e) {
        LOG.error("Shen Li: error decoding replica group id from "
            + replicaGroup + ": " + e.getMessage());
        throw e;
      }
    }

    if (chosenReplicaNum < numOfReplicas) {
      for (int i = chosenReplicaNum; i < numOfReplicas; ++i) {
        results.add(chooseRandom(NodeBase.ROOT, randomExcludeNodes, blockSize,
                                 maxNodesPerRack, results, avoidStaleNodes,
                                 storageType));
      }
    }

    return getPipeline(writer,
        results.toArray(new DatanodeStorageInfo[results.size()]));
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

  @Override
  public String getReplicaGroupLocation(String namespace, String rgId) {
    LOG.info("Shen Li: recieve getReplicaGroupLocation call in "
             + "BlockPlacementPolicyWithReplicaGroup instance");
    return rgManager.getReplicaGroupLocation(namespace, rgId);
  }

}
