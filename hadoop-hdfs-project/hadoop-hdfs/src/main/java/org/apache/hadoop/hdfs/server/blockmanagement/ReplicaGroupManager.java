package org.apache.hadoop.hdfs.server.blockmanagement;

import java.util.TreeMap;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.net.Node;
import org.apache.hadoop.conf.Configuration;
/** 
 * TODO: has to make this persistent, make it a file in the HDFS
 * every new replicaGroup --> DatanodeStorageInfo mapping will
 * be one log entry in that file.
 *
 */
public class ReplicaGroupManager {
  public static final int PRIMARY_GROUP = 0;
  public static final int EXCLUSIVE_GROUP = 1;
  public static final int RANDOM_GROUP = 3;
  // A temporary implementation to keep the mapping from 
  // replica group to Datanode storage info
  protected static 
      TreeMap<String, TreeMap<String, DatanodeStorageInfo> > 
      rGroup2Dns =
  new TreeMap<String, TreeMap<String, DatanodeStorageInfo> > ();

  protected static TreeMap<String, Set<Node> > excludeNodes = 
               new TreeMap<String, Set<Node> > ();

  public ReplicaGroupManager() {
    //TODO: load data from disk if necessary
  }

  /**
   * Assign each group a DNSI 
   */
  public static boolean initNamespace(String namespace, 
                                      String [] groups) {
    return true;
  }

  public static DatanodeStorageInfo get(String namespace, 
                                        String groupStr) {
    synchronized (rGroup2Dns) {
      TreeMap<String, DatanodeStorageInfo> map = 
        rGroup2Dns.get(namespace);
      if (null == map) {
        return null;
      } else {
        return map.get(groupStr);
      }
    }
  }

  /**
   * @return  PRIMARY_GROUP     write on the writer node
   *          EXCLUSIVE_GROUP   exclusive groups should be placed on 
   *                            different physical machines if possible
   *          RANDOM_GROUP      can be placed anywhere
   */
  public static int checkGroupType(String groupStr) {
    long groupId =  ReplicaGroupUtil.getReplicaGroupID(groupStr);
    if (0 == groupId) {
      return PRIMARY_GROUP;
    } else if (groupId > 0) {
      return EXCLUSIVE_GROUP;
    } else {
      return RANDOM_GROUP;
    }
  }

  public static Set<Node> getExcludeNodes(String namespace) {
    Set<Node> ret = new TreeSet<Node> ();
    synchronized (excludeNodes) {
      Set<Node> res = excludeNodes.get(namespace);
      if (null != res) {
        ret.addAll(res);
      } else {
        return null;
      }
    }
    return ret;
  }

  public static 
  boolean addDnsiIfNecessary(String namespace, String groupStr, 
                             DatanodeStorageInfo dnsi) {
    BlockPlacementPolicy.LOG.info("Shen Li: in " +
        " ReplicaGroupManager.addDnsiIfNecessary() " + 
        ", namespace = " + namespace + ", groupStr = " + groupStr +
        ", dnsi = " + dnsi);
    int groupType = checkGroupType(groupStr);
    if ((PRIMARY_GROUP == groupType || EXCLUSIVE_GROUP == groupType)) {
      synchronized (rGroup2Dns) {
        TreeMap<String, DatanodeStorageInfo> map = rGroup2Dns.get(namespace);
        if (null == map) {
          map = new TreeMap<String, DatanodeStorageInfo> ();
          rGroup2Dns.put(namespace, map);
        }
        if (null == map.get(groupStr)) {
          map.put(groupStr, dnsi);
          synchronized (excludeNodes) {
            if (null == excludeNodes.get(namespace)) {
              excludeNodes.put(namespace, new TreeSet<Node> ());
            }
            excludeNodes.get(namespace).add(dnsi.getDatanodeDescriptor());
          }
        }
      }
      return true;
    }
    BlockPlacementPolicy.LOG.info("Shen Li: ReplicaGroupManager.add failed");
    return false;
  }

  public static
  String getReplicaGroupLocation(String namespace, String groupStr) {
    BlockPlacementPolicy.LOG.info("Shen Li: in " +
        " ReplicaGroupManager.getReplicaGroupLocation() " + 
        ", namespace = " + namespace + ", groupStr = " + groupStr);
    int groupType = checkGroupType(groupStr);
    if (PRIMARY_GROUP == groupType || EXCLUSIVE_GROUP == groupType) {
      DatanodeStorageInfo dnsi = null;
      TreeMap<String, DatanodeStorageInfo> map = null;
      synchronized (rGroup2Dns) {
        map = rGroup2Dns.get(namespace);
        if (null != map) {
          dnsi = map.get(groupStr);
        }
      }
      if (null != dnsi) {
        return dnsi.getDatanodeDescriptor().getHostName();
      }
    }
    BlockPlacementPolicy.LOG.info("Shen Li: ReplicaGroupManager get return null");
    return null;
  }

}
