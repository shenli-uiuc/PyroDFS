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
  protected static TreeMap<String, DatanodeStorageInfo> rGroup2Dns =
               new TreeMap<String, DatanodeStorageInfo> ();

  protected static TreeMap<String, Set<Node> > excludeNodes = 
               new TreeMap<String, Set<Node> > ();
  public ReplicaGroupManager() {
    //TODO: load data from disk if necessary
  }

  public static DatanodeStorageInfo get(String groupStr) {
    synchronized (rGroup2Dns) {
      return rGroup2Dns.get(groupStr);
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

  public static Set<Node> getExcludeNodes(String groupStr) {
    String prefix = ReplicaGroupUtil.getPrefix(groupStr);
    synchronized (excludeNodes) {
      return excludeNodes.get(prefix);  
    }
  }

  public static 
  boolean addDnsiIfNecessary(String groupStr, 
                             DatanodeStorageInfo dnsi) {
    int groupType = checkGroupType(groupStr);
    synchronized (rGroup2Dns) {
      if ((PRIMARY_GROUP == groupType || EXCLUSIVE_GROUP == groupType)
          && null == rGroup2Dns.get(groupStr)) {
        rGroup2Dns.put(groupStr, dnsi);
        String prefix = ReplicaGroupUtil.getPrefix(groupStr);
        synchronized (excludeNodes) {
          if (null == excludeNodes.get(prefix)) {
            excludeNodes.put(prefix, new TreeSet<Node> ());
          }
          excludeNodes.get(prefix).add(dnsi.getDatanodeDescriptor());
        }
        return true;
      }
    }
    return false;
  }

}
