public class TestString {
  public static final String SEPARATOR_CHAR = ":";

  private long getReplicaGroupID(String replicaGroupStr) {
    int startIndex = replicaGroupStr.lastIndexOf(SEPARATOR_CHAR);
    String strId = replicaGroupStr.substring(startIndex + 1);
    if (null == strId || strId.isEmpty()) {
       throw IllegalStateException("Shen Li: Illegal replica "
           + "group id in " + replicaGroupStr);
     }
     return Long.parseLong(strId);
  }

  public static void main(String args[]) {
    String str1 = "hdfs:///tmp/shen/abc" + SEPARATOR_CHAR +
  }

}
