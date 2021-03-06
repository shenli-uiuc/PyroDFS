import java.io.*;
import java.util.*;
import java.net.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;

public class TestSeal {
  public static void main(String args[]) {
    try {
      String input = args[0];

      Configuration conf = new Configuration();
      FileSystem hdfs = FileSystem.get(new URI("hdfs://localhost:9000"), conf);
      Path file = new Path("hdfs://localhost:9000/test/test_seal.txt");
      if (hdfs.exists(file)) {
        hdfs.delete(file, true);
      }

      FSDataOutputStream fdos = 
        hdfs.create(file);

      OutputStream os = fdos;

      BufferedWriter br = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));
      String prefix = "hbase:text/test_seal.txt";
      String[] replicaGroups = new String[1];
      String replicaNamespace = "test_region";
      replicaGroups[0] = "0";
      fdos.setReplicaGroups(replicaNamespace, replicaGroups);
      fdos.sealCurBlock();
      br.write("test test before seal! " + input + "\n");
      br.flush();
      fdos.sealCurBlock();
      //replicaGroups[0] = prefix + ":1";
      fdos.setReplicaGroups(replicaNamespace, replicaGroups);
      br.write("after seal");
      br.close();
      System.out.println("Replica group " + replicaGroups[0] + " is stored on "
                         + hdfs.getReplicaGroupLocation(replicaNamespace, 
                                                        replicaGroups[0]));
      System.out.println("Get replica group location: " 
          + hdfs.getReplicaGroupLocation(replicaNamespace, replicaGroups[0]));
      hdfs.close();
    } catch (Exception e) {
      System.out.println(e.getMessage());
      e.printStackTrace();
    }
  }
}
