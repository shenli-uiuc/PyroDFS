import java.io.*;
import java.util.*;
import java.net.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;

public class TestSplit {
  public static void main(String args[]) {
    try {
      String input = args[0];
      String filePrefix = "hdfs://localhost:9000/test/";
      Configuration conf = new Configuration();
      FileSystem hdfs = FileSystem.get(new URI("hdfs://localhost:9000"), conf);
      Path file = new Path(filePrefix + "test_seal.txt");
      if (hdfs.exists(file)) {
        hdfs.delete(file, true);
      }

      FSDataOutputStream fdos = 
        hdfs.create(file);

      OutputStream os = fdos;

      BufferedWriter br = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));
      String prefix = "hbase:text/test_seal.txt";
      String[] replicaGroups = new String[1];
      replicaGroups[0] = "0";
      fdos.setReplicaGroups(prefix, replicaGroups);
      fdos.sealCurBlock();
      // block 1
      String firstBlockStr = "test test before seal! " + input + "\n";
      br.write(firstBlockStr);
      br.flush();
      fdos.sealCurBlock();
      System.out.println("written " + firstBlockStr.length() + " bytes");
      // block 2
      br.write(firstBlockStr);
      br.flush();
      fdos.sealCurBlock();
      System.out.println("written " + firstBlockStr.length() + " bytes");
      //block 3
      fdos.setReplicaGroups(prefix, replicaGroups);
      br.write("after seal");
      br.close();
      Path destA = new Path(filePrefix + "destA");
      Path destB = new Path(filePrefix + "destB");
      hdfs.splitFileReuseBlocks(file, destA, destB, firstBlockStr.length() * 2);
      hdfs.close();
    } catch (Exception e) {
      System.out.println(e.getMessage());
      e.printStackTrace();
    }
  }
}
