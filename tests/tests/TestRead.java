import java.io.*;
import java.util.*;
import java.net.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;

public class TestRead {
  public static void main(String args[]) {
    try {
      String fn = args[0];

      Configuration conf = new Configuration();
      FileSystem hdfs = FileSystem.get(new URI("hdfs://localhost:9000"), conf);
      Path file = new Path("hdfs://localhost:9000/test/" + fn);
      if (!hdfs.exists(file)) {
        throw new IOException("File hdfs://localhost:9000/test/" + fn 
            + " does not exist!");
      }

      FSDataInputStream fdis = 
        hdfs.open(file);

      byte buffer [] = new byte [20];
      int pos = 0;
      while (fdis.read(pos, buffer, 0, 1) > 0){
        System.out.println(pos + ": " + (char)(buffer[0] & 0xFF));
        ++pos;
      }

      fdis.close();
      hdfs.close();
    } catch (Exception e) {
      System.out.println(e.getMessage());
      e.printStackTrace();
    }
  }
}
