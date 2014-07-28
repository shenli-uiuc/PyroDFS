rm -rf /tmp/hadoop*
mkdir -p /home/spatiotemporal/srcs
mkdir -p /home/spatiotemporal/software
rm /home/spatiotemporal/srcs/hadoop-2.4.1-shen.tar.gz
rm -rf /home/spatiotemporal/software/hadoop-2.4.1
scp spatiotemporal@icusrv95.watson.ibm.com:/home/spatiotemporal/tar/hadoop-2.4.1-shen.tar.gz /home/spatiotemporal/srcs/
tar zxf /home/spatiotemporal/srcs/hadoop-2.4.1-shen.tar.gz -C /home/spatiotemporal/software/
rm -rf /home/spatiotemporal/hdfs
mkdir -p /home/spatiotemporal/hdfs/name
mkdir -p /home/spatiotemporal/hdfs/tmp
