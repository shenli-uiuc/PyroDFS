# execute on tareka05
rm /home/tarek/shenli3/software/src/hadoop-2.4.1-shen.tar.gz
cp ../../hadoop-2.4.1-shen.tar.gz /home/tarek/shenli3/software/src/
cp ./setup_hdfs_node.sh /home/tarek/shenli3/script/pssh/hadoop/
pssh -h ../etc/hadoop/master -e /home/tarek/shenli3/script/pssh/hadoop/msgerr -o /home/tarek/shenli3/script/pssh/hadoop/msgout /home/shenli3/script/pssh/hadoop/setup_hdfs_node.sh
pssh -h ../etc/hadoop/slaves -e /home/tarek/shenli3/script/pssh/hadoop/msgerr -o /home/tarek/shenli3/script/pssh/hadoop/msgout /home/shenli3/script/pssh/hadoop/setup_hdfs_node.sh
pssh -h ../etc/hadoop/hbase_nodes -e /home/tarek/shenli3/script/pssh/hadoop/msgerr -o /home/tarek/shenli3/script/pssh/hadoop/msgout /home/shenli3/script/pssh/hadoop/setup_hdfs_node.sh
