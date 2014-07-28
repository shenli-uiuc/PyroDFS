rm -rf /tmp/hadoop*
rm -rf /tmp/hdfs
mkdir -p /srv/scratch/shenli3/srcs
mkdir -p /srv/scratch/shenli3/software
rm /srv/scratch/shenli3/srcs/hadoop-2.4.1-shen.tar.gz
rm -rf /srv/scratch/shenli3/software/hadoop-2.4.1
cp /home/shenli3/software/src/hadoop-2.4.1-shen.tar.gz /srv/scratch/shenli3/srcs/
tar zxf /srv/scratch/shenli3/srcs/hadoop-2.4.1-shen.tar.gz -C /srv/scratch/shenli3/software/
rm -rf /srv/scratch/shenli3/hdfs
mkdir -p /srv/scratch/shenli3/hdfs/name
mkdir -p /srv/scratch/shenli3/hdfs/tmp
