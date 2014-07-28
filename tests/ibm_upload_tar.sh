cp ./conf-ibm/* ../hadoop-dist/target/hadoop-2.4.1/etc/hadoop
rm -rf ../hadoop-dist/target/hadoop-2.4.1/scripts
cp -r ./scripts-ibm ../hadoop-dist/target/hadoop-2.4.1/scripts
cp -r ./tests ../hadoop-dist/target/hadoop-2.4.1/
cd ../hadoop-dist/target/
tar -cvzf hadoop-2.4.1-shen.tar.gz ./hadoop-2.4.1
#mv hadoop-2.4.1-shen.tar.gz ../../../tests/
scp ./hadoop-2.4.1-shen.tar.gz spatiotemporal@icusrv95.watson.ibm.com:/home/spatiotemporal/tar
