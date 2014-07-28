cp ./conf-dist/* ../hadoop-dist/target/hadoop-2.4.1/etc/hadoop
cp -r ./scripts ../hadoop-dist/target/hadoop-2.4.1/
cp -r ./tests ../hadoop-dist/target/hadoop-2.4.1/
cd ../hadoop-dist/target/
rm hadoop-2.4.1-shen.tar.gz
tar -cvzf hadoop-2.4.1-shen.tar.gz ./hadoop-2.4.1
#mv hadoop-2.4.1-shen.tar.gz ../../../tests/
scp ./hadoop-2.4.1-shen.tar.gz shenli3@tareka05.cs.uiuc.edu:/scratch/shenli3/software
