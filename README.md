#In hbase shell create the table with 10 regions
create 'htab3', {NAME => 'cf'},   {SPLITS => ['0', '1', '2', '3', '4','5','6','7','8','9']}

#/user/cloudera/hhbase/wc contains the data to be loaded into the hbase table created above
export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:/etc/hbase/conf/:/opt/cloudera/parcels/CDH/jars/*

hadoop jar HBLoad1-0.0.1-SNAPSHOT.jar com.aaa.mapr.hbmapr.Driver /user/cloudera/hhbase/wc /user/cloudera/hhbase/wc1_out wc1

#Change owner of files created by the above mapreduce job to hbase

sudo -u hdfs hdfs dfs -chown -R   hbase:hbase /user/cloudera/hhbase/wc1_out

# Bulk Load the files into HBase
hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles -Dcreate.table=no /user/cloudera/hhbase/wc1_out wc1

