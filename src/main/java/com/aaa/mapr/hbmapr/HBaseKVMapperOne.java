package com.aaa.mapr.hbmapr;

import java.io.IOException;
import java.util.Locale;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * HBase bulk import example
 * <p>
 * Parses Facebook and Twitter messages from CSV files and outputs
 * <ImmutableBytesWritable, KeyValue>.
 * <p>
 * The ImmutableBytesWritable key is used by the TotalOrderPartitioner to map it
 * into the correct HBase table region.
 * <p>
 * The KeyValue value holds the HBase mutation information (column family,
 * column, and value)
 */
public class HBaseKVMapperOne extends
    Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue> {

  final static byte[] SRV_COL_FAM = "cf".getBytes();

  String tableName = "";


  ImmutableBytesWritable hKey;
  KeyValue kv;

  /** {@inheritDoc} */
  @Override
  protected void setup(Context context) throws IOException,
      InterruptedException {
    Configuration c = context.getConfiguration();

 //   tipOffSeconds = c.getInt("epoch.seconds.tipoff", 0);
    tableName = c.get("hbase.table.name");
  }

  /** {@inheritDoc} */
  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {


    String[] fields = null;

    fields = value.toString().split("\\,");

    hKey =  new ImmutableBytesWritable(Bytes.toBytes(fields[0])); // fields[0].getBytes();
    
    Integer in = new Integer(fields[1]);
    byte[] inb = Bytes.toBytes(in);

    
    // Service columns
    kv = new KeyValue(hKey.get(), SRV_COL_FAM,
          "wc_cnt".getBytes(), inb); // fields[1].getBytes());
    context.write(hKey, kv);
    
    context.getCounter("HBaseKVMapper", "NUM_MSGS").increment(1);

  }
}