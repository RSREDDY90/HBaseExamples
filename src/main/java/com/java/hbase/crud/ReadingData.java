package com.java.hbase.crud;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import java.io.IOException;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
public class ReadingData {
	 public static void main(String[] args) throws IOException, Exception{
		   
		// Instantiating a configuration class
			Configuration conf = HBaseConfiguration.create();
			Path maprfsCoreSitePath = new Path(
					"/Users/hadoop/hadoop-2.5.2/etc/hadoop/core-site.xml");
			Path hdfsSitePath = new Path(
					"/Users/hadoop/hadoop-2.5.2/etc/hadoop/hdfs-site.xml");
			Path hBaseSitePath = new Path(
					"/Users/hadoop/hbase-1.1.2/confhbase-site.xml");

			// Add the resources to Configuration instance
			conf.addResource(maprfsCoreSitePath);
			conf.addResource(hdfsSitePath);
			conf.addResource(hBaseSitePath);

			// Instantiating HBaseAdmin class
			HBaseAdmin admin = new HBaseAdmin(conf);

	      // Instantiating HTable class
	      HTable table = new HTable(conf, "emp");

	      // Instantiating Get class
	      Get g = new Get(Bytes.toBytes("row1"));

	      // Reading the data
	      Result result = table.get(g);

	      // Reading values from Result class object
	      byte [] value = result.getValue(Bytes.toBytes("personal"),Bytes.toBytes("name"));

	      byte [] value1 = result.getValue(Bytes.toBytes("personal"),Bytes.toBytes("city"));

	      // Printing the values
	      String name = Bytes.toString(value);
	      String city = Bytes.toString(value1);
	      
	      System.out.println("name: " + name + " city: " + city);
	   }
}
