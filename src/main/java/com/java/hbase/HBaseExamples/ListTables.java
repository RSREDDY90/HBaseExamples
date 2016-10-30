package com.java.hbase.HBaseExamples;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
public class ListTables {

	public static void main(String[] args) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
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

	      // Getting all the list of tables using HBaseAdmin object
	      HTableDescriptor[] tableDescriptor = admin.listTables();

	      // printing all the table names.
	      for (int i=0; i<tableDescriptor.length;i++ ){
	         System.out.println(tableDescriptor[i].getNameAsString());
	      }

	}

}
