package com.java.hbase.HBaseExamples;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;

import com.google.common.primitives.Bytes;

public class AlterAddColumnFamily {
	public static void main(String args[]) throws MasterNotRunningException, IOException{
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
	      //admin.disableTable("employee");
	      //admin.deleteTable("employee");
	   // Instantiating columnDescriptor class
	      HColumnDescriptor columnDescriptor = new HColumnDescriptor("contactDetails");
	      
	      // Adding column family
	      //admin.addColumn("employee", columnDescriptor);
	      //admin.deleteColumn("employee", "contactDetails");
	      HTable hTable = new HTable(conf, "employee");
	      
	      boolean isColumExist = hTable.exists(new Get("contactDetails".getBytes()));
	      System.out.println("coloumn added"+isColumExist);
	      //Stop the HBase
	      //admin.shutdown();
	   }
}
