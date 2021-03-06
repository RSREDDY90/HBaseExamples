package com.java.hbase.crud;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
public class InsertData {
	@SuppressWarnings("deprecation")
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
	      
	   // Instantiating HTable class
	      HTable hTable = new HTable(conf, "emp");

	      // Instantiating Put class
	      // accepts a row name.
	      Put p = new Put(Bytes.toBytes("row1")); 

	      // adding values using add() method
	      // accepts column family name, qualifier/row name ,value
	      p.add(Bytes.toBytes("personal"),
	      Bytes.toBytes("name"),Bytes.toBytes("raju"));

	      p.add(Bytes.toBytes("personal"),
	      Bytes.toBytes("city"),Bytes.toBytes("hyderabad"));

	      p.add(Bytes.toBytes("professional"),Bytes.toBytes("designation"),
	      Bytes.toBytes("manager"));

	      p.add(Bytes.toBytes("professional"),Bytes.toBytes("salary"),
	      Bytes.toBytes("50000"));
	      
	      // Saving the put Instance to the HTable.
	      hTable.put(p);
	      System.out.println("data inserted");
	      
	      // closing HTable
	      hTable.close();

	      
	      
	}
}
