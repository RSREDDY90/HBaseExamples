package com.java.hbase.HBaseExamples;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class EnableOrDisableTable {
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
	      boolean isTableExist = admin.isTableAvailable("emp");
	      if(isTableExist){
	    	  System.out.println("table exist");
	    	// Verifying weather the table is disabled
		      Boolean bool = admin.isTableDisabled("emp");
		      System.out.println("is table disabled:"+bool);

		      // Disabling the table using HBaseAdmin object
		      if(!bool){
		         admin.disableTable("emp");
		         System.out.println("Table disabled");
		      }
	      }else {
	    	  Boolean bool = admin.isTableEnabled("emp");
		      System.out.println("is table disabled:"+bool);
		      if(!bool){
		    	  admin.enableTable("emp");
			      System.out.println("Table enabled");
		      }
	      }
	      
	   }
}
