package com.java.hbase.crud;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
public class ScanTable {
	public static void main(String args[]) throws IOException{

		   
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

	      // Instantiating the Scan class
	      Scan scan = new Scan();

	      // Scanning the required columns
	      scan.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("name"));
	      scan.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("city"));

	      // Getting the scan result
	      ResultScanner scanner = table.getScanner(scan);

	      // Reading values from scan result
	      for (Result result = scanner.next(); result != null; result = scanner.next())

	      System.out.println("Found row : " + result);
	      //closing the scanner
	      scanner.close();
	   }
}
