package com.java.hbase.crud;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

public class DeleteData {
	public static void main(String[] args) throws IOException {

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

		// Instantiating Delete class
		Delete delete = new Delete(Bytes.toBytes("row1"));
		delete.deleteColumn(Bytes.toBytes("personal"), Bytes.toBytes("name"));
		delete.deleteFamily(Bytes.toBytes("professional"));

		// deleting the data
		table.delete(delete);

		// closing the HTable object
		table.close();
		System.out.println("data deleted.....");
	}
}
