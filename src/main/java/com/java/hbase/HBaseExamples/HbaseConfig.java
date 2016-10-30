package com.java.hbase.HBaseExamples;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggerFactory;

public class HbaseConfig {
	    public static org.apache.hadoop.conf.Configuration getHHConfig() {
	        Configuration conf = HBaseConfiguration.create();
	        InputStream confResourceAsInputStream = conf.getConfResourceAsInputStream("hbase-site.xml");
	        int available = 0;
	        try {
	            available = confResourceAsInputStream.available();
	        } catch (Exception e) {
	            //for debug purpose
	           System.out.println("configuration files not found locally");
	        } finally {
	            IOUtils.closeQuietly(confResourceAsInputStream);
	        }
	        if (available == 0 ) {
	            conf = new Configuration();
	            conf.addResource("core-site.xml");
	            conf.addResource("hbase-site.xml");
	            conf.addResource("hdfs-site.xml");
	        }
	        return conf;
	    }
	    public static void main(String[] args) throws IOException {
	        Configuration config = HbaseConfig.getHHConfig();
	        HTable table = new HTable(config, "s1");
	        System.out.println(table.getTableName());
	}
}
