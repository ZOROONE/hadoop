package com.hadoop.test1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

public class ClientTest {

	Configuration conf = null;
	FileSystem fs = null;
	
	public void getConn() throws IOException{
		conf = new Configuration(true);
		fs = FileSystem.get(conf);
	}
	
}
