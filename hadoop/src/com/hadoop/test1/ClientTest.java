package com.hadoop.test1;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ClientTest {

	Configuration conf = null;
	FileSystem fs = null;

	@Before
	public void getConn() throws IOException {
		// 得到配置文件
		conf = new Configuration(true);
		// 根据配置文件得到文件系统
		fs = FileSystem.get(conf);
	}

	@After
	public void after() throws IOException {
		// 关闭配置文件
		fs.close();
	}

	@Test
	public void mkdir() throws IOException {
		// 当前是root账户，所有默认hdfs:/mycluster/user/root/temp的意思
		Path path = new Path("temp");
		// 查看是否存在目录，如果存在就删除
		if (fs.exists(path)) {
			fs.delete(path, true);
		}
		fs.mkdirs(path);
	}

	@Test
	public void upload() throws IOException {
		// 文件上传
		Path path = new Path("hdfs://mycluster/user/root/temp/aa.txt");
		FSDataOutputStream outputStream = fs.create(path);

		// 输入流
		InputStream inputStream = new BufferedInputStream(new FileInputStream(new File("e:/test1")));

		IOUtils.copyBytes(inputStream, outputStream, conf, true);
	}

	@Test
	public void readHDFSFileTest() throws IOException {
		Path path = new Path("/user/root/test");

		FileStatus file = fs.getFileStatus(path);
		BlockLocation[] blockLocations = fs.getFileBlockLocations(file, 0, file.getLen());
		for (BlockLocation blockLocation : blockLocations) {
			System.err.println(blockLocation);
		}

		FSDataInputStream inputStream = fs.open(path);
		System.out.println((char) inputStream.readByte());
		System.out.println((char) inputStream.readByte());
		System.out.println((char) inputStream.readByte());
		System.out.println((char) inputStream.readByte());
		// 跳到哪个字节读取
		inputStream.seek(0);
		System.out.println((char) inputStream.readByte());
		System.out.println((char) inputStream.readByte());
		System.out.println((char) inputStream.readByte());
		System.out.println((char) inputStream.readByte());
	}

}
