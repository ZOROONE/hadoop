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
		// �õ������ļ�
		conf = new Configuration(true);
		// ���������ļ��õ��ļ�ϵͳ
		fs = FileSystem.get(conf);
	}

	@After
	public void after() throws IOException {
		// �ر������ļ�
		fs.close();
	}

	@Test
	public void mkdir() throws IOException {
		//��ǰ��root�˻�������Ĭ��hdfs://node001:8020/user/root/temp����˼
		Path path = new Path("temp");
		// �鿴�Ƿ����Ŀ¼��������ھ�ɾ��
		if (fs.exists(path)) {
			fs.delete(path, true);
		}
		fs.mkdirs(path);
	}
	
	@Test
	public void upload() throws IOException {
		//�ļ��ϴ�
		Path path = new Path("hdfs://mycluster/user/root/temp/aa.txt");
		FSDataOutputStream outputStream = fs.create(path);
		
		//������
		InputStream inputStream = new BufferedInputStream(new FileInputStream(new File("e:/test1")));
		
		IOUtils.copyBytes(inputStream, outputStream, conf, true);
	}
	
	@Test
	public void readHDFSFileTest() throws IOException{
		Path path = new Path("/user/root/test");
		
		FileStatus file = fs.getFileStatus(path);
		BlockLocation[] blockLocations = fs.getFileBlockLocations(file, 0, file.getLen());
		for (BlockLocation blockLocation : blockLocations) {
			System.err.println(blockLocation);
		}
		
		FSDataInputStream inputStream = fs.open(path);
		System.out.println((char)inputStream.readByte());
		System.out.println((char)inputStream.readByte());
		System.out.println((char)inputStream.readByte());
		System.out.println((char)inputStream.readByte());
		//�����ĸ��ֽڶ�ȡ
		inputStream.seek(0);
		System.out.println((char)inputStream.readByte());
		System.out.println((char)inputStream.readByte());
		System.out.println((char)inputStream.readByte());
		System.out.println((char)inputStream.readByte());
	}
	
}
