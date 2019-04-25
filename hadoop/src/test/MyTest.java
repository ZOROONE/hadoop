package test;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;

import org.junit.Test;

public class MyTest {
	
	@Test
	public void test1() throws Exception{
		BufferedInputStream inputStream = new BufferedInputStream(new FileInputStream(new File("e:/tq.txt")));
	}
}
