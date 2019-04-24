package com.hadoop.wc;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//默认行为，Mapper传进来一行，按偏移量传，鼠标看提示怎么写，Object其实本质是LongWritable
public class MyMapper extends Mapper<Object, Text, Text, IntWritable> {

	private IntWritable one = new IntWritable(1);
	// 输出文本单词，文本类型，不能用string,Text文本类型
	private Text word = new Text();

	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		// 默认按\t\n\r\f"去切，String split只支持但字符切割，例如 ；
		StringTokenizer words = new StringTokenizer(value.toString());
		while (words.hasMoreTokens()) {
			// word值传递，引用传递，所以后面的会覆盖前面的，但是context.write即mapper输出会给buffer(字节数组)，
			// 将对象序列化为字节数组，你后面改不改跟这个没关系了

			word.set(words.nextToken());
			context.write(word, one);
		}

	}
}
