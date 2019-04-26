package com.hadoop.tfidf;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class SecondTfidfMapper extends Mapper<Object, Text, Text, IntWritable> {
	
	Text mkey = new Text();
	IntWritable one = new IntWritable(1);
		
	@Override
	protected void map(Object key, Text value, Context context)throws IOException, InterruptedException {
		// 获取当前 mapper task的数据片段（split）
		FileSplit fileSplit =(FileSplit) context.getInputSplit();
		
		//如果不是3号分区，因为3号分区是记录微博总数的
		if(!fileSplit.getPath().getName().contains("part-r-00003")){
			
			// value:  豆浆_023y423894	17  代表着在023y423894微博中有17个豆浆词语
			String[] split = value.toString().split("\t");
			
			//我们想要统计豆浆一共在多少偏微博中出现过
			if(split.length > 1) {
				String[] ss = split[0].split("_");
				mkey.set(ss[0]);
				context.write(mkey, one);
			} else {
				System.err.println(value.toString());
			}
			
		}
	}
}
