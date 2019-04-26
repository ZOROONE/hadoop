package com.hadoop.tfidf;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SecondTfidfReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> iter, Context context) throws IOException, InterruptedException {
		
		//用于记录该单词在多少偏文章中出现过
		int sum = 0;
		for (IntWritable num : iter) {
			sum += num.get();
		}
		
		context.write(key, new IntWritable(sum));
	}

}
