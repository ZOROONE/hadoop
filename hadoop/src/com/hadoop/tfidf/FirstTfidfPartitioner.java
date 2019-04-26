package com.hadoop.tfidf;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class FirstTfidfPartitioner extends HashPartitioner<Text, IntWritable> {

	@Override
	public int getPartition(Text key, IntWritable value, int numPartitions) {
		
		if(key.equals(new Text("count"))){
			return 3;
		}
		
		return super.getPartition(key, value, numPartitions - 1);
	}

}
