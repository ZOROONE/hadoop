package com.hadoop.tq;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class TianQiPartitioner extends Partitioner<TianQi, IntWritable> {

	@Override
	public int getPartition(TianQi key, IntWritable value, int numPartitions) {
		//简单定义一下
		return key.getYear() % numPartitions;
	}
	
	
}
