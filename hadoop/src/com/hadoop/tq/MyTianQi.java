package com.hadoop.tq;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 
 * @author ZORO 取每月温度最高的两天
 *         1949-10-01 14:21:02 34c 
 *         1949-10-01 19:21:02 38c 
 *         1949-10-02 14:01:02 36c 
 *         1950-01-01 11:21:02 32c 
 *         1950-10-01 12:21:02 37c 
 *         1951-12-01 12:21:02 23c 
 *         1950-10-02 12:21:02 41c 
 *         1950-10-03 12:21:02 27c
 *         1951-07-01 12:21:02 45c 
 *         1951-07-02 12:21:02 46c 
 *         1951-07-03 12:21:03 47c
 * 
 */
public class MyTianQi {
	
	public static void main(String[] args) throws Exception {
		
		//1. conf
		Configuration conf = new Configuration(true);
		//2. job
		Job job = Job.getInstance(conf);
		
		//3. setJar
		job.setJarByClass(MyTianQi.class);
		job.setJobName("tianqi");
		
		//4. input
		Path inputPath = new Path("/user/root/tq/input");
		FileInputFormat.addInputPath(job, inputPath);
		
		//5. output
		Path outputDir = new Path("/user/root/tq/output");
		if(outputDir.getFileSystem(conf).exists(outputDir)){
			outputDir.getFileSystem(conf).delete(outputDir, true);
		}
		FileOutputFormat.setOutputPath(job, outputDir);
		
		//6. map
		job.setMapperClass(TianQiMapper.class);
		job.setMapOutputKeyClass(TianQi.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		//7. reducer
		job.setReducerClass(TianQiReducer.class);
		
		// others
		job.setNumReduceTasks(2);
		job.setPartitionerClass(TianQiPartitioner.class);
		job.setSortComparatorClass(TianQiSortComparator.class);
		job.setGroupingComparatorClass(TianQiGroupingComparator.class);
		
		//8. submmit
		job.waitForCompletion(true);
		
		
	}
	
}
