package com.hadoop.fof;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author ZORO
 * 找出好友的好友，做推荐
 * 数据如下，
 * tom hello hadoop cat
   world hadoop hello hive
   cat tom hive
   mr hive hello
   hive cat hadoop world hello mr
   hadoop tom hive world
   hello tom world hive mr

 * 
 * 需要得到结果：
 * hello: hadoop  3  即hello和hadoop有3个共同好友
 * hello: cat   2    即hello和cat有两个共同好友
 * 
 * 对这些结果排序，取前几名做推荐
 *
 * 该job实现第一个步骤结果
 */
public class MyFoF {
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration(true);
		Job job = Job.getInstance(conf);
		//设置jar包名称
		job.setJarByClass(MyFoF.class);
		//设置job名字
		job.setJobName("fof");
		
		//输入
		Path inputPath = new Path("hdfs://mycluster/user/root/fof/input");
		FileInputFormat.addInputPath(job, inputPath);
		
		//输出
		Path outputDir = new Path("hdfs://mycluster/user/root/fof/output");
		if(outputDir.getFileSystem(conf).exists(outputDir)){
			outputDir.getFileSystem(conf).delete(outputDir, true);
		}
		FileOutputFormat.setOutputPath(job, outputDir);
		
		//map
		job.setMapperClass(FofMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		//reduce
		job.setReducerClass(FofReducer.class);
		
		//other
		// job.setNumReduceTasks(3);
		// job.setPartitionerClass(FofPartitioner.class);  分区器
		// job.setSortComparatorClass(FofSortComparator.class);  map buffer环异写时sort， 不设置默认key的类型比较器
		// job.setGroupingComparatorClass(FofGroupingComparator.class);  reduce分组比较器，决定一组的宽度
		
		//提交
		job.waitForCompletion(true);
		
	}
}



















