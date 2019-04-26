package com.hadoop.tfidf;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 该mr实现统计一共有多少偏文章，每个文章每个单词的有多少次
 * @author ZORO
 *
 */
public class FirstTfidf {
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration(true);
		conf.set("mapreduce.app-submission.coress-paltform", "true");
		conf.set("mapreduce.framework.name", "local");
		
		FileSystem fs = FileSystem.get(conf);
		Job job = Job.getInstance(conf);
		job.setJarByClass(FirstTfidf.class);
		job.setJobName("firsttfidf");
		
		//input
		FileInputFormat.addInputPath(job, new Path("hdfs://mycluster/user/root/tfidf/input"));
		//output
		Path outputDir = new Path("hdfs://mycluster/user/root/tfidf/output/firstjob");
		if(fs.exists(outputDir)){
			fs.delete(outputDir, true);
		}
		FileOutputFormat.setOutputPath(job, outputDir);
		
		//map
		job.setMapperClass(FirstTfidfMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		//reducer
		job.setReducerClass(FirstTfidfReducer.class);
		
		//others
		job.setCombinerClass(FirstTfidfReducer.class);
		job.setNumReduceTasks(4);
		job.setPartitionerClass(FirstTfidfPartitioner.class);
		
		//submmit
		job.waitForCompletion(true);

	}
}
