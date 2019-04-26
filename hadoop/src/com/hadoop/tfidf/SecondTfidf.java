package com.hadoop.tfidf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 计算某个词语在多少个微博中出现过
 * @author ZORO
 *
 */
public class SecondTfidf {

	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration(true);
		conf.set("mapreduce.app-submission.coress-paltform", "true");
		conf.set("mapreduce.framework.name", "local");
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(SecondTfidf.class);
		job.setJobName("secondtfidf");
		
		//input
		FileInputFormat.addInputPath(job, new Path("hdfs://mycluster/user/root/tfidf/output/firstjob"));
		
		//output
		Path outputDir =  new Path("hdfs://mycluster/user/root/tfidf/output/secondjob");
		if(outputDir.getFileSystem(conf).exists(outputDir)) {
			outputDir.getFileSystem(conf).delete(outputDir, true);
		}
		FileOutputFormat.setOutputPath(job, outputDir);
		
		//map 
		job.setMapperClass(SecondTfidfMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		//reduce
		job.setReducerClass(SecondTfidfReducer.class);
		
		//others
		job.setCombinerClass(SecondTfidfReducer.class);
		
		
		//submit
		job.waitForCompletion(true);
		
		
	}
}
