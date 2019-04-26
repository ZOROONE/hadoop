package com.hadoop.tfidf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ThirdTfidf {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration(true);

		// 集群运行，指定jar包位置，因为客户端要提交jar包到HDFS
		conf.set("mapred.jar", "C:\\Users\\ZORO\\Desktop\\tfidf.jar");
		// 如果想window运行必须设置下面
		conf.set("mapreduce.app-submission.cross-platform", "true");
		// 本地运行，要设置local

		Job job = Job.getInstance(conf);
		job.setJarByClass(ThirdTfidf.class);
		job.setJobName("thirdtfidf");

		// 把微博总数加载到内存
		job.addCacheFile(new Path("/user/root/tfidf/output/firstjob/part-r-00003").toUri());

		// 把df加载到内存
		job.addCacheFile(new Path("/user/root/tfidf/output/secondjob/part-r-00000").toUri());

		// input
		FileInputFormat.addInputPath(job, new Path("hdfs://mycluster/user/root/tfidf/output/firstjob"));
		
		// output
		Path outputDir = new Path("hdfs://mycluster/user/root/tfidf/output/third");
		if(outputDir.getFileSystem(conf).exists(outputDir)){
			outputDir.getFileSystem(conf).delete(outputDir, true);
		}
		FileOutputFormat.setOutputPath(job, outputDir);
		
		// map, key, value
		job.setMapperClass(ThirdTfidfMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		// reduce
		job.setReducerClass(ThirdTfidfReducer.class);
		
		// others

		// submit
		job.waitForCompletion(true);
	}
}
