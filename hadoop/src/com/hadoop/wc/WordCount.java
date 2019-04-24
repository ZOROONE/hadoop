package com.hadoop.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 该WordCount 要提交到集群，用Hadoop jar MyWC.jar com.hadoop.WordCount 命令运行
 * @author ZORO
 *
 */
public class WordCount {
	
	public static void main(String[] args) throws Exception {
		// 配置文件
		Configuration conf = new Configuration(true);
		// 可以conf.set()设置属性

		Job job = Job.getInstance(conf);
		// jar包名称
		job.setJarByClass(WordCount.class);
		// 给job作业起名字
		job.setJobName("wc");

		Path inputPath = new Path("/user/root/test");
		// 输入可以有多个，所以add
		FileInputFormat.addInputPath(job, inputPath);

		// 一般大数据文件存在都有意义，所以需要检查是否存在
		Path outputDir = new Path("/user/root/wc_result");
		// 存在就删除，创建目录
		if (outputDir.getFileSystem(conf).exists(outputDir)) {
			outputDir.getFileSystem(conf).delete(outputDir, true);
		}
		// 一个输出目录，set
		FileOutputFormat.setOutputPath(job, outputDir);
		// 设置mapper类
		job.setMapperClass(MyMapper.class);
		// 设置mapper输出的key的类型
		job.setMapOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setReducerClass(MyReducer.class);

		// Submit the job, then poll for progress until the job is complete
		job.waitForCompletion(true);
	}

}
