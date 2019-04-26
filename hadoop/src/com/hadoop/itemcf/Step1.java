package com.hadoop.itemcf;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 数据清洗，去除重复行
 * 
 * @author ZORO
 *
 */
public class Step1 {

	public static boolean run(Configuration conf, Map<String, String> paths) throws Exception {

		Job job = Job.getInstance(conf);
		job.setJarByClass(Step1.class);
		job.setJobName("step1");

		// input
		Path inputPath = new Path(paths.get("Step1Input"));
		FileInputFormat.addInputPath(job, inputPath);

		// output
		Path outputDir = new Path(paths.get("Step1Output"));
		if (outputDir.getFileSystem(conf).exists(outputDir)) {
			outputDir.getFileSystem(conf).delete(outputDir, true);
		}
		FileOutputFormat.setOutputPath(job, outputDir);

		// map
		job.setMapperClass(Step1Mapper.class);
		job.setMapOutputKeyClass(Text.class);
		// 不需要输出value，所以定义为NullWritable类型
		job.setMapOutputValueClass(NullWritable.class);

		// reducer
		job.setReducerClass(Step1Reducer.class);
		
		// others
		//在map端先去重，
		job.setCombinerClass(Step1Reducer.class);
		
		// submit
		return job.waitForCompletion(true);
	}

	/**
	 * 这个进来的一个参数就是默认索引
	 * 
	 * @author ZORO
	 *
	 */
	static class Step1Mapper extends Mapper<LongWritable, Text, Text, NullWritable> {

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// 我们想要抹除第一行, 因为第一行是列名，并不是数据
			if (key.get() != 0) {
				// 将一行内容作为key输出，到reduce只读取一组的一条数据，达到去重的效果
				context.write(value, NullWritable.get());
			}
		}
	}

	static class Step1Reducer extends Reducer<Text, NullWritable, Text, NullWritable> {
		@Override
		protected void reduce(Text key, Iterable<NullWritable> iter, Context context) throws IOException, InterruptedException {
			////不用迭代，只输出第一行就可以，该组只输出一行,因为要达到去重的效果
			context.write(key, NullWritable.get());
		}
	}
}
