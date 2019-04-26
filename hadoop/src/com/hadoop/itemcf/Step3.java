package com.hadoop.itemcf;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import sun.org.mozilla.javascript.internal.ast.ForInLoop;

/**
 * 构建商品的同现矩阵 i100:i100 3 i100:i105 1 i100:i106 1
 * 
 * @author ZORO
 *
 */
public class Step3 {

	public static boolean run(Configuration conf, Map<String, String> paths) throws Exception {
		Job job = Job.getInstance(conf);
		job.setJarByClass(Step3.class);
		job.setJobName("setp3");
		
		//input
		Path inputPath = new Path(paths.get("Step3Input"));
		FileInputFormat.addInputPath(job, inputPath);
		
		//output
		Path outputDir = new Path(paths.get("Step3Output"));
		if(outputDir.getFileSystem(conf).exists(outputDir)){
			outputDir.getFileSystem(conf).delete(outputDir, true);
		}
		FileOutputFormat.setOutputPath(job, outputDir);
		
		//map
		job.setMapperClass(Step3Mapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		//reducer
		job.setReducerClass(Step3Reducer.class);
		
		//other
		job.setCombinerClass(Step3Reducer.class);
		
		//submit
		return job.waitForCompletion(true);
	}
	
	static class Step3Mapper extends Mapper<Object, Text, Text, IntWritable> {
		Text mkey = new Text();
		IntWritable one = new IntWritable(1);
		
		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			//val u14	i25:0,i160:0,i223:0
			String[] split = value.toString().split("\t")[1].split(",");
			for (int i = 0; i < split.length; i++) {
				String itemA = split[i].split(":")[0];
				for (int j = 0; j < split.length; j++) {
					String itemB = split[j].split(":")[0];
					mkey.set(itemA + ":" + itemB);
					//组合
					context.write(mkey, one);
				}
				
			}
			
			
		}
	}
	
	static class Step3Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		IntWritable mval = new IntWritable();
		@Override
		protected void reduce(Text key, Iterable<IntWritable> iter, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable num : iter) {
				sum += num.get();
			}
			mval.set(sum);
			context.write(key, mval);
		}
	}
	
}
