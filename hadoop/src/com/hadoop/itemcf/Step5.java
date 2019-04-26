package com.hadoop.itemcf;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 把相乘之后的矩阵相加获得结果矩阵
 * @author ZORO
 *
 */
public class Step5 {

	public static boolean run(Configuration conf, Map<String, String> paths) throws Exception {
		Job job = Job.getInstance(conf);
		job.setJarByClass(Step5.class);
		job.setJobName("step5");
		
		FileInputFormat.addInputPath(job, new Path(paths.get("Step5Input")));
		Path outputDir = new Path(paths.get("Step5Output"));
		if(outputDir.getFileSystem(conf).exists(outputDir)){
			outputDir.getFileSystem(conf).delete(outputDir, true);
		}
		FileOutputFormat.setOutputPath(job, outputDir);
		
		job.setMapperClass(Step5Mapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(Step5Reducer.class);
		
		return job.waitForCompletion(true);
	}
	
	static class Step5Mapper extends Mapper<Object, Text, Text, Text>{
		Text mkey = new Text();
		Text mval = new Text();
		
		@Override
		protected void map(Object key, Text value, Context context)throws IOException, InterruptedException {
			// value : uid	itemid,source
			String[] split = Pattern.compile("[\t,]").split(value.toString());
			mkey.set(split[0]);
			mval.set(split[1] + "," + split[2]);
			context.write(mkey, mval);
		}
	}
	
	static class Step5Reducer extends Reducer<Text, Text, Text, Text>{
		@Override
		protected void reduce(Text key, Iterable<Text> iter, Context context)throws IOException, InterruptedException {
			Map<String, Double> map = new HashMap<String, Double>();
			
			for (Text val : iter) {
				//val : itemid,source
				String[] split = val.toString().split(",");
				String itemid = split[0];
				double score = Double.valueOf(split[1]);
				
				if(map.containsKey(itemid)){
					map.put(itemid, map.get(itemid) + score);
				} else {
					map.put(itemid, score);
				}
			}
			
			Set<Entry<String, Double>> entrySet = map.entrySet();
			
			for (Entry<String, Double> entry : entrySet) {
				String itemid = entry.getKey();
				Double score = entry.getValue();
				
				context.write(key, new Text(itemid + "," + score));
			}
		}
	}
	
	
}
