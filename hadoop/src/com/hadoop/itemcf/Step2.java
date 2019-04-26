package com.hadoop.itemcf;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Step2 extends Mapper<Object, Text, Text, Text> {

	public static boolean run(Configuration conf, Map<String, String> paths) throws Exception {
		Job job = Job.getInstance(conf);
		job.setJarByClass(Step2.class);
		job.setJobName("setpe2");

		// input
		Path inputPath = new Path(paths.get("Step2Input"));
		FileInputFormat.addInputPath(job, inputPath);

		// output
		Path outputDir = new Path(paths.get("Step2Output"));
		if (outputDir.getFileSystem(conf).exists(outputDir)) {
			outputDir.getFileSystem(conf).delete(outputDir, true);
		}
		FileOutputFormat.setOutputPath(job, outputDir);

		// map
		job.setMapperClass(Step2Mapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// reducer
		job.setReducerClass(step2Reducer.class);

		// submit
		return job.waitForCompletion(true);
	}

	static class Step2Mapper extends Mapper<Object, Text, Text, Text> {
		Text mkey = new Text();
		Text mval = new Text();

		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			// i1,u2723,click,2014/9/1 18:31 商品名称， 用户id, 操作行为
			String[] split = value.toString().split(",");
			String itemid = split[0];
			String userid = split[1];
			String action = split[2];

			mkey.set(userid);
			mval.set(itemid + ":" + StartRun.R.get(action));
			// u2723 i1:1
			context.write(mkey, mval);
		}
	}

	/**
	 * 输出格式 userid itemid:2,itemid:1,itemid:8
	 * 
	 * @author ZORO
	 *
	 */
	static class step2Reducer extends Reducer<Text, Text, Text, Text> {
		Map<String, Integer> map = new HashMap<String, Integer>();

		@Override
		protected void reduce(Text key, Iterable<Text> iter, Context context) throws IOException, InterruptedException {
			for (Text val : iter) {
				String[] split = val.toString().split(":");
				String itemid = split[0];
				Integer action = Integer.parseInt(split[1]);
				action = map.get(itemid) == null ? 0 : map.get(itemid) + action;
				map.put(itemid, action);
			}
			

			StringBuilder sb = new StringBuilder();
			for (Entry<String, Integer> entry : map.entrySet()) {
				sb.append(entry.getKey()).append(":").append(entry.getValue()).append(",");
			}
			String mval = sb.substring(0, sb.lastIndexOf(","));

			context.write(key, new Text(mval.toString()));
		}
	}
}
