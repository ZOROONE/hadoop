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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Step4 {

	public static boolean run(Configuration conf, Map<String, String> paths) throws Exception {
		Job job = Job.getInstance(conf);
		job.setJarByClass(Step4.class);
		job.setJobName("setp4");

		// input
		FileInputFormat.setInputPaths(job,
				new Path[] { new Path(paths.get("Step4Input1")), new Path(paths.get("Step4Input2")) });
		// output
		Path outputDir = new Path(paths.get("Step4Output"));
		if (outputDir.getFileSystem(conf).exists(outputDir)) {
			outputDir.getFileSystem(conf).delete(outputDir, true);
		}
		FileOutputFormat.setOutputPath(job, outputDir);

		// map
		job.setMapperClass(Step4Mapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// reducer
		job.setReducerClass(Step4Reducer.class);

		// others
		// submit
		return job.waitForCompletion(true);
	}

	static class Step4Mapper extends Mapper<Object, Text, Text, Text> {
		// A同现矩阵 or B得分矩阵
		private String parentName = null;
		Text mkey = new Text();
		Text mval = new Text();

		/**
		 * 每个maptask，初始化时调用一次
		 */
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			// 用于下面判断读取数据集
			parentName = fileSplit.getPath().getParent().getName();
			System.out.println("*********************8");
		}

		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] tokens = Pattern.compile("[\t,]").split(value.toString());

			if (parentName.equals("step3")) {
				// 同现矩阵内容是i100:i100 725
				String[] split = tokens[0].split(":");
				System.out.println();
				String itemA = split[0];
				String itemB = split[1];
				mkey.set(itemA);
				mval.set("A:" + itemB + "," + tokens[1]);
				context.write(mkey, mval);

			} else if (parentName.equals("step2")) {
				// 用户对物品喜爱得分矩阵
				// u3403 i9:17,i8:4,i7:7,i6:4,i5:5
				String uid = tokens[0];
				String itemid = null;
				String pre = null;
				for (int i = 1; i < tokens.length; i++) {
					String[] splits = tokens[i].split(":");
					itemid = splits[0];
					pre = splits[1];
					mkey.set(itemid);
					mval.set("B:" + uid + "," + pre);
					context.write(mkey, mval);
				}
			}

		}
	}

	static class Step4Reducer extends Reducer<Text, Text, Text, Text> {
		// A同现矩阵 or B得分矩阵
		// 某一个物品，针对它和其他所有物品的同现次数，都在mapA集合中
		// 和该物品（key中的itemID）同现的其他物品的同现集合// 。其他物品ID为map的key，同现数字为值
		Map<String, Integer> mapA = new HashMap<String, Integer>();
		// 该物品（key中的itemID），所有用户的推荐权重分数。
		Map<String, Integer> mapB = new HashMap<String, Integer>();

		@Override
		protected void reduce(Text key, Iterable<Text> iter, Context context) throws IOException, InterruptedException {
			for (Text val : iter) {
				if(val.toString().startsWith("A:")) {
					//同现矩阵 A:i100,725
					String[] split = val.toString().substring(2).split(",");
					String itemid = split[0];
					Integer num = Integer.parseInt(split[1]);
					mapA.put(itemid, num);
				} else if(val.toString().startsWith("B:")) {
					//用户偏好矩阵 u3403,17
					String[] split = val.toString().substring(2).split(",");
					String uid = split[0];
					Integer pre = Integer.parseInt(split[1]);
					mapB.put(uid, pre);
				}
			}
			
			Set<Entry<String, Integer>> entrySetA = mapA.entrySet();
			for (Entry<String, Integer> entryA : entrySetA) {
				String itemid = entryA.getKey();
				Integer num = entryA.getValue();
				
				Set<Entry<String, Integer>> entrySetB = mapB.entrySet();
				for (Entry<String, Integer> entryB : entrySetB) {
					String uid = entryB.getKey();
					Integer pre = entryB.getValue();
					double result = num * pre;
					context.write(new Text(uid), new Text(itemid + "," + result));
				}
			}
		}
	}
}
