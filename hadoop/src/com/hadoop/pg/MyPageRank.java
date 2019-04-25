package com.hadoop.pg;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyPageRank {
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration(true);
		//在linux平台外跑的时候设置为true，否则可能会报错
		conf.set("mapreduce.app-submission.corss-paltform", "true");
		//本机模拟跑，设置为local
		conf.set("mapreduce.framework.name", "local");

		
		
		//看看第几次跑，动态设置读取和输出路径
		int i = 0;
		//结束条件
		double d = 0.00001;
		while(true){
			i++;
			//设置数，写到配置文件中，map中就能取到，判断第几次跑
			conf.setInt("runCount", i);
			FileSystem fs = FileSystem.get(conf);
			Job job = Job.getInstance(conf);
			job.setJarByClass(MyPageRank.class);
			job.setJobName("pagerankd");
			
			//input
			Path input = null;
			if(i == 1) {
				input = new Path("hdfs://mycluster/user/root/pg/input");
			} else {
				//否则就是上一次mr输出目录
				input = new Path("hdfs://mycluster/user/root/pg/output/pr" + (i - 1));
			}
			FileInputFormat.addInputPath(job, input);
			
			//output
			Path outputDir = new Path("hdfs://mycluster/user/root/pg/output/pr" + i);
			if(fs.exists(outputDir)){
				fs.delete(outputDir, true);
			}
			FileOutputFormat.setOutputPath(job, outputDir);
			
			//map
			job.setMapperClass(PageRankMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			
			//reduce
			job.setReducerClass(PageRankReducer.class);
			
			//others
			//该输入格式化，读取一行，按制表符分割前面为key,后面为val    key: A    val: B C         
			job.setInputFormatClass(KeyValueTextInputFormat.class);
			
			boolean flag = job.waitForCompletion(true);
			
			if(flag){
				System.out.println("success" + i);
				//取回计数器
				long sum = job.getCounters().findCounter(MyCounter.my).getValue();
				System.out.println(sum);
				//因为在reduce里面放大了1000倍
				double avg = sum / 4000.0;
				//如果满足结束条件，则结束mr
				if(avg < d){
					break;
				}
			}
		}
		
	}
}
