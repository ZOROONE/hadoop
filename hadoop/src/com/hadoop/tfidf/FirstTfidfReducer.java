package com.hadoop.tfidf;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FirstTfidfReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	@Override
	protected void reduce(Text key, Iterable<IntWritable> iter, Context context) throws IOException, InterruptedException {
		//进来两种类型的数据
		// 第一种是前三个分区的    key: 豆浆_123124234  val: 3
		// 第二种是3号分区的           key: count   val: 1000 
		
		//计算每个单词在每偏文章的出现频率
		int sum = 0;
		for (IntWritable num : iter) {
			sum += num.get();
		}
		
		if(key.equals(new Text("count"))){
			//如果是微博条目，打印出来，一共多少条
			System.out.println("weibo count-----------" + sum);
		}
		////输出结果集
		context.write(key, new IntWritable(sum));
	}

}
