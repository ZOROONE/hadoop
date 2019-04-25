package com.hadoop.pg;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PageRankMapper extends Mapper<Text, Text, Text, Text> {

	Text mkey = new Text();
	Text mval = new Text();

	@Override
	protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		//从配置文件中取到runCount数，默认是1，如果没设置默认都是1
		int runCount = context.getConfiguration().getInt("runCount", 1);

		Node node;
		if (runCount == 1) {
			// 第一次运行
			node = Node.getNode("1.0", value.toString());
		} else {
			node = Node.getNode(value.toString());
		}

		// A:1.0 B D 传递老的pr值和对应的页面关系
		// K:A
		// V： 1.0 B D
		context.write(key, new Text(node.toString()));

		// 如果这个A有输出链
		if (node.containsOutNodeNames()) {
			String[] outNodeNames = node.getOutNodeNames();
			double quanzhong = node.getPageRank() / outNodeNames.length;

			// 因为出链连接权重一样
			mval.set(quanzhong + "");

			for (int i = 0; i < outNodeNames.length; i++) {
				mkey.set(outNodeNames[i]);
				// B:0.5
				// D:0.5 页面A投给谁，谁作为key，val是票面值，票面值为：A的pr值除以超链接数量
				context.write(mkey, mval);
			}
		}

	}
}
