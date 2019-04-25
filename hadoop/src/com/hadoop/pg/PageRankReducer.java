package com.hadoop.pg;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PageRankReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<Text> iter, Context context) throws IOException, InterruptedException {

		// 相同的key为一组
		// key：页面名称比如B
		// 包含两类数据
		// B:1.0 C //页面对应关系及老的pr值

		// B:0.5 //投票值
		// B:0.5

		// 用于接收老的页面对应关系
		Node sourceNode = null;

		// 用于接收计算新的pr值
		double sum = 0.0;

		for (Text val : iter) {
			//遍历得到i的数据为1.0 C  B（页面对应关系及老的pr值）   或者    0.5
			Node node = Node.getNode(val.toString());
			if (node.containsOutNodeNames()) {
				//如果是页面对应关系及老的pr值 1.0 C  B  ，则将其赋给sourceNode
				sourceNode = node;
			} else {
				//如果不是，则将票面值相加，最后得到新的票面值
				sum += node.getPageRank();
			}
		}
		
		System.out.println("*********** new pageRank value is " + sum);
		// 循环结束以后，得到新老pr值, 求差值
		double d = sum - sourceNode.getPageRank();
		int j = (int) (d * 1000);
		context.getCounter(MyCounter.my).increment(Math.abs(j));

		sourceNode.setPageRank(sum);
		// 输出 K:A V:0.9 C B
		context.write(key, new Text(sourceNode.toString()));
	}
}
