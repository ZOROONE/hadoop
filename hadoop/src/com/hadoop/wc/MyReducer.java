package com.hadoop.wc;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	private IntWritable result = new IntWritable();

	@Override
	protected void reduce(Text word, Iterable<IntWritable> iter, Context context)
			throws IOException, InterruptedException {
		// sxt 1
		// sxt 1
		// sxt (1,1,1,1,1.....) sxt一组数据1t, 内存溢出了 解决排序时做简单合并，源码时分析
		// 这里的values，底层是将一个归并算法迭代器套在最后合并成的几个文件上面，
		//     调用.get（）方法时底层调用的nextKeyValue方法来更新k,v，值
		

		int sum = 0;
		for (IntWritable num : iter) {
			sum += num.get();
		}
		result.set(sum);
		context.write(word, result);

	}

}
