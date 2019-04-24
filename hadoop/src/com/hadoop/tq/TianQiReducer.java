package com.hadoop.tq;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TianQiReducer extends Reducer<TianQi, IntWritable, Text, IntWritable> {
	
	Text rkey = new Text();
	IntWritable rvalue = new IntWritable();
	
	
	@Override
	protected void reduce(TianQi tq, Iterable<IntWritable> iter, Context context) throws IOException, InterruptedException {
		
		/**
		 * 我们这迭代，因为我们map输出的value都是1，所以这里都是1,对我们没有什么意义
		 * 那么我们为什么还要迭代呢，这就涉及到源码了，底层调用nextKeyValue(),key和value都重新赋值
		 * 即上面传进来的tq是不断变化的，由于我们根据key排序决定相同年月，wd倒序排序，
		 * 根据group分组比较器，决定这一组的宽度，即相同年月为一组，调用一次reduce
		 */
		
		for (IntWritable intWritable : iter) {
			int num = 0;
			int day = 0 ;
			
			//如果是第一个数，直接输出，并赋值day
			if(num == 0){
				rkey.set(tq.getYear() + "-" + tq.getMonth() + "-" + tq.getDay());
				rvalue.set(tq.getWd());
				context.write(rkey, rvalue);
				day = tq.getDay();
			} else{
				//如果和第一不是同一天，输出，否则继续执行
				if(tq.getDay() != day){
					rkey.set(tq.getYear() + "-" + tq.getMonth() + "-" + tq.getDay());
					rvalue.set(tq.getWd());
					context.write(rkey, rvalue);
					break;
				}
			}
		}
	}
}
