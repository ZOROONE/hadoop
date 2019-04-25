package com.hadoop.fof;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.StringUtils;

public class FofReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	Text rkey = new Text();
	IntWritable rval = new IntWritable();
	
	@Override
	protected void reduce(Text nameconn, Iterable<IntWritable> iter, Context context) throws IOException, InterruptedException {
		//一组数据，可能值既有1，又有0，有0表示这两个人是直接好友，没必要继续下面了，输出结果了
		// tom:hadoop 1
		// tom:hadoop 0
		
		int sum = 0;
		boolean isNotFriend = true;
		
		for (IntWritable val : iter) {
			if(val.get() == 0){
				isNotFriend = false;
				//直接结束
				break;
			} else {
				sum += val.get();
			}
		}
		
		//最后判断
		if(isNotFriend){
			//解决map端的不能交换输出问题
			rval.set(sum);
			context.write(nameconn, rval);
			context.write(getRkey(nameconn), rval);
			
		}
		
	}

	/**
	 * 传进来 map:hadoop   return hadoop:map
	 * @param nameconn
	 * @return
	 */
	private Text getRkey(Text nameconn) {
		String[] split = StringUtils.split(nameconn.toString(), ':');
		rkey.set(split[1] + ":" + split[0]);
		return rkey;
	}
}
