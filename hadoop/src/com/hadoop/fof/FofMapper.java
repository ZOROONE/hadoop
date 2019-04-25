package com.hadoop.fof;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

public class FofMapper extends Mapper<Object, Text, Text, IntWritable> {

	Text mkey = new Text();
	// 用于表示间接好友关系
	IntWritable one = new IntWritable(1);
	// 用于表示直接好友关系
	IntWritable zero = new IntWritable(0);

	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		// 一条数据 tom hello hadoop cat ，以空格分开， 第一个名字是本人，第二个是他的好友
		String[] names = StringUtils.split(value.toString(), ' ');

		for (int i = 1; i < names.length; i++) {
			// 先打印第i个好友与本人的直接关系
			mkey.set(getNameConn(names[0], names[1]));
			context.write(mkey, zero);

			// 打印第i个好友与其他好友的间接关系
			// 可能你会发现，只有a:b, 没有b:a,怎么给b推荐呢，reduce可以输出结果两个
			for (int j = i + 1; j < names.length; j++) {
				mkey.set(this.getNameConn(names[i], names[j]));
				context.write(mkey, one);
			}

		}

	}

	/**
	 * 将两个名字拼接，并排序，为了方便后面计算
	 * 
	 * @param name0
	 * @param name1
	 * @return
	 */
	public String getNameConn(String name0, String name1) {
		if (name0.compareTo(name1) > 0) {
			return name0 + ":" + name1;
		}
		return name1 + ":" + name0;
	}
}
