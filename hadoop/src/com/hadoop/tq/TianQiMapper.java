package com.hadoop.tq;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

public class TianQiMapper extends Mapper<Object, Text, TianQi, IntWritable> {

	TianQi tq = new TianQi();
	//只是一个占位符而已，因为key里面已经有温度了
	IntWritable one = new IntWritable(1);

	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		try {
			// value 1949-10-01 14:21:02 34c, 首先按照制表符切割 得到日期和温度
			String[] split = StringUtils.split(value.toString(), '\t');
			// 时间类
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			Date date = sdf.parse(split[0]);
			// 日历类
			Calendar cal = Calendar.getInstance();
			cal.setTime(date);

			tq.setYear(cal.get(cal.YEAR));
			tq.setMonth(cal.get(cal.MONTH) + 1);
			tq.setDay(cal.get(cal.DAY_OF_MONTH));

			int wd = Integer.parseInt(split[1].substring(0, split[1].indexOf("c")));
			tq.setWd(wd);
			
			context.write(tq, one);
		} catch (ParseException e) {
			e.printStackTrace();
		}

	}
}
