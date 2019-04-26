package com.hadoop.tfidf;

import java.io.IOException;
import java.io.StringReader;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

public class FirstTfidfMapper extends Mapper<Object, Text, Text, IntWritable> {

	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		// val: 3823890210294392 今天我约了豆浆，油条
		String[] split = value.toString().split("\t");

		if (split.length > 1) {
			// 3823890210294392
			String id = split[0].trim();
			// 今天我约了豆浆，油条
			String content = split[1];
			
			//下面这两两行是中文分词器，做分词用的
			StringReader sr = new StringReader(content);
			IKSegmenter ik = new IKSegmenter(sr, true);
			Lexeme word = null;
			//对一行迭代，得到每个词
			while((word = ik.next()) != null){
				String w = word.getLexemeText();
				//输出k:今天_3823890210294392 v:1
				context.write(new Text(w + "_" + id), new IntWritable(1));
			}
			
			//一行即一个微博条目，一篇文章，用于统计有多少偏文章
			context.write(new Text("count"), new IntWritable(1));
		}

	}
}
