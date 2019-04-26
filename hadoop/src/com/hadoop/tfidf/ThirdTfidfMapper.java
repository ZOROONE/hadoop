package com.hadoop.tfidf;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class ThirdTfidfMapper extends Mapper<Object, Text, Text, Text> {
	
	//存放微博总数
	private static Map<String, Integer> cmap = null;
	
	//存放df
	private static Map<String, Integer> dmap = null;
	
	//执行map前执行这个
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		System.out.println("*****************");
		if(cmap == null || cmap.size() == 0 || dmap == null || dmap.size() == 0){
			//取到缓存到内存的文件
			URI[] ss = context.getCacheFiles();
			if(ss != null) {
				for (URI uri : ss) {
					//将微博总数赋值给cmap
					if(uri.getPath().endsWith("part-r-00003")){
						Path path = new Path(uri.getPath());
						BufferedReader br = new BufferedReader(new FileReader(path.getName()));
						String line = br.readLine();
						//如果是总数
						if(line.startsWith("count")){
							String[] ls = line.split("\t");
							
							cmap = new HashMap<String, Integer>();
							cmap.put(ls[0], Integer.valueOf(ls[1]));
						}
						br.close();
					} else if(uri.getPath().endsWith("000")){
						// 词条的DF 即 豆浆	78
						dmap = new HashMap<String, Integer>();
						Path path = new Path(uri.getPath());
						BufferedReader br = new BufferedReader(new FileReader(path.getName()));
						String line = null;
						while((line = br.readLine()) != null){
							String[] ls = line.split("\t");
							//k 词名称      v 再多少个微博中出现过
							dmap.put(ls[0], Integer.parseInt(ls[1]));
						}
						
						br.close();
					}
				}
			}
		}
	}
	
	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		FileSplit fileSplit = (FileSplit)context.getInputSplit();
		//如果不是存放微博总数的文件，读取进来
		if(!fileSplit.getPath().getName().endsWith("part-r-00003")){
			//value 输给_3823992677218442	2
			String[] split = value.toString().split("\t");
			if(split.length >1) {
				// 输给  3823992677218442
				String[] ss = split[0].split("_");
				String w = ss[0];
				String id = ss[1];
				int tf = Integer.parseInt(split[1]);
				
				//这里有瑕疵，缺少一个job统计一个文件中所有单词的总和，看公式就知道了
				double tfidf = tf * Math.abs(Math.log(cmap.get("count") / dmap.get(w))); 
				
				//数字格式化类，保留5位小数
				NumberFormat nf = NumberFormat.getInstance();
				nf.setMaximumFractionDigits(5);
				
				//输出，k:词  v：tfidf
				context.write(new Text(id), new Text(w + ":" + nf.format(tfidf)));
			}
		}
		
	}
}
