package com.hadoop.tq;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * 用于存储map输出的key
 * 
 * @author ZORO
 *
 */
public class TianQi implements WritableComparable<TianQi> {

	private int year;
	private int month;
	private int day;
	private int wd;

	public int getWd() {
		return wd;
	}

	public void setWd(int wd) {
		this.wd = wd;
	}

	public int getYear() {
		return year;
	}

	public void setYear(int year) {
		this.year = year;
	}

	public int getMonth() {
		return month;
	}

	public void setMonth(int month) {
		this.month = month;
	}

	public int getDay() {
		return day;
	}

	public void setDay(int day) {
		this.day = day;
	}

	/**
	 * 和下面方法的写出顺序对应，反序列化
	 * 
	 * @param in
	 * @throws IOException
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		this.year = in.readInt();
		this.month = in.readInt();
		this.day = in.readInt();
		this.wd = in.readInt();
	}

	/**
	 * 序列化写出
	 * 
	 * @param out
	 * @throws IOException
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.year);
		out.writeInt(this.month);
		out.writeInt(this.day);
		out.writeInt(this.wd);
	}

	/**
	 * 国际规则正序， 按年月日排序
	 * 而我们想要的是，按年月正序，按温度倒序，需要另外写一个比较器
	 * @param o
	 * @return
	 */
	@Override
	public int compareTo(TianQi that) {
		int y = Integer.compare(this.year, that.year);
		// 如果是同一年
		if (y == 0) {
			// 继续比较月
			int m = Integer.compare(this.month, that.month);
			if (m == 0) {
				//继续比较日
				return Integer.compare(this.day, that.day);
			}
			return m;

		}

		return y;
	}

}
