package com.hadoop.tq;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * map 异写时排序比较器
 * 
 * @author ZORO
 *
 * 继承WritableComparator类，这个类里面的方法比较全，字节，字符串都已经封装好了，比较简单
 */
public class TianQiSortComparator extends WritableComparator {

	public TianQiSortComparator() {
		// 告诉WritableComparator，key的类型，否则序列化反序列化时找不到，报空指针异常
		super(TianQi.class, true);
	}

	// 需求是每个月中最高两天的温度，因此map端异写排序时应该按年月里面的温度进行倒序
	@Override
	public int compare(WritableComparable o1, WritableComparable o2) {
		TianQi tq1 = (TianQi) o1;
		TianQi tq2 = (TianQi) o2;

		int y = tq1.getYear() - tq2.getYear();
		// 或者 int y = Integer.compare(tq1.getYear(), tq2.getYear())

		// 如果年份相同
		if (y == 0) {
			int m = tq1.getMonth() - tq2.getMonth();
			// 如果月份相同
			if (m == 0) {
				// 实现温度倒序
				return tq2.getWd() - tq1.getWd();
			}

			return m;

		}

		return y;
	}

}
