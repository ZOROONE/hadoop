package com.hadoop.tq;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

//原理同TianQiSortComparator
public class TianQiGroupingComparator extends WritableComparator {

	public TianQiGroupingComparator() {
		super(TianQi.class, true);
	}

	// 分组比较器，根据需求，相同的年月为一组
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		TianQi tq1 = (TianQi) a;
		TianQi tq2 = (TianQi) b;

		int y = Integer.compare(tq1.getYear(), tq2.getYear());
		if (y == 0) {
			return Integer.compare(tq1.getMonth(), tq2.getMonth());
		}
		return y;
	}
}
