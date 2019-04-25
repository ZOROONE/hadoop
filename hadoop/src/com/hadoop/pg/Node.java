package com.hadoop.pg;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.lang.StringUtils;

public class Node {

	// 该网站pr值
	private double pageRank = 1.0;
	// 出链
	private String[] outNodeNames;
	// 定义分隔符制表符， 后面方便使用
	private static final char SEPARATOR = '\t';

	public double getPageRank() {
		return pageRank;
	}

	public void setPageRank(double pageRank) {
		this.pageRank = pageRank;
	}

	public String[] getOutNodeNames() {
		return outNodeNames;
	}

	public void setOutNodeNames(String[] outNodeNames) {
		this.outNodeNames = outNodeNames;
	}

	/**
	 * 是否有出链
	 * 
	 * @return boolean
	 */
	public boolean containsOutNodeNames() {
		return this.outNodeNames != null && this.outNodeNames.length > 0;
	}

	@Override
	public String toString() {
		// 1.0 B D 用制表符拼接起来
		StringBuilder sb = new StringBuilder();
		sb.append(pageRank);
		// 如果有链再继续拼接
		if (this.containsOutNodeNames()) {
			sb.append(Node.SEPARATOR).append(StringUtils.join(this.getOutNodeNames(), Node.SEPARATOR));
		}
		return sb.toString();
	}

	public static Node getNode(String v0, String v1) throws IOException {
		// 第一次的时候调用该方法，将该网站的pr值传进来，拼成两个 //1.0 B D
		return Node.getNode(v0 + Node.SEPARATOR + v1);
	}

	public static Node getNode(String string) throws IOException {
		// 1.3 B D
		Node node = new Node();
		String[] splits = StringUtils.splitPreserveAllTokens(string, Node.SEPARATOR);
		if (splits.length < 1) {
			throw new IOException("Expected 1 or more parts but receive " + splits.length);
		}

		// 给本网点赋值pr值
		node.setPageRank(Double.valueOf(splits[0]));
		if (splits.length > 1) {
			// 将所有出链数赋值给adjacentNodeNames
			node.setOutNodeNames(Arrays.copyOfRange(splits, 1, splits.length));
		}

		return node;
	}

}
