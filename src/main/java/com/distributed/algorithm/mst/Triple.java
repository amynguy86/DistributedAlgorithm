package com.distributed.algorithm.mst;
//Triple is a an object that defines lexigraphic order to same cost edges

import java.io.Serializable;

import org.springframework.util.Assert;

public class Triple implements Comparable<Triple> {


	private static final long serialVersionUID = 1L;
	private int cost;
	private int smallUID;
	private int largeUID;

	// Pass the nodes in any order;
	public Triple(int node1, int node2, int edgeCost) {
		smallUID = Math.min(node1, node2);
		largeUID = Math.max(node1, node2);
		cost = edgeCost;
	}
	
	public Triple(String node1, String node2, String edgeCost) {
		this(Integer.parseInt(node1),Integer.parseInt(node2),Integer.parseInt(edgeCost));
	}


	@Override
	public int compareTo(Triple triple) {
		// TODO Auto-generated method stub
		if (this.cost == triple.cost) {
			if (this.smallUID == triple.smallUID) {
				if(this.largeUID == triple.largeUID) {
					return 0;
				}
				else if (this.largeUID < triple.largeUID) {
					return -1;
				} else
					return 1;
			} else if (this.smallUID < triple.smallUID) {
				return -1;
			} else
				return 1;

		} else if (this.cost < triple.cost) {
			return -1;
		} else
			return 1;
	}
	
	public String toString() {
		return "<"+cost+","+smallUID+","+largeUID+">";
	}
	
	public String serialize() {
		return toString();
		
	}
	
	public static Triple deserialize(String tripleStr) {
		if(tripleStr.equalsIgnoreCase("null")) {
			return null;
		}
		
		String[] data = tripleStr.replace("<", "").replaceAll(">","").split(",");
		return  new Triple(data[0],data[1],data[2]);
		
	}

}
