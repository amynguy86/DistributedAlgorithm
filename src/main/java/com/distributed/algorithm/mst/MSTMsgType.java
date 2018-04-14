package com.distributed.algorithm.mst;

import java.util.HashMap;
import java.util.Map;

/*
 * SEARCH=no DATA
 * ConvergeCAST = edgeCost and Id of node with that edgeCost in data
 * Accept = no Data
 * Merge = includes nodeID that is suppose to initiateMerge
 * Leader = data contains LeaderID
 * TEST = no Data
 * TESTREPLY = Data contains LEADERID
 */

public enum MSTMsgType {
	SEARCH("SEARCH"), CONVERGECAST("CONVERGECAST"), ACCEPT("ACCEPT"), MERGE("MERGE"), LEADER("LEADER"),TEST("TEST"),TESTREPLY("TESTREPLY");
	private String string;

	MSTMsgType(String name) {
		string = name;
	}

	@Override
	public String toString() {
		return string;
	}

	public Map<String, String> getMap() {
		Map<String, String> map = new HashMap<>();
		map.put("MsgType", this.toString());
		return map;
	}

	/*
	 * Adds more data for the MsgType
	 */
	public Map<String, String> getMap(String key, String val) {
		Map<String, String> map = new HashMap<>();
		map.put("MsgType", this.toString());
		map.put(key, val);
		return map;
	}

	public static enum MSTMsgTypeData {
		MERGE_ID("MERGE-ID"),LEADER("LEADER"),NODE_ID("NODE_ID"),EDGE_COST("EDGE_COST");
		private String string;
		MSTMsgTypeData(String name){string =name;}

		@Override
		public String toString() {
			return string;
		}

	}

}
