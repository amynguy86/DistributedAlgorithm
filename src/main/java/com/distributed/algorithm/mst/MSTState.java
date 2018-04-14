package com.distributed.algorithm.mst;

import java.util.HashMap;
import java.util.Map;

/*
 * SEARCH=no DATA
 * ConvergeCAST = edgeCost in data
 * Accept = includes nodeID that is suppose to initiateMerge
 * Merge = no Data
 * Leader = data contains Leader ID
 */

public enum MSTState {
	MWOE("MWOE"), Merge("Merge"),LeaderBroadcast("LeaderBroadCast"),NONE("None");
	
private String string;

MSTState(String name){string =name;}
	
@Override
public String toString() {
	return string;
 }

}

