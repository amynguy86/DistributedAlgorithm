package com.distributed.algorithm.bfs;

import java.util.HashMap;
import java.util.Map;

public enum BFSMsgType {
SEARCH("SEARCH"), ACK("ACK"), NACK("NACK"),NOC("NOC"),NOCACK("NOCACK"),COMPUTE("COMPUTE");
	
private String string;

BFSMsgType(String name){string =name;}
	
@Override
public String toString() {
	return string;
 }

public Map<String,String> getMap(){
	Map<String,String> map=new HashMap<>();
	map.put("MsgType",this.toString());
	return map;
}

/*
 * Adds more data for the MsgType
 */
public Map<String,String> getMap(String data){
	Map<String,String> map=new HashMap<>();
	map.put("MsgType",this.toString());
	map.put("Data", data);
	return map;
}

}

