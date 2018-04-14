package com.distributed.algorithm.util;

public enum AlgorithmType {
	PALEV("Palev"),BFS("BFS"),MST("MST");
	
	private String string;
	AlgorithmType(String name){string =name;}
		
	@Override
	public String toString() {
		return string;
	 }
}
