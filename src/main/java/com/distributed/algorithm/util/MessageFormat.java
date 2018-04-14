package com.distributed.algorithm.util;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.springframework.util.SerializationUtils;

@SuppressWarnings("serial")
public class MessageFormat implements Serializable {
 private int myId;
 private AlgorithmType algorithmType;
 private int roundNo;
 private Map<String,String> algoData;
 
 public MessageFormat(int myId,AlgorithmType algorithmType) {
	 this.myId=myId;
	 this.algorithmType=algorithmType;
	 roundNo=-1;
	 algoData=null;
 }
 
 public MessageFormat(int myId,AlgorithmType algorithmType,Map<String,String> algoData) {
	 this.myId=myId;
	 this.algorithmType=algorithmType;
	 roundNo=-1;
	 this.algoData=algoData;
 }
 
 public void setRoundNo(int roundNo) {
	 this.roundNo=roundNo;
 }
 
 public int getRoundNo() {
	 return this.roundNo;
 }
 
 public  void setAlgoData(Map<String,String> algoData) {
	 this.algoData=algoData;
 }
 
 public int getMyId() {
	return myId;
}

public AlgorithmType getAlgorithmType() {
	return algorithmType;
}

public Map<String, String> getAlgoData() {
	return algoData;
}

@Override
public String toString() {
	return "MessageFormat [myId=" + myId + ", algorithmType=" + algorithmType + ", roundNo=" + roundNo + ", algoData="
			+ algoData + "]";
}
 
 public static byte[] buildMessage(MessageFormat msg) {
	 return SerializationUtils.serialize(msg);
 }
 
 public static MessageFormat buildMessage(byte[] data) {
	 return (MessageFormat) SerializationUtils.deserialize(data);
 }
 
}
