package com.distributed.algorithm.bfs;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import com.distributed.algorithm.Project1Application;
import com.distributed.algorithm.Project1Application.ToTCP;
import com.distributed.algorithm.util.AlgorithmType;
import com.distributed.algorithm.util.Config;
import com.distributed.algorithm.util.MessageFormat;
import com.distributed.algorithm.util.Synchronizer;

@Component
public class BFS implements Consumer<MessageFormat> {

	static Logger logger = LoggerFactory.getLogger(BFS.class);
	@Autowired
	Config config;

	@Autowired
	Synchronizer synchronizer;
	@Autowired
	ToTCP toTcp;
	@Autowired
	BFSCompute bfsCompute;

	// BFS Data
	Map<Integer, MessageFormat> msgToNeiboursMap;
	Map<Integer, Boolean> childrenMap; // mapping childeren to their NOC

	int parent;
	int numNOC;
	int numSearchSent;
	int numAckNackRecvd;
	int round;
	Semaphore semaphore;

	boolean algoDone;

	public BFS() {
		msgToNeiboursMap = new HashMap<>();
		childrenMap = new HashMap<>();
		parent = -1;
		numNOC = 0;
		numSearchSent = 0;
		numAckNackRecvd = 0;
		round = 0;
		semaphore = new Semaphore(1);
		algoDone=false;
	}
	
	

	public final Map<Integer, Boolean> getChildrenMap() {
		return childrenMap;
	}
	
	public void sendRoundMsg(Integer neibour,MessageFormat msg) {
		msgToNeiboursMap.put(neibour,msg);
	}

	public void init() throws InterruptedException {
		logger.debug("Init() BFS");
		for (int neibour : config.getNeibours()) {
			this.msgToNeiboursMap.put(neibour, null);
		}
		synchronizer.registerConsumer(AlgorithmType.BFS, this);
		// Send null messags to kick off the synchronization, todo try if not doing this
		// makes a difference
		MessageFormat msg = new MessageFormat(config.getMyId(), AlgorithmType.BFS);
		msg.setRoundNo(round);
		TimeUnit.SECONDS.sleep(5);
		for (int neibour : config.getNeibours()) {
			logger.debug("Sending Msg to "+neibour + " for Round: "+round+" Message:"+ msg.toString());
			this.toTcp.send(MessageFormat.buildMessage(msg), config.getNodesMap().get(neibour));
		}
	}

	public void buildBFS() throws InterruptedException {
		
		if(this.config.getNeibours().size()<1) {
			logger.info("This is the only node in the treE?");
			return;
		}
		logger.info("Staring BFS At:"+this.config.getMyId());
		parent = config.getMyId();// BFS being build at Leader
		numSearchSent = config.getNeibours().size();
		MessageFormat msg = new MessageFormat(config.getMyId(), AlgorithmType.BFS);
		// msg.setRoundNo(round); will be set by end of round method
		Map<String, String> algoData = new HashMap<String, String>();
		algoData.put("MsgType", BFSMsgType.SEARCH.toString());
		msg.setAlgoData(algoData);
		// Send Search Msgs to All at the end of the round
		semaphore.acquire();
		this.numSearchSent = config.getNeibours().size();
		for (int neibour : config.getNeibours()) {
			this.msgToNeiboursMap.put(neibour, msg);
			// this.toTcp.send(MessageFormat.buildMessage(msg),
			// config.getNodesMap().get(neibour));
		}
		semaphore.release();
	}

	public void endOfRound() {
		try {
			semaphore.acquire();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		logger.debug("End Of Round:" + round);
		round++;
		MessageFormat msgToSend;
		for (Entry<Integer, MessageFormat> entry : this.msgToNeiboursMap.entrySet()) {
			if (entry.getValue() != null) {
				msgToSend = entry.getValue();
				msgToSend.setRoundNo(this.round);
				logger.info("Sending Message to Node: "+ entry.getKey()+", round " + round + ": " + entry.getValue().toString());
			} else {
				msgToSend = new MessageFormat(config.getMyId(), AlgorithmType.BFS);
			}

			msgToSend.setRoundNo(this.round);
			logger.debug("Sending Message round " + round + ": " + msgToSend.toString());
			this.toTcp.send(MessageFormat.buildMessage(msgToSend), config.getNodesMap().get(entry.getKey()));
			entry.setValue(null); // clear the msg
		}
		semaphore.release();
	}
	
	public void markAlgoDone() {
		logger.info("Algorithm Ended");
		if(this.parent!=config.getMyId()) {
			logger.info("Node: "+config.getMyId()+"sending NOC to parent, All Children sent NOC");
			logger.info("Parent:"+this.parent);
			}
		else {
			logger.info("BFS Terminated at me:"+config.getMyId());
			logger.info("Starting BFS Compute(Max Degree):"+config.getMyId());
			bfsCompute.init();
		}
		
		for(int child:this.childrenMap.keySet()) {
			logger.info("Children: "+ child);	
		}
			this.algoDone=true;
	}
	@Override
	public void accept(MessageFormat t) {

		if (t == null) {
			logger.debug("End of Round BFS: " +this.round);
			endOfRound();
		} else {
			/* You may stull get a search message despite algorithm completion!
			if(this.algoDone)
				return;
			*/
			
			logger.info("BFS Message Recieved:"+t.toString());
			switch (BFSMsgType.valueOf(t.getAlgoData().get("MsgType"))) {
			case SEARCH:

				if (this.parent == -1) { // no parent
					this.parent = t.getMyId();

					// Send all Search to neibours
					MessageFormat replyMsg = new MessageFormat(config.getMyId(), AlgorithmType.BFS,
							BFSMsgType.SEARCH.getMap());
					for (Entry<Integer, MessageFormat> entry : this.msgToNeiboursMap.entrySet()) {
						entry.setValue(replyMsg);
					}

					this.msgToNeiboursMap.put(t.getMyId(),
							new MessageFormat(config.getMyId(), AlgorithmType.BFS, BFSMsgType.ACK.getMap()));
					/*
					 * WARNING: Make sure to Subtract from the numSearchSent when search message gets overriden in the case
					 * where you get a search msg from one the nodes which get replaced by the NACK.
					 */
					this.numSearchSent = this.config.getNeibours().size() - 1; 
					
					if(this.numSearchSent<1) {
						//Send NOCACK
						this.msgToNeiboursMap.get(t.getMyId()).getAlgoData().put("MsgType", BFSMsgType.NOCACK.toString());
						this.markAlgoDone();
					}
				} else {
					if(this.msgToNeiboursMap.get(t.getMyId())!=null) {
						if(this.msgToNeiboursMap.get(t.getMyId()).getAlgoData().get("MsgType")==BFSMsgType.SEARCH.toString()) {
							logger.info("Overiding Search Msg with NACK for Node:"+t.getMyId());
							this.numSearchSent--;
							if(this.numSearchSent<1) {
								//Send NOCACK
								this.msgToNeiboursMap.put(this.parent,new MessageFormat(config.getMyId(), AlgorithmType.BFS, BFSMsgType.NOCACK.getMap()));
								this.markAlgoDone();
							}
						}
						else {
							Assert.state(false, "Should not be overriding Msg Type"+this.msgToNeiboursMap.get(t.getMyId()).getAlgoData().get("MsgType"));
						}
					}
					this.msgToNeiboursMap.put(t.getMyId(),
							new MessageFormat(config.getMyId(), AlgorithmType.BFS, BFSMsgType.NACK.getMap()));
				}
				break;
			case ACK:
				if (this.numSearchSent > 0) {
					numSearchSent--;
					this.childrenMap.put(t.getMyId(), false);
				} else {
					logger.error("No Searches Sent, yet Ack Recieved from Node:" + t.getMyId());
				}
				break;
			case NACK:
				if (this.numSearchSent > 0) {
					numSearchSent--;
					if(this.numSearchSent<1) {
						//Send NOCACK
						this.msgToNeiboursMap.put(this.parent,new MessageFormat(config.getMyId(),AlgorithmType.BFS,BFSMsgType.NOC.getMap()));
						this.markAlgoDone();
					}
				} else {
					logger.error("No Searches Sent, yet NACK Recieved from Node:" + t.getMyId());
				}
				break;
			case NOCACK:
				if (this.numSearchSent > 0) {
					this.childrenMap.put(t.getMyId(), true);
					numSearchSent--;
				} else {
					logger.error("No Searches Sent, yet NOCACK Recieved from Node:" + t.getMyId());
				}
			case NOC:
				if(!this.childrenMap.containsKey(t.getMyId())) {
					logger.error("Not a child this node but got NOC from it: "+t.getMyId());
					return;
				}
				this.childrenMap.put(t.getMyId(), true);
				if(numSearchSent==0) { //All Ack and Nack are received
					boolean sendNOC=true;
					for(Entry<Integer,Boolean> entry:this.childrenMap.entrySet()) {
						if(!entry.getValue()) {
							sendNOC=false;
							break;
						}
					}
					
					if(sendNOC) {
						if(this.parent!=config.getMyId()) {
							this.msgToNeiboursMap.put(this.parent,
									new MessageFormat(config.getMyId(), AlgorithmType.BFS, BFSMsgType.NOC.getMap()));
						}
						this.markAlgoDone();
					}
					
				}
				break;
			case COMPUTE:
				bfsCompute.processMessage(t);
				break;
			default:
				break;

			}
		}

	}

}
