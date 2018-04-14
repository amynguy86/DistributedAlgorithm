package com.distributed.algorithm.mst;

import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
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
import com.distributed.algorithm.mst.MSTMsgType.MSTMsgTypeData;
import com.distributed.algorithm.util.AlgorithmType;
import com.distributed.algorithm.util.Config;
import com.distributed.algorithm.util.MessageFormat;
import com.distributed.algorithm.util.Synchronizer;

@Component
public class MST implements Consumer<MessageFormat> {

	static Logger logger = LoggerFactory.getLogger(MST.class);
	@Autowired
	Config config;

	@Autowired
	Synchronizer synchronizer;
	@Autowired
	ToTCP toTcp;

	// MFS Data

	// <COST,ID>,sorted by cost, these nodes could or could not be in the same
	// component, Test Messages are required
	TreeMap<Integer, Integer> nonTreeNeibourMap;

	Set<Integer> treeNeibourSet; // <ID> //Neibours that are in the tree
	Map<Integer, MessageFormat> msgToNeiboursMap; // <ID,MSG>, contains ALL neibours with their messages to be send at
													// the beggining of the new round
	
	Set<Integer> nodesThatSentAccept; //Set to hold node IDs that send Accept Messages, this is added to the treeEdges and removed from the 
	//non tree edges at the end of the mergeState

	int parent; // node that sent the search
	int numSearchSent;
	int numConvergeCast;
	int round;
	MSTState state;
	boolean algoDone;
	int phase;
	int leader;
	MWOE mwoe;

	boolean acceptSent;
	int nodeThatwasSentAccept;
	
	int numTestMessagesSent;
	int numSearchMessagesSent;

	public static class MWOE {
		public int nodeID;
		public int cost;

		public void put(Map<String, String> map) {
			map.put("nodeID", Integer.toString(nodeID));
			map.put("cost", Integer.toString(cost));
		}

		public void get(Map<String, String> map) {
			nodeID = Integer.parseInt(map.get("nodeID")); // nodeID that won!
			cost = Integer.parseInt(map.get("cost"));
		}
	}

	public MST() {
		treeNeibourSet = new HashSet<>();
		nonTreeNeibourMap = new TreeMap<>();
		nodesThatSentAccept = new HashSet<>();
		parent = -1;
		numSearchSent = 0;
		numConvergeCast = 0;
		round = 0;
		phase = 0;
		algoDone = false;
		state = MSTState.NONE;
		leader = -1;
		mwoe = new MWOE();
		acceptSent = false;
		nodeThatwasSentAccept = -1;
		numTestMessagesSent = 0;
		numSearchMessagesSent = 0;
	}

	public void sendRoundMsg(Integer neibour, MessageFormat msg) {
		msgToNeiboursMap.put(neibour, msg);
	}

	public void init() throws InterruptedException {
		logger.debug("Init() MST");
		leader = config.getMyId();
		setState(MSTState.MWOE);
		mwoe.nodeID = config.getMyId();

		for (Entry<Integer, Integer> neibour : config.getNeiboursCostmap().entrySet()) {
			this.msgToNeiboursMap.put(neibour.getKey(), null);
			this.nonTreeNeibourMap.put(neibour.getValue(), neibour.getKey()); // todo change cost
		}

		mwoe.cost = nonTreeNeibourMap.firstKey(); // todo edge case here where it does not have any neibour
		synchronizer.registerConsumer(AlgorithmType.MST, this);
		MessageFormat msg = new MessageFormat(config.getMyId(), AlgorithmType.MST);
		msg.setRoundNo(round);
		// Send null messags to kick off the synchronization(No Broadcast/Converge
		// because nothing in tree at beginning)
		TimeUnit.SECONDS.sleep(config.getStartupTime());

		for (Entry<Integer, Integer> neibour : config.getNeiboursCostmap().entrySet()) {
			logger.debug("Sending Msg to " + neibour + " for Round: " + round + " Message:" + msg.toString());
			this.toTcp.send(MessageFormat.buildMessage(msg), config.getNodesMap().get(neibour.getKey()));
		}
	}

	private void setState(MSTState state) {
		logger.info("Entering State:" + state.toString());
		this.state = state;
	}

	private void setPhase(int phase) {
		logger.info("Entering Phase:" + phase);
		this.phase = phase;
	}

	private void updateStateAndPhase(int lastRound) {
		int totalRoundElapsed = lastRound - phase * (config.getPhaseSize()) + 1;
		if (totalRoundElapsed == config.getNumRoundMWOE()) {
			startMerge();
		} else if (totalRoundElapsed == config.getNumRoundMerge() + config.getNumRoundMWOE()) {
			startLeaderBroadCast();
		} else if (totalRoundElapsed == config.getPhaseSize()) {
			startFindingMWOE();
		}
	}

	private void startFindingMWOE() {
		setPhase(phase + 1);
		setState(MSTState.MWOE);
		if (config.getMyId() == leader) {
			MessageFormat msgToSend = new MessageFormat(config.getMyId(), AlgorithmType.MST,
					MSTMsgType.SEARCH.getMap());
			for (int treeNeibour : treeNeibourSet) {
				msgToNeiboursMap.put(treeNeibour, msgToSend);
			}
		}
		// New phase,reset values
		parent = -1;
		numTestMessagesSent = 0;
		numSearchMessagesSent = 0;
	}

	private void startMerge() {
		setState(MSTState.Merge);
		if (leader == config.getMyId()) { // If the leader is suppose to initiate the merge since it identified the MWOE
			if (mwoe.nodeID == config.getMyId()) {
				logger.info(
						"Leader node: " + config.getMyId() + " must initiate merge with node with cost: " + mwoe.cost);
				MessageFormat msgToSend = new MessageFormat(config.getMyId(), AlgorithmType.MST,
						MSTMsgType.ACCEPT.getMap());
				msgToNeiboursMap.put(this.nonTreeNeibourMap.firstKey(), msgToSend);
				treeNeibourSet.add(this.nonTreeNeibourMap.firstKey());
				this.nodeThatwasSentAccept = nonTreeNeibourMap.firstKey();
				this.nonTreeNeibourMap.remove(nodeThatwasSentAccept);
				acceptSent = true;

			} else {
				// BroadCast Merge
				logger.info("Leader node: " + config.getMyId() + " must broadcast to tree node " + mwoe.nodeID
						+ " to initate merge with cost: " + mwoe.cost);
				Map<String, String> map = MSTMsgType.MERGE.getMap();
				map.put(MSTMsgTypeData.MERGE_ID.toString(), Integer.toString(mwoe.nodeID));
				MessageFormat msgToSend = new MessageFormat(config.getMyId(), AlgorithmType.MST, map);
				for (int treeNeibour : treeNeibourSet) {
					msgToNeiboursMap.put(treeNeibour, msgToSend);
				}
			}
			// Reset
			mwoe.nodeID = -1;
			mwoe.cost = -1;
		}

	}

	private void startLeaderBroadCast() {
		setState(MSTState.LeaderBroadcast);
		if (nodesThatSentAccept.contains(nodeThatwasSentAccept) && this.acceptSent) {
			leader = Math.max(nodeThatwasSentAccept, config.getMyId());
			if (leader == config.getMyId()) {
				// broadcast
				logger.info("Broadcasting myself as the Leader");
				MessageFormat msgToSend = new MessageFormat(config.getMyId(), AlgorithmType.MST, MSTMsgType.LEADER
						.getMap(MSTMsgType.MSTMsgTypeData.LEADER.toString(), Integer.toString(config.getMyId())));
				for (int treeNeibour : treeNeibourSet) {
					msgToNeiboursMap.put(treeNeibour, msgToSend);
				}
			}
		}
		
		for(int nodeId:nodesThatSentAccept) {
			this.treeNeibourSet.add(nodeId);
			this.nonTreeNeibourMap.remove(config.getNeiboursCostmap().get(nodeId));
		}

		acceptSent = false;
		nodeThatwasSentAccept = -1;
		nodesThatSentAccept.clear();
	}

	public void endOfRound() {
		logger.debug("End Of Round:" + round);
		updateStateAndPhase(round);
		round++;
		MessageFormat msgToSend;
		for (Entry<Integer, MessageFormat> entry : this.msgToNeiboursMap.entrySet()) {
			if (entry.getValue() != null) {
				msgToSend = entry.getValue();
				msgToSend.setRoundNo(this.round);
				logger.info("Sending Message to Node: " + entry.getKey() + ", round " + round + ": "
						+ entry.getValue().toString());
			} else {
				msgToSend = new MessageFormat(config.getMyId(), AlgorithmType.MST);
			}

			msgToSend.setRoundNo(this.round);
			logger.debug("Sending Message round " + round + ": " + msgToSend.toString());
			this.toTcp.send(MessageFormat.buildMessage(msgToSend), config.getNodesMap().get(entry.getKey()));
			entry.setValue(null); // clear the msg
		}

	}

	public void markAlgoDone() {
		logger.info("Algorithm Ended");
		if (leader == config.getMyId()) {
			logger.info("Algorithm Terminated");
			// todo broadcast termination and print final tree edges
		}
		this.algoDone = true;
		synchronizer.UnRegisterConsumer(AlgorithmType.MST);
	}

	private void sendEndofRound(int toNode, MessageFormat msg) {
		if (this.msgToNeiboursMap.get(toNode) != null) {
			logger.error("Software Error, Could not be overiding message");
			assert (false);
		}

		this.msgToNeiboursMap.put(toNode, msg);
	}

	private void sendConvergeCast() {
		if (this.parent != -1) {
			Map<String, String> map = MSTMsgType.CONVERGECAST.getMap();
			map.put(MSTMsgType.MSTMsgTypeData.EDGE_COST.toString(), Integer.toString(this.mwoe.cost));
			map.put(MSTMsgType.MSTMsgTypeData.NODE_ID.toString(), Integer.toString(this.mwoe.nodeID));
			MessageFormat ConvergeCastMsg = new MessageFormat(config.getMyId(), AlgorithmType.MST, map);
			sendEndofRound(this.parent, ConvergeCastMsg);
		} else {
			Assert.state(this.leader == config.getMyId(), "Parent is null yet this node is not the leader");
			logger.info("Leader process revieced all MWOE from childrens, Mwoe.nodeID=" + mwoe.nodeID + " Mwoe.cost="
					+ mwoe.cost);
		}
	}

	private void sendTestMessages() {
		// Send Test Messages to figure out MWOE
		MessageFormat testMsg = new MessageFormat(config.getMyId(), AlgorithmType.MST, MSTMsgType.TEST.getMap());

		for (Entry<Integer, Integer> entry : this.nonTreeNeibourMap.entrySet()) {
			sendEndofRound(entry.getValue(), testMsg);
		}
	}

	private void forwardLeaderBroadcast() {
		MessageFormat msg = new MessageFormat(config.getMyId(), AlgorithmType.MST,
				MSTMsgType.LEADER.getMap(MSTMsgType.MSTMsgTypeData.LEADER.toString(), Integer.toString(this.leader)));
		for (int entry : this.treeNeibourSet) {
			sendEndofRound(entry, msg);
		}
	}

	private void forwardMergeBroadcast(int intendedNodeId) {
		MessageFormat msg = new MessageFormat(config.getMyId(), AlgorithmType.MST, MSTMsgType.MERGE
				.getMap(MSTMsgType.MSTMsgTypeData.MERGE_ID.toString(), Integer.toString(intendedNodeId)));
		for (int entry : this.treeNeibourSet) {
			sendEndofRound(entry, msg);
		}
	}
	
	private void sendAcceptMessage() {
		MessageFormat msg = new MessageFormat(config.getMyId(), AlgorithmType.MST, MSTMsgType.ACCEPT.getMap());
		logger.info("Sending Accept Message to Node"+mwoe.nodeID+" with cost: "+mwoe.cost);
		sendEndofRound(mwoe.nodeID, msg);
	}

	@Override
	public void accept(MessageFormat t) {

		if (t == null) {
			logger.debug("End of Round MST: " + this.round);
			endOfRound();
		} else {

			logger.info("MST Message Recieved:" + t.toString());
			switch (MSTMsgType.valueOf(t.getAlgoData().get("MsgType"))) {
			case SEARCH:
				/*
				 * Broadcast message recieved from leader, just forward it to all the tree
				 * edges, we will do the test messages in the ConvergeCast backup, or here if
				 * there are no more tree edges
				 */

				if (this.state != MSTState.MWOE) {
					logger.error("Search Message recieved but State is not: " + MSTState.MWOE.toString());
					assert (false);
				}
				if (this.parent != -1) {
					logger.error("Parent is already assigned to this node, Parent=" + this.parent);
					assert (false);
				}

				this.parent = t.getMyId();

				// forward Search to Neibours

				MessageFormat forwardMsg = new MessageFormat(config.getMyId(), AlgorithmType.MST,
						MSTMsgType.SEARCH.getMap());

				for (int treeNeibour : this.treeNeibourSet) {
					if (this.parent != treeNeibour) {
						sendEndofRound(treeNeibour, forwardMsg);
						this.numSearchMessagesSent++;
					}
				}

				if (this.treeNeibourSet.size() == 1) {
					logger.info("No more tree edges except the parent, starting convergecast process");

					sendTestMessages();
					this.numTestMessagesSent = this.nonTreeNeibourMap.size();

					if (this.nonTreeNeibourMap.size() == 0) {
						logger.info("No more non-tree edges, sending convergecast back to parent: " + this.parent);
						this.mwoe.cost = -1;
						this.mwoe.nodeID = -1;
						sendConvergeCast();
					}
				}

				break;

			case CONVERGECAST:
				if (this.state != MSTState.MWOE) {
					logger.error("ConvergeCast Message recieved but State is not: " + MSTState.MWOE.toString());
					assert (false);
				}

				if (this.numSearchMessagesSent == 0) {
					logger.error("No Search messages were sent, still converge cast was recieved");
					assert (false);
				}

				this.numSearchMessagesSent--;

				MWOE mwoeRecvd = new MWOE();
				mwoeRecvd.nodeID = Integer
						.parseInt(t.getAlgoData().get(MSTMsgType.MSTMsgTypeData.EDGE_COST.toString()));
				mwoeRecvd.cost = Integer.parseInt(t.getAlgoData().get(MSTMsgType.MSTMsgTypeData.NODE_ID.toString()));

				if (mwoeRecvd.cost < this.mwoe.cost) {
					this.mwoe.cost = mwoeRecvd.cost;
					this.mwoe.nodeID = mwoeRecvd.nodeID;
				}

				if (this.numSearchMessagesSent == 0) {
					// no more replies to be recieved

					if (this.nonTreeNeibourMap.size() == 0) {
						logger.info("No more non-tree edges, sending convergecast back to parent: " + this.parent);
						sendConvergeCast();
					} else {
						if (this.nonTreeNeibourMap.firstKey() < mwoe.cost) {
							logger.info("Starting to send test messages");
							sendTestMessages();
							this.numTestMessagesSent = this.nonTreeNeibourMap.size();
						} else {
							logger.info("No need to send test messages as nonTreeNeibourMap's min cost is bigger");
							sendConvergeCast();

						}
					}
				}

				break;
			case TEST:
				if (this.state != MSTState.MWOE) {
					logger.error("Test Message recieved but State is not: " + MSTState.MWOE.toString());
					assert (false);
				}

				MessageFormat msg = new MessageFormat(config.getMyId(), AlgorithmType.MST, MSTMsgType.TESTREPLY
						.getMap(MSTMsgType.MSTMsgTypeData.LEADER.toString(), Integer.toString(this.leader)));

				sendEndofRound(t.getMyId(), msg);
				break;

			case TESTREPLY:
				if (this.state != MSTState.MWOE) {
					logger.error("Test Message recieved but State is not: " + MSTState.MWOE.toString());
					assert (false);
				}

				if (this.numTestMessagesSent == 0) {
					logger.error("TestReply Message recieved but no test messages were sent");
					assert (false);
				}

				this.numTestMessagesSent--;
				int nodeLeaderId = Integer.parseInt(t.getAlgoData().get(MSTMsgType.MSTMsgTypeData.LEADER.toString()));
				assert (nodeLeaderId != -1);

				if (nodeLeaderId == this.leader) {
					logger.info("Removing node " + t.getMyId() + " from nonTreeEdges");
					this.nonTreeNeibourMap.remove(config.getNeiboursCostmap().get(t.getMyId()));
				} else {
					if (this.mwoe.cost > config.getNeiboursCostmap().get(t.getMyId())) {
						this.mwoe.cost = config.getNeiboursCostmap().get(t.getMyId());
						logger.debug("New MWOE being selected, nodeID:" + t.getMyId());
						this.mwoe.nodeID = t.getMyId();
					}
				}

				logger.info("Current MWOE NodeID:" + this.mwoe.nodeID + " ,Cost:" + this.mwoe.cost);

				if (this.numTestMessagesSent == 0) {
					logger.info("All Test Messages recieved");
					sendConvergeCast();
				}

				break;
			case ACCEPT:
				if (this.state != MSTState.Merge) {
					logger.error("ACCEPT Message recieved but State is not: " + MSTState.MWOE.toString());
					assert (false);
				}
				
				this.nodesThatSentAccept.add(t.getMyId());
				/* 
				Dont do this here, you may get accept from another component that will interfere with the merge broadcast as the
				tree would change
				
				this.treeNeibourSet.add(t.getMyId());
				this.nonTreeNeibourMap.remove(config.getNeiboursCostmap().get(t.getMyId()));
				*/
				
				break;

			case MERGE:
				if (this.state != MSTState.Merge) {
					logger.error("MERGE Message recieved but State is not: " + MSTState.MWOE.toString());
					assert (false);
				}

				int targetNodeId = -1; // node Id for which this merge is intented for
				targetNodeId = Integer.parseInt(t.getAlgoData().get(MSTMsgType.MSTMsgTypeData.MERGE_ID.toString()));

				if (targetNodeId == config.getMyId()) {
					this.sendAcceptMessage(); //accept message goes to mwoe.nodeId
					this.treeNeibourSet.add(mwoe.nodeID);
					this.nonTreeNeibourMap.remove(mwoe.cost);
					this.acceptSent=true;
					this.nodeThatwasSentAccept=mwoe.nodeID;
				} else {
					// forward this message intended for the node that is suppose to trigger ACCEPT
					forwardMergeBroadcast(targetNodeId);
				}
				break;

			case LEADER:
				if (this.state != MSTState.LeaderBroadcast) {
					logger.error("LeaderBroadcast Message recieved but State is not: "
							+ MSTState.LeaderBroadcast.toString());
					assert (false);
				}

				this.leader = Integer.parseInt(t.getAlgoData().get(MSTMsgType.MSTMsgTypeData.LEADER.toString()));

				// forward this on the tree edges
				this.forwardLeaderBroadcast();
				break;
			default:
				break;

			}
		}

	}

}