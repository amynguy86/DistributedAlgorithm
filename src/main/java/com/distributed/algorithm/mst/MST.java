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

	// <Tripe,neibourID>,sorted by Triple, these nodes could or could not be in the
	// same
	// component, Test Messages are required
	TreeMap<Triple, Integer> nonTreeNeibourMap;

	Set<Integer> treeNeibourSet; // <ID> //Neibours that are in the tree
	Map<Integer, MessageFormat> msgToNeiboursMap; // <ID,MSG>, contains ALL neibours with their messages to be send at
													// the beggining of the new round

	Set<Integer> nodesThatSentAccept; // Set to hold node IDs that send Accept Messages, this is added to the
										// treeEdges and removed from the
	// non tree edges at the end of the mergeState

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
		public Triple cost;

		public void put(Map<String, String> map) {
			map.put(MSTMsgType.MSTMsgTypeData.NODE_ID.toString(), Integer.toString(nodeID));
			if (cost != null)
				map.put(MSTMsgType.MSTMsgTypeData.EDGE_COST.toString(), cost.toString());
			else
				map.put(MSTMsgType.MSTMsgTypeData.EDGE_COST.toString(), "NULL");
		}

		public void get(Map<String, String> map) {
			nodeID = Integer.parseInt(map.get(MSTMsgType.MSTMsgTypeData.NODE_ID.toString())); // nodeID that won!
			cost = Triple.deserialize(map.get(MSTMsgType.MSTMsgTypeData.EDGE_COST.toString()));
		}
	}

	public MST() {
		treeNeibourSet = new HashSet<>();
		nonTreeNeibourMap = new TreeMap<>();
		nodesThatSentAccept = new HashSet<>();
		msgToNeiboursMap = new HashMap<>();
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

		for (Entry<Integer, Triple> neibour : config.getNeiboursCostmap().entrySet()) {
			this.msgToNeiboursMap.put(neibour.getKey(), null);
			this.nonTreeNeibourMap.put(neibour.getValue(), neibour.getKey());
		}

		mwoe.cost = nonTreeNeibourMap.firstKey(); // todo edge case here where it does not have any neibour
		synchronizer.registerConsumer(AlgorithmType.MST, this);
		MessageFormat msg = new MessageFormat(config.getMyId(), AlgorithmType.MST);
		msg.setRoundNo(round);
		// Send null messags to kick off the synchronization(No Broadcast/Converge
		// because nothing in tree at beginning)
		TimeUnit.SECONDS.sleep(config.getStartupTime());

		logger.info("Sending Message to total of" + config.getNeiboursCostmap().size() + " Neibours");
		for (Entry<Integer, Triple> neibour : config.getNeiboursCostmap().entrySet()) {
			logger.info("Sending Msg to " + neibour.getKey() + " for Round: " + round + " Message:" + msg.toString());
			this.toTcp.send(MessageFormat.buildMessage(msg), config.getNodesMap().get(neibour.getKey()));
		}
	}

	private void setState(MSTState state) {
		logger.info("Entering State:" + state.toString());
		this.state = state;
	}

	private void setPhase(int phase) {
		logger.info("----------------------------------------------END OF PHASE:" + (phase-1)+"-------------------------------------------------------");
		logger.info("MST Tree Edges:");
		for (int node : this.treeNeibourSet) {
			logger.info(config.getNeiboursCostmap().get(node).toString());
		}
		logger.info("---------------------------------------------------------------------------------------------------------------------------");
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
		if (this.phase == config.getTotalPhase()) {
			this.markAlgoDone();
			return;
		}

		setState(MSTState.MWOE);
		parent = -1;
		numTestMessagesSent = 0;
		numSearchMessagesSent = 0;
		mwoe.nodeID = -1;
		mwoe.cost = null;

		if (config.getMyId() == leader) {
			MessageFormat msgToSend = new MessageFormat(config.getMyId(), AlgorithmType.MST,
					MSTMsgType.SEARCH.getMap());
			for (int treeNeibour : treeNeibourSet) {
				msgToNeiboursMap.put(treeNeibour, msgToSend);
				numSearchMessagesSent++;
			}
		}
	}

	private void startMerge() {
		setState(MSTState.Merge);
		if (leader == config.getMyId()) { // If the leader is suppose to initiate the merge since it identified the MWOE
			if (mwoe.nodeID == config.getMyId()) {
				logger.info("Leader node: " + config.getMyId() + " must initiate merge with node:" + " with cost: "
						+ mwoe.cost);
				MessageFormat msgToSend = new MessageFormat(config.getMyId(), AlgorithmType.MST,
						MSTMsgType.ACCEPT.getMap());
				msgToNeiboursMap.put(this.nonTreeNeibourMap.firstEntry().getValue(), msgToSend);
				treeNeibourSet.add(this.nonTreeNeibourMap.firstEntry().getValue());
				this.nodeThatwasSentAccept = nonTreeNeibourMap.firstEntry().getValue();
				this.nonTreeNeibourMap.remove(nonTreeNeibourMap.firstEntry().getKey());
				acceptSent = true;

			} else if (mwoe.nodeID != -1) {
				// BroadCast Merge
				logger.info("Leader node: " + config.getMyId() + " must broadcast to tree node " + mwoe.nodeID
						+ " to initate merge with cost: " + mwoe.cost);
				Map<String, String> map = MSTMsgType.MERGE.getMap();
				map.put(MSTMsgTypeData.MERGE_ID.toString(), Integer.toString(mwoe.nodeID));
				MessageFormat msgToSend = new MessageFormat(config.getMyId(), AlgorithmType.MST, map);
				for (int treeNeibour : treeNeibourSet) {
					msgToNeiboursMap.put(treeNeibour, msgToSend);
				}
			} else {
				// End of Algorithm
				logger.info("No more Edges to Merge");
				// Lets not end here, but end at the end of all phases!, this.markAlgoDone();
			}
		}

	}

	private void startLeaderBroadCast() {
		setState(MSTState.LeaderBroadcast);
		boolean resetLeader = true;

		for (int nodeId : nodesThatSentAccept) {
			this.treeNeibourSet.add(nodeId);
			this.nonTreeNeibourMap.remove(config.getNeiboursCostmap().get(nodeId));
		}

		if (nodesThatSentAccept.contains(nodeThatwasSentAccept) && this.acceptSent) {
			leader = Math.max(nodeThatwasSentAccept, config.getMyId());
			if (leader == config.getMyId()) {
				resetLeader = false;
				// broadcast
				logger.info("Broadcasting myself as the Leader");
				MessageFormat msgToSend = new MessageFormat(config.getMyId(), AlgorithmType.MST, MSTMsgType.LEADER
						.getMap(MSTMsgType.MSTMsgTypeData.LEADER.toString(), Integer.toString(config.getMyId())));
				for (int treeNeibour : treeNeibourSet) {
					msgToNeiboursMap.put(treeNeibour, msgToSend);
				}
			}
		}

		if (resetLeader)
			leader = -1;
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

		logger.info("Algorithm Terminated");
		logger.info("MST Tree Edges:");
		for (int node : this.treeNeibourSet) {
			logger.info(config.getNeiboursCostmap().get(node).toString());
		}

		this.algoDone = true;
		synchronizer.UnRegisterConsumer(AlgorithmType.MST);
	}

	private void sendEndofRound(int toNode, MessageFormat msg) {
		if (this.msgToNeiboursMap.get(toNode) != null) {
			logger.error("Overiding: " + this.msgToNeiboursMap.get(toNode) + "+ with " + msg);
			Assert.state(false, "Software Error, Could not be overiding message");
		}

		this.msgToNeiboursMap.put(toNode, msg);
	}

	private void sendConvergeCast() {
		if (this.parent != -1) {
			Map<String, String> map = MSTMsgType.CONVERGECAST.getMap();
			this.mwoe.put(map);
			MessageFormat ConvergeCastMsg = new MessageFormat(config.getMyId(), AlgorithmType.MST, map);
			sendEndofRound(this.parent, ConvergeCastMsg);
		} else {
			// Leader Process
			Assert.state(this.leader == config.getMyId(), "Parent is -1 yet this node is not the leader");
			logger.info("Leader process revieced all MWOE from childrens, Mwoe.nodeID=" + mwoe.nodeID + " Mwoe.cost="
					+ mwoe.cost);
		}
	}

	private void sendTestMessages() {
		// Send Test Messages to figure out MWOE
		MessageFormat testMsg = new MessageFormat(config.getMyId(), AlgorithmType.MST, MSTMsgType.TEST.getMap());

		for (Entry<Triple, Integer> entry : this.nonTreeNeibourMap.entrySet()) {

			if (this.msgToNeiboursMap.get(entry.getValue()) != null && this.msgToNeiboursMap.get(entry.getValue())
					.getAlgoData().get(MSTMsgTypeData.MSG_TYPE.toString()).equals(MSTMsgType.TESTREPLY.toString())) {
				// Handle Situation # 2, TestReply Message will be overridden by a Test Message
				this.msgToNeiboursMap.put(entry.getValue(), null);
				logger.info("Overiding TESTREPLY with a TESTREPLYREQUEST");
				MessageFormat msg = new MessageFormat(config.getMyId(), AlgorithmType.MST, MSTMsgType.TESTREPLYREQUEST
						.getMap(MSTMsgType.MSTMsgTypeData.LEADER.toString(), Integer.toString(this.leader)));
				this.sendEndofRound(entry.getValue(), msg);
			} else {
				sendEndofRound(entry.getValue(), testMsg);
			}
		}
	}

	private void forwardLeaderBroadcast(int sendingNode) {
		MessageFormat msg = new MessageFormat(config.getMyId(), AlgorithmType.MST,
				MSTMsgType.LEADER.getMap(MSTMsgType.MSTMsgTypeData.LEADER.toString(), Integer.toString(this.leader)));
		for (int entry : this.treeNeibourSet) {
			if (entry != sendingNode)
				sendEndofRound(entry, msg);
		}
	}

	private void forwardMergeBroadcast(int sendingNode, int intendedNodeId) {
		MessageFormat msg = new MessageFormat(config.getMyId(), AlgorithmType.MST, MSTMsgType.MERGE
				.getMap(MSTMsgType.MSTMsgTypeData.MERGE_ID.toString(), Integer.toString(intendedNodeId)));
		for (int entry : this.treeNeibourSet) {
			if (entry != sendingNode)
				sendEndofRound(entry, msg);
		}
	}

	private void sendAcceptMessage() {
		MessageFormat msg = new MessageFormat(config.getMyId(), AlgorithmType.MST, MSTMsgType.ACCEPT.getMap());
		logger.info(
				"Sending Accept Message to Node" + this.nonTreeNeibourMap.get(mwoe.cost) + " with cost: " + mwoe.cost);
		sendEndofRound(this.nonTreeNeibourMap.get(mwoe.cost), msg);
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
					Assert.state(false, "Search Message recieved but State is not: " + MSTState.MWOE.toString());
				}

				if (this.parent != -1) {
					Assert.state(false, "Parent is already assigned to this node, Parent=" + this.parent);

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
						this.mwoe.cost = null;
						this.mwoe.nodeID = -1;
						sendConvergeCast();
					}
				}

				break;

			case CONVERGECAST:
				if (this.state != MSTState.MWOE) {
					Assert.state(false, "ConvergeCast Message recieved but State is not: " + MSTState.MWOE.toString());

				}

				if (this.numSearchMessagesSent == 0) {
					Assert.state(false, "No Search messages were sent, still converge cast was recieved");
				}

				this.numSearchMessagesSent--;

				MWOE mwoeRecvd = new MWOE();
				mwoeRecvd.get(t.getAlgoData());

				if (this.mwoe.cost == null) {
					this.mwoe.cost = mwoeRecvd.cost;
					this.mwoe.nodeID = mwoeRecvd.nodeID;
				} else if (mwoeRecvd.cost != null && mwoeRecvd.cost.compareTo(this.mwoe.cost) < 0) {
					this.mwoe.cost = mwoeRecvd.cost;
					this.mwoe.nodeID = mwoeRecvd.nodeID;
				}

				if (this.numSearchMessagesSent == 0) {
					// no more replies to be recieved

					// Non Leader Process
					if (this.nonTreeNeibourMap.size() == 0) {
						logger.info("No more non-tree edges, sending convergecast back to parent: " + this.parent);
						sendConvergeCast();
					} else {
						if (mwoe.cost == null || (this.nonTreeNeibourMap.size() > 0 && mwoe.cost != null
								&& this.nonTreeNeibourMap.firstKey().compareTo(mwoe.cost) < 0)) {

							logger.info("Starting to send test messages");
							sendTestMessages();
							this.numTestMessagesSent = this.nonTreeNeibourMap.size();
						} else {
							logger.info(
									"No need to send test messages as nonTreeNeibourMap's min cost is bigger or its empty");
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

				if (this.msgToNeiboursMap.get(t.getMyId()) != null && this.msgToNeiboursMap.get(t.getMyId())
						.getAlgoData().get(MSTMsgTypeData.MSG_TYPE.toString()).equals(MSTMsgType.TEST.toString())) {
					// Handle Situation # 1, Test Message will be overridden by a TestReply Message
					this.msgToNeiboursMap.put(t.getMyId(), null);
					logger.info("Overiding TEST with a TESTREPLYREQUEST");
					MessageFormat msg = new MessageFormat(config.getMyId(), AlgorithmType.MST,
							MSTMsgType.TESTREPLYREQUEST.getMap(MSTMsgType.MSTMsgTypeData.LEADER.toString(),
									Integer.toString(this.leader)));
					this.sendEndofRound(t.getMyId(), msg);
				} else {
					MessageFormat msg = new MessageFormat(config.getMyId(), AlgorithmType.MST, MSTMsgType.TESTREPLY
							.getMap(MSTMsgType.MSTMsgTypeData.LEADER.toString(), Integer.toString(this.leader)));

					sendEndofRound(t.getMyId(), msg);
				}

				break;

			case TESTREPLYREQUEST:
				MessageFormat msg = new MessageFormat(config.getMyId(), AlgorithmType.MST, MSTMsgType.TESTREPLY
						.getMap(MSTMsgType.MSTMsgTypeData.LEADER.toString(), Integer.toString(this.leader)));

				sendEndofRound(t.getMyId(), msg);
			case TESTREPLY:
				String msgType = t.getAlgoData().get(MSTMsgType.MSTMsgTypeData.MSG_TYPE.toString());

				if (this.state != MSTState.MWOE) {
					logger.error(msgType + " recieved but State is not: " + MSTState.MWOE.toString());
					assert (false);
				}

				if (this.numTestMessagesSent == 0) {
					logger.error(msgType + " Message recieved but no test messages were sent");
					assert (false);
				}

				this.numTestMessagesSent--;
				int nodeLeaderId = Integer.parseInt(t.getAlgoData().get(MSTMsgType.MSTMsgTypeData.LEADER.toString()));
				Assert.state(nodeLeaderId != -1, "Leader Id of recieving node should not be is -1");

				if (nodeLeaderId == this.leader) {
					logger.info("Removing node " + t.getMyId() + " from nonTreeEdges");
					this.nonTreeNeibourMap.remove(config.getNeiboursCostmap().get(t.getMyId()));
				} else {
					if (this.mwoe.cost == null || (this.mwoe.cost != null
							&& this.mwoe.cost.compareTo(config.getNeiboursCostmap().get(t.getMyId())) > 0)) {
						this.mwoe.cost = config.getNeiboursCostmap().get(t.getMyId());
						logger.debug("New MWOE being selected, nodeID:" + config.getMyId() + " Cost: " + mwoe.cost);
						this.mwoe.nodeID = config.getMyId();
					}
				}

				logger.info("Current MWOE NodeID:" + this.mwoe.nodeID + " ,Cost:" + this.mwoe.cost);

				if (this.numTestMessagesSent == 0) {
					logger.info("All Replies to Test Messages recieved");
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
				 * Dont do this here, you may get accept from another component that will
				 * interfere with the merge broadcast as the tree would change
				 * 
				 * this.treeNeibourSet.add(t.getMyId());
				 * this.nonTreeNeibourMap.remove(config.getNeiboursCostmap().get(t.getMyId()));
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
					this.sendAcceptMessage(); // accept message goes to nodeThatwasSentAccept
					this.nodeThatwasSentAccept = this.nonTreeNeibourMap.get(mwoe.cost);
					this.acceptSent = true;
					this.treeNeibourSet.add(nodeThatwasSentAccept);
					this.nonTreeNeibourMap.remove(mwoe.cost);
				} else {
					// forward this message intended for the node that is suppose to trigger ACCEPT
					forwardMergeBroadcast(t.getMyId(), targetNodeId);
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
				this.forwardLeaderBroadcast(t.getMyId());
				break;
			default:
				break;

			}
		}

	}

}