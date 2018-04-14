package com.distributed.algorithm.bfs;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import com.distributed.algorithm.util.AlgorithmType;
import com.distributed.algorithm.util.Config;
import com.distributed.algorithm.util.MessageFormat;

@Component
public class BFSCompute {
	@Autowired
	Config config;
	@Autowired
	BFS bfs;

	private int totalRepliesRecvd;
	static Logger logger = LoggerFactory.getLogger(BFSCompute.class);
	private int highestDegree;
	private boolean sendToParent;

	public BFSCompute() {
		totalRepliesRecvd = 0;
		sendToParent = false;
	}

	public void init() {
		highestDegree = bfs.getChildrenMap().size();
		sendMessages();
	}

	private void sendMessages() {
		if (bfs.getChildrenMap().size() < 1) {
			if (!sendToParent) {
				logger.info("This is the only node in the treE?");
				return;
			} else {
				sendConvergeCastMsg();
			}
		}
		MessageFormat msg = new MessageFormat(config.getMyId(), AlgorithmType.BFS, BFSMsgType.COMPUTE.getMap());
		for (int neibour : bfs.getChildrenMap().keySet()) {
			bfs.sendRoundMsg(neibour, msg);
		}
	}

	private void sendConvergeCastMsg() {
		MessageFormat msg = new MessageFormat(config.getMyId(), AlgorithmType.BFS, BFSMsgType.COMPUTE.getMap(String.valueOf(this.highestDegree)));
		bfs.sendRoundMsg(bfs.parent, msg);
	}

	public void processMessage(MessageFormat t) {
		// From Parent
		if (this.bfs.parent == t.getMyId()) {
			sendToParent = true;
			init();
		}
		// Converge Cast
		else if (this.bfs.getChildrenMap().containsKey(t.getMyId())) {
			totalRepliesRecvd++;
			if(this.highestDegree<Integer.parseInt(t.getAlgoData().get("Data"))){
				this.highestDegree=Integer.parseInt(t.getAlgoData().get("Data"));
			}
			if(totalRepliesRecvd== bfs.getChildrenMap().size()) {
				if(sendToParent)
					this.sendConvergeCastMsg();
				else {
					logger.info("BFSCompute Ended,"+" Max Degree:"+this.highestDegree);
				}
			}
		} else {
			Assert.state(false, "Msg from neigther child or Parent but from node:" + t.getMyId());
		}
	}
}
