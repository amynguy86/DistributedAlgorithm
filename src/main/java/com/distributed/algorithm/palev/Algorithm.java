package com.distributed.algorithm.palev;



import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.MessagingException;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import com.distributed.algorithm.Project1Application;
import com.distributed.algorithm.Project1Application.ToTCP;
import com.distributed.algorithm.bfs.BFS;
import com.distributed.algorithm.util.AlgorithmType;
import com.distributed.algorithm.util.Config;
import com.distributed.algorithm.util.MessageFormat;
import com.distributed.algorithm.util.Synchronizer;

@Component
public class Algorithm implements Consumer<MessageFormat> {

	@Autowired
	Config config;

	@Autowired
	public Synchronizer synchronizer;

	int currentRound;
	@Autowired
	ToTCP toTcp;
	
	@Autowired
	BFS bfs;

	int maxAttempts;
	int leaderUnchanged;
	Map<String, String> AlgoData;

	int leader;
	int d;
	
	boolean leaderFound;

	// What my neibours sent me
	int maxLeader;
	int leaderD;

	static Logger logger = LoggerFactory.getLogger(Algorithm.class);

	public Algorithm() {
		currentRound = 0;
		maxAttempts = 2;
		AlgoData = new HashMap<>();
		leaderUnchanged = 0;
		maxLeader = -1;
		leaderD =-1;
		leaderFound=false;
	}

	public void init() throws InterruptedException {
		synchronizer.registerConsumer(AlgorithmType.PALEV,this);
		logger.debug("Wating for "+config.getStartupTime()+" seconds to startup");
		TimeUnit.SECONDS.sleep(config.getStartupTime());
		logger.debug("Initial Period Up");
		leader = config.getMyId();
		d = 0;
		this.sendMsgToAllNeibours(0, leader, d);
	}

	private void sendMsgToAllNeibours(int round, int leader, int d) {

		MessageFormat msg = new MessageFormat(config.getMyId(), AlgorithmType.PALEV);
		msg.setRoundNo(round);
		AlgoData.put("x", String.valueOf(leader));
		AlgoData.put("d", String.valueOf(d));
		msg.setAlgoData(AlgoData);
		for (Integer neigbour : config.getNeibours()) {
			for (int i = 1; i <= this.maxAttempts; i++) {
				try {
					logger.info("Sending Message: "+msg.toString());
					toTcp.send(MessageFormat.buildMessage(msg), config.getNodesMap().get(neigbour));
					i = maxAttempts;
				} catch (MessagingException ex) {
					logger.error("Failed to connect to " + config.getNodesMap().get(neigbour) + " " + ex.getMessage());

					Assert.state(i != maxAttempts, "Lost Connection, Max Attempts reached");

					logger.info("Trying Again");
					try {
						TimeUnit.SECONDS.sleep(2);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		}
	}

	// Will be Called from Another Thread
	@Override
	public void accept(MessageFormat t) {
		// TODO Auto-generated method stub
		if (t == null) {
			logger.info("End of Round: " + currentRound);
			processEndOfRound();
		} else if (t.getAlgorithmType() == AlgorithmType.PALEV) {
			processMsg(t.getAlgoData());
			logger.info(t.toString());
		}
	}

	public void processEndOfRound() {
		currentRound++;
		
		if(leader<this.maxLeader) {
			leader=this.maxLeader;
			this.d=this.leaderD+1;
			leaderUnchanged=0;
		}
		else if(leader==this.maxLeader) {
			if(this.d<leaderD) {
				this.d=leaderD;
				leaderUnchanged=0;
			}
			else
				leaderUnchanged++;
		}
		else {
			leaderUnchanged++;
		}
		
		//reset Values
		maxLeader=-1;
		leaderD=-1;
		
		if(leaderUnchanged==3 && this.config.getMyId()==leader) {
			logger.info("Leader Elected:"+this.leader+" and D:"+this.d);
			leaderFound=true;
			//start the BFS Now
			this.synchronizer.UnRegisterConsumer(AlgorithmType.PALEV);
			try {
				bfs.buildBFS();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		else {
		this.sendMsgToAllNeibours(currentRound,leader,d);
		}
	}

	public void processMsg(Map<String, String> algoData) {
		int neibourId = Integer.parseInt(algoData.get("x"));
		int neibourD = Integer.parseInt(algoData.get("d"));

		if (neibourId > this.maxLeader) {
			this.maxLeader = neibourId;
			this.leaderD = neibourD;
		} else if (neibourId == this.maxLeader) {
			if (leaderD < neibourD)
				leaderD = neibourD;
		}
	}
}
