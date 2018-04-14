package com.distributed.algorithm.util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.PostConstruct;

import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.distributed.algorithm.Project1Application;

import org.slf4j.Logger;

@Component
public class Config {

	static Logger logger = LoggerFactory.getLogger(Config.class);
	@Value("${server.port:2281}")
	private String port;
	@Value("${config.file:C://Users//amin//Desktop//Distributed//DistributedAlgorithm//src//main//resources//config.txt}")
	private String configFile;
	@Value("${node.id:184}")
	private int myId;
	@Value("${max.cached:10}")
	private int maxCached;

	private Map<Integer, Integer> neiboursCostmap;

	public int getMaxCached() {
		return maxCached;
	}

	@Value("${wait.time.seconds}")
	int startupTime;

	// Either new(mst, by default) or old
	@Value("${file.format:new}")
	String fileFormat;

	public int getMyId() {
		return myId;
	}

	public int getStartupTime() {
		return this.startupTime;
	}

	public void setMyId(int myId) {
		this.myId = myId;
	}

	public int getNumNodes() {
		return numNodes;
	}

	public void setNumNodes(int numNodes) {
		this.numNodes = numNodes;
	}

	public Map<Integer, String> getNodesMap() {
		return nodesMap;
	}

	public void setNodesMap(Map<Integer, String> nodesMap) {
		this.nodesMap = nodesMap;
	}

	public Set<Integer> getNeibours() {
		return neibours;
	}

	public void setNeibours(Set<Integer> neibours) {
		this.neibours = neibours;
	}

	private int numNodes;

	private Map<Integer, String> nodesMap;
	private Set<Integer> neibours;

	// MST Data

	//Includes Max time to broadcast Search,covergecast and test messages=(4n)
	int numRoundMWOE;
	
	//Included time for leader to send merge request to node and the actual merge (n+1)
	int numRoundMerge;
	
	//Include time a new leader takes to broadcast itself as the leader(n)
	int numRoundLeaderBroadcast;

	public int getNumRoundMWOE() {
		return numRoundMWOE;
	}

	public void setNumRoundMWOE(int numRoundMWOE) {
		this.numRoundMWOE = numRoundMWOE;
	}

	public int getNumRoundMerge() {
		return numRoundMerge;
	}

	public void setNumRoundMerge(int numRoundMerge) {
		this.numRoundMerge = numRoundMerge;
	}

	public int getNumRoundLeaderBroadcast() {
		return numRoundLeaderBroadcast;
	}

	public void setNumRoundLeaderBroadcast(int numRoundLeaderBroadcast) {
		this.numRoundLeaderBroadcast = numRoundLeaderBroadcast;
	}

	public int getPhaseSize() {
		return this.getNumRoundLeaderBroadcast() + this.getNumRoundMerge() + this.getNumRoundMWOE();
	}

	public Config() {
		nodesMap = new HashMap<>();
		neibours = new HashSet<>();
		neiboursCostmap = new HashMap<>();
		numNodes = 0;
	}

	public Map<Integer, Integer> getNeiboursCostmap() {
		return neiboursCostmap;
	}

	public void setNeiboursCostmap(Map<Integer, Integer> neiboursCostmap) {
		this.neiboursCostmap = neiboursCostmap;
	}

	@PostConstruct
	public void init() throws IOException, URISyntaxException {

		BufferedReader br = null;
		FileReader fr = null;
		
		logger.info("Started Readign File");
		Map<Integer, String> neibourMap;
		try {

			// br = new BufferedReader(new FileReader(FILENAME));
			fr = new FileReader(configFile);
			br = new BufferedReader(fr);

			String line;
			int totalNodes = -1;

			while ((line = br.readLine()) != null ) {
				String trimmedLine = line.trim();
				if (trimmedLine.length() == 0 || trimmedLine.startsWith("#"))
					continue;

				// Reading No. of Nodes
				if (totalNodes < 0) {
					totalNodes = Integer.parseInt(trimmedLine);
					numNodes = totalNodes;
					continue;
				}

				// Reading Hostname and Port of Nodes
				if (totalNodes != 0) {
					totalNodes--;
					String[] nodes = trimmedLine.split(" +");
					if (nodes.length != 3)
						throw new IOException("Invalid File");
					if (!(Integer.parseInt(nodes[0]) == myId)) {
						nodesMap.put(Integer.parseInt(nodes[0]), nodes[1].concat(":").concat(nodes[2]));
					}
					continue;
				}

				// Reading the Neibours and/or Edges
				if (this.fileFormat.equalsIgnoreCase("old")) {
					if (Integer.parseInt(trimmedLine.substring(0, trimmedLine.indexOf(' '))) == myId) {
						logger.info("Found MyId");
						String[] neibours = trimmedLine.split(" +");
						for (String neibour : neibours) {
							if (Integer.parseInt(neibour) == myId) {
								continue;
							} else {
								this.neibours.add(Integer.parseInt(neibour));
							}
						}
					} else
						continue;
				} else if (fileFormat.equalsIgnoreCase("new")) {
					String[] lineData = trimmedLine.split(" +");
					String edgeInfo = lineData[0];
					String cost = lineData[1];
					String[] nodes = edgeInfo.replace("(", "").replace(")","").split(",");
					logger.info(nodes[0].trim() + "-->" + nodes[1].trim() + "=" + cost);
					if (myId == Integer.parseInt(nodes[0].trim()) || myId == Integer.parseInt(nodes[1].trim()) ) {
						int idToPut=myId == Integer.parseInt(nodes[0].trim())?Integer.parseInt(nodes[1].trim()):Integer.parseInt(nodes[0].trim());
						this.neiboursCostmap.put(idToPut, Integer.parseInt(cost));
					}
				}
			}

			debug();

		} catch (IOException e) {

			e.printStackTrace();

		} finally {

			try {

				if (br != null)
					br.close();

				if (fr != null)
					fr.close();

			} catch (IOException ex) {

				ex.printStackTrace();

			}
		}

		this.numRoundMWOE = numNodes * 4;
		this.numRoundMerge = numNodes + 1;
		this.numRoundLeaderBroadcast = numNodes;

	}

	private void debug() {
		logger.info("MyId " + this.myId);
		logger.info("My Neibours:");
		
		if (this.fileFormat.equals("old")) {
		
			for (int neibour : neibours) {
				logger.info(neibour + ": " + nodesMap.get(neibour));
			}
		}
		
		else if (this.fileFormat.equalsIgnoreCase("new")) {
			for (Entry<Integer, Integer> edge : this.neiboursCostmap.entrySet()) {
				logger.info(myId+"-->"+edge.getKey()+"="+edge.getValue());
			}
		}
	}
}
