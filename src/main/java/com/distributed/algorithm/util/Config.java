package com.distributed.algorithm.util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
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
	@Value("${config.file:C://Users//amin//Desktop//Distributed//project1//src//main//resources//config.txt}")
	private String configFile;
	@Value("${node.id:200}")
	private int myId;
	@Value("${max.cached:10}")
	private int maxCached;
	
	private Map<Integer,Integer> neiboursCostmap;
	
	public int getMaxCached() {
		return maxCached;
	}

	@Value("${wait.time.seconds}")
	int startupTime;
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
	
	//MST Data
	
	int numRoundMWOE;
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
		return this.getNumRoundLeaderBroadcast()+this.getNumRoundMerge()+this.getNumRoundMWOE();
	}

	int numRoundMerge;
	int numRoundLeaderBroadcast;

	public Config() {
		nodesMap = new HashMap<>();
		neibours = new HashSet<>();
		neiboursCostmap = new HashMap<>();
		numNodes=0;
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
		
		Map<Integer, String> neibourMap;
		try {

			// br = new BufferedReader(new FileReader(FILENAME));
			fr = new FileReader(configFile);
			br = new BufferedReader(fr);

			String line;
			int totalNodes=-1;
			
			while ((line = br.readLine()) != null) {
				String trimmedLine= line.trim();
				if(trimmedLine.length()==0 || trimmedLine.startsWith("#"))
					continue;
				
				if(totalNodes<0) {
					totalNodes=Integer.parseInt(trimmedLine);
					numNodes=totalNodes;
					continue;
				}
				
				if(totalNodes!=0) {
					totalNodes--;
					String[] nodes =trimmedLine.split(" +");
					if(nodes.length!=3) throw new IOException("Invalid File");
					if(!(Integer.parseInt(nodes[0])==myId)) {
						nodesMap.put(Integer.parseInt(nodes[0]),nodes[1].concat(":").concat(nodes[2]));
					}
					continue;
				}
				
				
				if(Integer.parseInt(trimmedLine.substring(0, trimmedLine.indexOf(' ')))==myId) {
					logger.info("Found MyId");
					String[] neibours =trimmedLine.split(" +");
					for(String neibour:neibours) {
						if(Integer.parseInt(neibour)==myId) {
							continue;
						}
						else {
							this.neibours.add(Integer.parseInt(neibour));
						}
					}
				}
				else
					continue;	
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
		
		this.numRoundMWOE=numNodes*2;
		this.numRoundMerge=numNodes+1;
		this.numRoundLeaderBroadcast=numNodes;

	}
	
	private void debug() {
		logger.info("MyId "+ this.myId);
		logger.info("My Neibours:");
		
		for(int neibour:neibours) {
			logger.info(neibour+": "+nodesMap.get(neibour) );
		}
	}
}
