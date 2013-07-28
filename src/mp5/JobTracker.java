package mp5;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import mp3.Helper;
import mp3.NodeInfo;
import mp4.FileUtils;
import mp4.SdfsMessageHandler;
import mp4.SdfsNode;

public class JobTracker {
	
	private SdfsNode node;
	private HashMap<String,String> taskToStatusMap;
	private HashMap<String,TaskHearbeat> taskToHeartbeatMap;
	public volatile boolean completed;
	public volatile boolean jobRunning;
	private HashSet<String> taskNodes;
	
	/**
	 * @param node
	 */
	public JobTracker(SdfsNode node){
		this.node = node;
		taskToStatusMap = new HashMap<String,String>();
	}
	
	/**
	 * @param exe
	 * @param prefix
	 * @param sdfsFiles
	 */
	public void startMapleJob(String exe,String prefix,ArrayList<String> sdfsFiles){
		jobRunning = true;
		taskNodes = new HashSet<String>();
		taskToHeartbeatMap = new HashMap<String,TaskHearbeat>();
		Random rd = new Random();
		NodeInfo currentNodeInfo = Helper.extractNodeInfoFromId(node.getNodeId());
		int taskId = 1;
		
		//1- first locate node which have the data
		for(String file: sdfsFiles){
			
			if (!node.fileMetadata.keySet().contains(file)){
				System.out.println(String.format("%s not found in SDFS system",file));
				continue;
			}
			
			HashMap<Integer,Set<String>> blockLocations = FileUtils.getSdfsFileBlockLocations(node,file);
			
			//for each block of a file, run the map task
			for(int blockId:blockLocations.keySet()){
				
				ArrayList<String> locations = new ArrayList<String>(blockLocations.get(blockId));
				String candidateNodeId = locations.get(rd.nextInt(blockLocations.size()));	
				
				if (!taskNodes.contains(candidateNodeId)){
					//send program to node
					NodeInfo candidateNodeInfo = Helper.extractNodeInfoFromId(candidateNodeId);
					String configPath = FileUtils.getConfigStorageFolder(currentNodeInfo);
					String programPath = String.format("%s/%s", configPath,exe);
					
					File prog = new File(programPath);
					if (prog.exists()){
						FileUtils.uploadProgram(prog,
								candidateNodeInfo.getHostname(), 
								candidateNodeInfo.getPort());
						
						taskNodes.add(candidateNodeId);
					}		
				}
				
				String sdfsFileName = String.format("%s.part%d",file, blockId);
				
				String message = String.format("%s:%s:%s:%s:%s", SdfsMessageHandler.MAPLE_PREFIX,taskId, exe,prefix,sdfsFileName);
				
				assignTaskToNode(candidateNodeId,message,SdfsMessageHandler.MAPLE,taskId);
				
				taskId+=1;
			}
		}
	}
	
	/**
	 * @param exe
	 * @param numberOfJuices
	 * @param prefix
	 * @param destinationSdfs
	 */
	public void startJuiceJob(String exe,int numberOfJuices, String prefix, String destinationSdfs){
		jobRunning = true;
		
		taskToHeartbeatMap = new HashMap<String,TaskHearbeat>();
		int taskId = 0;
		
		Random rd = new Random();
		
	    HashMap<String,String> fileKeysToJuiceNodeMap  = new HashMap<String,String>();
		
		//identify nodes which hold files with a particular prefix have a given prefix
		//and do not currently run a maple or juice task 
		
		synchronized(node.fileMetadata){
			
			ArrayList<String> targetSdfsFiles = new ArrayList<String>();
			
			for(String sdfsFile:node.fileMetadata.keySet()){
				if (sdfsFile.startsWith(prefix)){
					targetSdfsFiles.add(sdfsFile);
				}
			}
			
			for(String sdfsFile:targetSdfsFiles){
				
				HashMap<Integer,Set<String>> blockLocations = FileUtils.getSdfsFileBlockLocations(node,sdfsFile);
				
				if (blockLocations == null)
					continue;
				
				
				for(int blockId:blockLocations.keySet()){
								
					ArrayList<String> locations = new ArrayList<String>(blockLocations.get(blockId));				
					String candidateNodeId = locations.get(rd.nextInt(blockLocations.size()));				
								
					String message = String.format("%s:%s:%s:%s:%s", SdfsMessageHandler.JUICE_PREFIX,
							taskId, exe, sdfsFile, destinationSdfs);
					
					assignTaskToNode(candidateNodeId,message,SdfsMessageHandler.JUICE,taskId);
					
					taskId+=1;
				}
			}
		}
	}

	/**
	 * @param nodeId
	 * @param status
	 */
	public void notifyTaskProgress(String nodeId,int taskId,String taskType, String status) {
		 synchronized (taskToStatusMap) {
			
			String taskName = String.format("%s_%d",taskType, taskId);
					
			if (taskToStatusMap.containsKey(taskName)){
				taskToStatusMap.put(taskName, status);
			}		
			
			//TODO: send task progress to client ??
		}
	}
	
	/**
	 * @param nodeId
	 * @param message
	 */
	public void assignTaskToNode(String nodeId,String message,String taskType,int taskId){
		NodeInfo nodeInfo = Helper.extractNodeInfoFromId(nodeId);
		Helper.sendUnicastMessage(node.getSocket(), message, nodeInfo.getHostname(), nodeInfo.getPort());	
		
		//add task to heartbeat monitor list.
		String taskName = String.format("%s_%d",taskType,taskId);
		
		synchronized(taskToHeartbeatMap){
			if(!taskToHeartbeatMap.containsKey(taskName)){
				taskToHeartbeatMap.put(taskName, new TaskHearbeat(taskId, taskType, this));
			}
		}
	}
	
	public void notifyFailedTask(int taskId, String taskType) { 
		//restart task
		synchronized(taskToHeartbeatMap){
			String taskName = String.format("%s_%d",taskType,taskId);
			
			if(taskToHeartbeatMap.containsKey(taskName)){
				taskToHeartbeatMap.remove(taskName);
			}	
		}
		
		//restart the failed task
	}
	
	public static void main(String[] args){
		
	}


}