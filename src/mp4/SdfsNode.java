package mp4;
import java.io.File;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket; 
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap; 
import java.util.HashSet; 
import java.util.Random;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import mp3.ClientNode;
import mp3.Helper;
import mp3.MessageHandler;
import mp3.NodeInfo;
import mp5.JobTracker;
import mp5.TaskTracker;

public class SdfsNode extends ClientNode {

	private Boolean isMaster = false;
	private ServerSocket tcpSocket; 
	private String masterNodeId = "";
	 
	//Holds a mapping of a node to a map of sdfs files with the specific blocks they hold
	public HashMap<String,HashMap<String,Set<Integer>>> sdfsMetadata;
	
	//Holds file to block count information
	public HashMap<String,Integer> fileMetadata;
	
	private Timer replicationTimer;
	private volatile boolean replicationOngoing;
	
	private JobTracker jobTracker;
	public volatile boolean jobRunning;
	
	/**
	 * @param hostname
	 * @param port
	 */
	public SdfsNode(String hostname, int port) {
		this(hostname, port, false);
	}

	/**
	 * @param hostname
	 * @param port
	 * @param master
	 */
	public SdfsNode(String hostname, int port, boolean master) {
		this(hostname, port, "localhost", 10000, master);
	}

	/**
	 * @param hostname
	 * @param port
	 * @param master
	 */
	public SdfsNode(String hostname, int port, boolean master, int lifespan,
			boolean crash) {
		this(hostname, port, "localhost", 10000, master, lifespan, crash);
	}

	/**
	 * @param hostname
	 * @param port
	 * @param introducerHost
	 * @param introducerPort
	 * @param master
	 */
	public SdfsNode(String hostname, int port, String introducerHost,
			int introducerPort, boolean master) {
		this(hostname, port, introducerHost, introducerPort, master, 0, false);
	}

	/**
	 * @param hostname
	 * @param port
	 * @param introducerHost
	 * @param introducerPort
	 * @param master
	 * @param lifespan
	 * @param crash
	 */
	public SdfsNode(String hostname, int port, String introducerHost,
			int introducerPort, boolean master, int lifespan, boolean crash) {
		super(hostname, port, lifespan, crash, introducerHost, introducerPort,
				0);

		this.isMaster = master;

		if (this.isMaster) {
			masterNodeId = this.clientId;
		}
		
		NodeInfo currentNodeInfo = Helper.extractNodeInfoFromId(this.getNodeId());
		
		File storageFolder = new File(FileUtils.getStoragePath(currentNodeInfo));
		
		if (storageFolder.exists()){
			FileUtils.deleteDirectory(storageFolder);
		}

		sdfsMetadata = new HashMap<String, HashMap<String,Set<Integer>>>();
		fileMetadata = new HashMap<String,Integer>();

		super.initialize();

		startAdvancedConnectionManager();
	}

	@Override
	public void initialize() {

	}

	/**
	 * 
	 * @return
	 */
	public boolean getIsMaster() {
		return this.isMaster;
	}

	/**
	 * @return Gets the master node of the SDFS
	 */
	public String getMasterId() {
		return this.masterNodeId;
	}

	/**
	 * @param masterId
	 */
	public synchronized void setMasterId(String masterId) {
		this.masterNodeId = masterId;
		
		if (this.masterNodeId.equals(getNodeId())){
			this.isMaster = true;
		}else{
			this.isMaster = false;
		}		
	}
	
	/**
	 * 
	 */
	public void listenToTcpConnections() {
		while (true) {
			try {
				Socket socket = tcpSocket.accept();
				new AdvancedTcpManager(socket, this);
			} catch (IOException e) {
				// e.printStackTrace();
				System.out
						.println("Unable to accept to the incoming connection request");
			}
		}
	}

	/**
	 * Performs any necessary initialization
	 */
	public void initializeSdfsNode() { 
		if (getIsMaster()){
			
		}
	}

	@Override
	public void notifyUnreachable() {
		super.notifyUnreachable();

		if (getTrackedNodeId().equals(getMasterId())) {

			// start election with a coordinate message (ring election)
			String message = String.format("%s:%s",
					SdfsMessageHandler.COORD_MESSAGE_PREFIX, getNodeId());
			sendBMulticastMessage(message);
			log(String.format("Coord message for new master <%s>", getNodeId()));
		}
	}

	@Override
	public MessageHandler getMessageHandler(DatagramSocket socket,
			DatagramPacket packet, ClientNode node) {
		return new SdfsMessageHandler(socket, packet, (SdfsNode) node);
	}

	
	/**
	 * @param sdfsFileName
	 * @param chunkIndex
	 * 
	 * save block and broadcast the saved block to the rest of the system
	 * @param chunkCount 
	 */
	public synchronized void addBlockInfo(String nodeId, String sdfsFileName, int chunkIndex, int chunkCount) {

		HashMap<String,Set<Integer>> fileToStoredBlocks;
		
		if (!fileMetadata.containsKey(sdfsFileName)){
			fileMetadata.put(sdfsFileName, chunkCount);
		}
		
		if (!sdfsMetadata.containsKey(nodeId)){
			fileToStoredBlocks = new HashMap<String,Set<Integer>>();
			sdfsMetadata.put(nodeId,fileToStoredBlocks);
		}else{
			fileToStoredBlocks = sdfsMetadata.get(nodeId);
		}
			
		if (fileToStoredBlocks.containsKey(sdfsFileName)){
			Set<Integer> blocks = fileToStoredBlocks.get(sdfsFileName);
			blocks.add(chunkIndex);
		}else{
			Set<Integer> blocks = new HashSet<Integer>();
			blocks.add(chunkIndex);
			fileToStoredBlocks.put(sdfsFileName, blocks);
		}
		
		NodeInfo nodeInfo = Helper.extractNodeInfoFromId(nodeId);
		
		log(String.format("Saving metadata block info for file %s and chunk %d from <%s:%d>", 
				sdfsFileName,
				chunkIndex,
				nodeInfo.getHostname(),
				nodeInfo.getPort()));
		
		runReplicationTask();
	}
		
	/**
	 * Run task replication
	 */
	public void runReplicationTask(){
		if (this.getIsMaster() && !replicationOngoing){
			
			if (replicationTimer!=null)
				replicationTimer.cancel();
			
			replicationTimer = new Timer();
			
			replicationTimer.schedule(new TimerTask(){
				public void run(){
					performFileReplication();
				}
			}, 5000);
		}
	}
	
	/**
	 * @param sdfsFileName
	 */
	public synchronized void deleteSdfsFile(String sdfsFileName){
		synchronized(sdfsMetadata){	
			HashMap<String, Set<Integer>> fileToBlockMap = sdfsMetadata.get(getNodeId()); 
			
			if (fileToBlockMap!=null && 
				fileToBlockMap.containsKey(sdfsFileName)){
				
				NodeInfo nodeInfo = Helper.extractNodeInfoFromId(getNodeId());
						
				for(int chunkIndex:fileToBlockMap.get(sdfsFileName)){
					
					String fileName = String.format("%s.part%d",sdfsFileName,chunkIndex);
				    String filePath = String.format("%s/%s",FileUtils.getStoragePath(nodeInfo), fileName);
				    
					FileUtils.deleteFile(filePath);
					
					log(String.format("Deleted chunk %d of sdfs file [%s]",chunkIndex,sdfsFileName));
				}
				
				deleteSdfsFileMetadata(sdfsFileName);
				
				//broadcast delete to other nodes in the membership
				
				String deleteMetadataMessage = String.format("%s:%s", SdfsMessageHandler.DELETE_FILE_PREFIX,getNodeId());
				
				synchronized(membershipList){
					
					ArrayList<String> recipients = new ArrayList<String>();
					
					for(String id:membershipList){
						if (id.equals(getNodeId()))
							recipients.add(id);
					}
					
					Helper.sendBMulticastMessage(getSocket(), deleteMetadataMessage, recipients);
				}
			}	
		}
	}
	
	/**
	 * @param nodeId
	 * 
	 * Removes a crashed or leaving node from SDFS metadata
	 */
	public synchronized void removeNodeFromMetadata(String nodeId){
		synchronized(sdfsMetadata){
			if (sdfsMetadata.containsKey(nodeId)){
				sdfsMetadata.remove(nodeId);
			}
		}
	}
	
	public synchronized void deleteSdfsFileMetadata(String sdfsFileName) {
		if (fileMetadata!=null && 
			fileMetadata.containsKey(sdfsFileName)){
			
			fileMetadata.remove(sdfsFileName);
		}

		//remove any reference to this file in sdfsMetadata	
		synchronized(sdfsMetadata){
			for(String nodeId:sdfsMetadata.keySet()){
				
				HashMap<String, Set<Integer>> fileToBlockMap = sdfsMetadata.get(nodeId);
				
				if (fileToBlockMap.containsKey(sdfsFileName)){
					fileToBlockMap.remove(sdfsFileName);
				}
			}
		}
	}
	
	/**
	 * Performs a replication process
	 */
	public synchronized void performFileReplication(){
		
		log("Starting replication...");
		
		replicationOngoing = true;
		
		//The replication process looks at a file block and decides where to send it.
		//The destination node should not already have the part, so a filtering of nodes
		//with a particular block should be identified.
		
		synchronized(sdfsMetadata){
			
			 HashMap<String,HashMap<Integer,Integer>> fileToPartReplicaCount = 
					 new HashMap<String,HashMap<Integer,Integer>>() ;
			 
			 HashMap<String,HashMap<Integer,Set<String>>> fileToBlockHolders = 
					 new HashMap<String,HashMap<Integer,Set<String>>>();
			 
			 HashMap<String,HashMap<Integer,Integer>> fileToBlockReplicaNeeded = 
					 new HashMap<String,HashMap<Integer,Integer>>();
			
			 //1 - Book keeping
			 //Compute a report of File to block replica count
			 //We go through each node and count occurences of a
			 //block for the file. We then decide whether or not to
			 //replicate a block based on how many replicas the block holds.
			 //We can therefore replicate missing blocks in case of failure of a node
			 for(String nodeId:sdfsMetadata.keySet()){
				 
				 HashMap<String,Set<Integer>> fileBlocks = sdfsMetadata.get(nodeId);	 

				 for(String file:fileBlocks.keySet()){
					 
					 HashMap<Integer,Integer> counterMap;
					 		 
					 if (!fileToPartReplicaCount.containsKey(file)){
						 counterMap =  new HashMap<Integer,Integer>();
						 
						 int blockCount = fileMetadata.get(file);
						 
						 for(int k=0; k < blockCount; k ++){
							 counterMap.put(k, 0);
						 }
						 
						 fileToPartReplicaCount.put(file, counterMap);
						 
					 }else {
						 counterMap = fileToPartReplicaCount.get(file);
					 }
					 
					 HashMap<Integer,Set<String>> blockHolders;
					 
					 if (!fileToBlockHolders.containsKey(file)){
						 blockHolders = new HashMap<Integer,Set<String>>();
						 fileToBlockHolders.put(file, blockHolders);
					 }else{
						 blockHolders = fileToBlockHolders.get(file);
					 }
					  
					 for(int blockIndex:fileBlocks.get(file)){
						 if(counterMap.containsKey(blockIndex)){
							 int val = counterMap.get(blockIndex);
							 counterMap.put(blockIndex, val+1);
						 }else{
							 counterMap.put(blockIndex, 1);
						 }
						 
						 Set<String> holders ;
						 
						 if (blockHolders.containsKey(blockIndex)){
							 holders = blockHolders.get(blockIndex) ;					
						 }else{
							 holders = new HashSet<String>(); 
						 }
						 
						 holders.add(nodeId);
						 blockHolders.put(blockIndex, holders);
					 }	
				 }
			 }
			 
			 //go through the replica count and keep track of what to replicate
			 for(String file:fileToPartReplicaCount.keySet()){
				 HashMap<Integer,Integer> map = fileToPartReplicaCount.get(file);
				 
				 for(int blockIndex:map.keySet()){
					 int numReplicas = map.get(blockIndex) ;
					 if (numReplicas < FileUtils.NUMBER_OF_REPLICAS){
						 
						 HashMap<Integer,Integer> neededReplicaMap ;
						 
						 if (fileToBlockReplicaNeeded.containsKey(file)){
							 neededReplicaMap = fileToBlockReplicaNeeded.get(file);
						 }else{
							 neededReplicaMap  = new HashMap<Integer,Integer>(); 
							 fileToBlockReplicaNeeded.put(file, neededReplicaMap);
						 }
						 
						 neededReplicaMap.put(blockIndex, FileUtils.NUMBER_OF_REPLICAS - numReplicas);			 
					 }
				 }
			 }
			 
			 Set<String> fileInNeedForReplication = new HashSet<String>();
 
			 //replicate flagged blocks
			 for(String file:fileToBlockReplicaNeeded.keySet()){
				 
				 if (fileInNeedForReplication.contains(file))
					 return;
				 
				 fileInNeedForReplication.add(file);
				 
				 HashMap<Integer,Integer> replicationMap = fileToBlockReplicaNeeded.get(file);
				 
				 HashMap<Integer,Set<String>> blockHoldersMap = fileToBlockHolders.get(file);
				 
				 for(int blockIndex:replicationMap.keySet()){
					 int repCount = replicationMap.get(blockIndex);
					 Set<String> exceptList = blockHoldersMap.get(blockIndex);
					 ArrayList<String> candidates = getReplicationDestination(repCount,exceptList);

					 //actual replication
					 if (exceptList == null) 
						 exceptList =  new HashSet<String>();
					 
					 replicate(file,blockIndex,candidates,new ArrayList<String>(exceptList));
				 }		 
			 }
		}
		
		replicationOngoing = false;
	}
	
	/**
	 * @param sdfsFileName
	 * @param blockIndex
	 * @param destinations
	 * @param sources
	 * 
	 * Initiate a replication unicast
	 */
	private void replicate(String sdfsFileName,int blockIndex,ArrayList<String> destinations,ArrayList<String> sources){
		
		Random rd = new Random();
		
		for(String destinationId:destinations){
			
			 System.out.println(String.format("Replication of block %d of file %s assigned to [%s]",
					 blockIndex,sdfsFileName,Helper.join(destinations)));
			 
			 String sourceId = sources.get(rd.nextInt(sources.size()));
			 
//			 int numBlocks = fileMetadata.get(sdfsFileName);
//			 
//			 String message = String.format("%s:%s:%s:%d:%d", SdfsMessageHandler.REPLICATE_BLOCK_PREFIX,
//					 									destinationId,
//					 									sdfsFileName,
//					 									blockIndex,
//					 									numBlocks);
//			 
//			 String sourceId = sources.get(rd.nextInt(sources.size()));
//			 
//			 NodeInfo destinationInfo = Helper.extractNodeInfoFromId(sourceId);
//			 
//			 Helper.sendUnicastMessage(getSocket(), message, 
//					 destinationInfo.getHostname(), destinationInfo.getPort());
			 
			 transferBlock(sourceId,destinationId,sdfsFileName,blockIndex);
		}
	}
	
	/**
	 * Transfers a block from one node to another
	 * 
	 * @param sourceId
	 * @param destinationId
	 * @param sdfsFileName
	 * @param blockIndex
	 */
	public void transferBlock(String sourceId,String destinationId,String sdfsFileName,int blockIndex){
		synchronized(fileMetadata){
			int numBlocks = fileMetadata.get(sdfsFileName);
			
			String message = String.format("%s:%s:%s:%d:%d", SdfsMessageHandler.REPLICATE_BLOCK_PREFIX,
						destinationId,
						sdfsFileName,
						blockIndex,
						numBlocks);
			
			NodeInfo sourceInfo = Helper.extractNodeInfoFromId(sourceId);
			NodeInfo destinationInfo = Helper.extractNodeInfoFromId(destinationId);
			 
			Helper.sendUnicastMessage(getSocket(), message, 
					sourceInfo.getHostname(), sourceInfo.getPort());
			
			log(String.format("Transfering block from <%s:%d> to <%s:%d>", 
					sourceInfo.getHostname(),
					sourceInfo.getPort(),
					destinationInfo.getHostname(),
					destinationInfo.getPort()));
		}
	}
	
	/**
	 * @param numberOfCandidates
	 * @param excludedList
	 * @return
	 * 
	 * Gets candidates for shipping of blocks for replication
	 */
	private ArrayList<String> getReplicationDestination(int numberOfCandidates, Set<String> excludedList){
		ArrayList<String> candidates = new ArrayList<String>();
		Random rd = new Random();
		
		synchronized(membershipList){
			
			ArrayList<String> available = new ArrayList<String>();
			
			for(String nodeId: membershipList){
				if (excludedList!=null &&
					excludedList.contains(nodeId))
					continue;
				
				available.add(nodeId);
			}	
			
			for(int i =0; i< numberOfCandidates; i++){
				
				int choice = rd.nextInt(available.size());
				candidates.add(available.get(choice));			
				available.remove(choice);
			}
		}
		
		return candidates;
	}

	/**
	 * 
	 */
	private void startAdvancedConnectionManager() {
		try {
			tcpSocket = new ServerSocket(getNodePortNumber());
			listenToTcpConnections();
		} catch (IOException e) { 
			e.printStackTrace();
		}
	}
	
	@Override
	public void startLogQuerierServer(final String logname){
		
	}
	
	/**
	 * @param exe
	 * @param prefix
	 * @param sdfsFiles
	 * 
	 * Initializes maple job for execution across nodes, tracks progress and restart tasks if necessary.
	 * It also notifies the front-end of job progress.
	 */
	public void initializeMapleJob(final String exe,final String prefix,final ArrayList<String> sdfsFiles){
		
		if (!getIsMaster())
			return;
		
		if (!jobRunning)
			jobRunning = true;
		
		jobTracker = new JobTracker(this);
		
		(new Thread(){
			@Override
			public void run(){	
				jobTracker.startMapleJob(exe, prefix, sdfsFiles);
			}
		}).start();
	}
	
	/**
	 * @param exe
	 * @param inputFile
	 * @param outputFile
	 * 
	 * executes a maple task
	 */
	public void doMapleTask(int taskId,String exe, String prefix, String sdfsFileName){
		TaskTracker tracker = new TaskTracker(this,taskId); 
		tracker.startMapleJob(exe, prefix, sdfsFileName);
	}
	
	/**
	 * @param exe
	 * @param numberOfJuices
	 * @param prefix
	 * @param destinationSdfs
	 * 
	 * initializes Juice Job for execution across nodes, track progress and restart tasks if necessary.
	 */
	public void initializeJuiceJob(final String exe,final int numberOfJuices,
			final String prefix,final String destinationSdfs) {
		
		if (!getIsMaster())
			return;

		jobTracker = new JobTracker(this);
		
		(new Thread(){
			@Override
			public void run(){		
				jobTracker.startJuiceJob(exe, numberOfJuices, prefix, destinationSdfs);
			}
		}).start();
	}
	
	/**
	 * @param exe
	 * @param inputFile
	 * @param outputFile
	 * 
	 * executes a juice task
	 */
	public void doJuiceTask(int taskId,String exe, String sourceFile,String destinationFile){		
		TaskTracker tracker = new TaskTracker(this,taskId); 
		try {
			tracker.startJuiceJob(exe, sourceFile, destinationFile);
		} catch (IOException e) {
			 e.printStackTrace();
		}
	}
	
	/**
	 * Report task progress to the master node
	 * @param taskType
	 */
	public void reportTaskProgress(int taskId,String taskType,String status) {		
		String message = String.format("%s:%s:%d:%s:%s", SdfsMessageHandler.TASK_REPORT_PREFIX,
				getNodeId(),taskId,taskType, status);
		
		NodeInfo masterNode = Helper.extractNodeInfoFromId(getMasterId());
		
		Helper.sendUnicastMessage(getSocket(), message, masterNode.getHostname(), masterNode.getPort());
		
		log(String.format("Task report for %s task %d : %s",taskType, taskId, status));
	}
	
	/**
	 * @param nodeId
	 * @param status
	 */
	public void saveTaskProgress(String nodeId,int taskId,String taskType, String status){
		if (!getIsMaster())
			return;
			
		if (jobTracker == null)
			return;
		
		jobTracker.notifyTaskProgress(nodeId,taskId,taskType, status);
	}

	/**
	 * 
	 * @param args
	 */
	public static void main(String[] args) {

		if (args.length < 2) {
			System.out
					.println("Usage:java SdfsNode hostname portNumber [is_master (0|1)] [is_introducer (0|1)]");
			return;
		}

		String hostname = args[0];
		int port = Integer.parseInt(args[1]);
		String introducerHost = "localhost";
		int introducerPort = 10000;
		int lifespan = 0;
		boolean isMaster = false;
		boolean crash = false;

		if (args.length > 2) {
			introducerHost = args[2];
		}

		if (args.length > 3) {
			introducerPort = Integer.parseInt(args[3]);
		}

		if (args.length > 4) {
			isMaster = Integer.parseInt(args[4]) == 1;
		}

		if (args.length > 5) {
			lifespan = Integer.parseInt(args[5]);
		}

		if (args.length > 6) {
			crash = Integer.parseInt(args[6]) == 1;
		}

		new SdfsNode(hostname, port, introducerHost, introducerPort, isMaster,
				lifespan, crash);
	}
}
