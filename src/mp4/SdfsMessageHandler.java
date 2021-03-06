package mp4;
import java.io.File;
import java.net.DatagramPacket;
import java.net.DatagramSocket;

import mp3.Helper;
import mp3.MessageHandler;
import mp3.NodeInfo;

/**
 * This class extends the message handler class used in the group membership to handle
 * additional SDFS specific messages on the UDP channel
 */
public class SdfsMessageHandler extends MessageHandler{

	public static final String COORD_MESSAGE_PREFIX = "coordmaster";
	public static final String MASTER_MESSAGE_PREFIX = "master";
	public static final String SAVE_BLOCK_META_PREFIX = "metadata_put_block"; 
	public static final String DELETE_FILE_PREFIX = "remove";
	public static final String DELETE_FILE_META_PREFIX = "metadata_delete_file";
	public static final String REPLICATE_BLOCK_PREFIX = "replicate";
	public static final String MAPLE_PREFIX = "do_maple"; 
	public static final String JUICE_PREFIX = "do_juice";
	public static final String TASK_REPORT_PREFIX = "task_report";
	public static final String TASK_REPORT_BUSY_PREFIX = "task_busy";
	public static final String TASK_REPORT_COMPLETED_PREFIX = "task_completed";
	public static final String TASK_REPORT_FAILED_PREFIX = "task_failed";
	public static final String MAPLE = "maple";
	public static final String JUICE = "juice";
	public static final String ELECT_SECONDARY_MESSAGE_PREFIX = "electsecondary";
	public static final String COORD_SECONDARY_MESSAGE_PREFIX = "coordsecondary";
	public static final String JOB_INFO = "job_info";
	
	private SdfsNode node;
	
	/**
	 * @param socket
	 * @param packet
	 * @param node
	 */
	public SdfsMessageHandler(DatagramSocket socket, DatagramPacket packet,
			SdfsNode node) {
		super(socket, packet, node); 
		
		this.node = node;
	}

	@Override
	public void customHandleMessage(String message){		
		if (message.startsWith(JOIN_REQUEST_PREFIX)){
			registerSdfsNode(message);
		}
		else if (message.startsWith(MASTER_MESSAGE_PREFIX)){
			//send node block information - hadoop like functionality
			registerWithMaster(message);
		}
		else if (message.startsWith(COORD_MESSAGE_PREFIX)){	 
			//a new master was elected
			updateMaster(message);
		}else if (message.startsWith(CRASHED_MESSAGE_PREFIX)){
			initiateReplication(message);
		}else if (message.startsWith(SAVE_BLOCK_META_PREFIX)){
			saveBlockToMetadata(message);
		}else if (message.startsWith(DELETE_FILE_PREFIX)){
			deleteFile(message);
		}else if (message.startsWith(DELETE_FILE_META_PREFIX)){
			deleteFileMetadata(message);
		}
		else if (message.startsWith(REPLICATE_BLOCK_PREFIX)){
			replicateFile(message);
		}else if (message.startsWith(MAPLE_PREFIX)){
			performMaple(message);
		}else if (message.startsWith(JUICE_PREFIX)){
			performJuice(message);
		}else if (message.startsWith(TASK_REPORT_PREFIX)){
			performTaskReport(message);
		}else if (message.startsWith(ELECT_SECONDARY_MESSAGE_PREFIX)){
			electSecondary(message);
		}else if (message.startsWith(COORD_SECONDARY_MESSAGE_PREFIX)){
			updateSecondaryMaster(message);
		}else if (message.startsWith(JOB_INFO)){
			storeJobInfo(message);
		}
	}
	
	/**
	 * @param message
	 * Updates current master information
	 */
	private void updateMaster(String message){
		String masterId = message.split(FileUtils.INFO_DELIM)[1];	
		node.setMasterId(masterId);
		node.log(String.format("Storing new master node info <%s>",masterId));
		
		node.runReplicationTask();
	}

	/**
	 * @param message
	 * Sends master info to newly joining node
	 */
	private void registerSdfsNode(String message){
		
		if (!node.getIsIntroducer())
			return;
		
		String nodeId = message.split(FileUtils.INFO_DELIM)[1];
		String masterMessage = String.format("%s:%s", MASTER_MESSAGE_PREFIX,node.getMasterId());
		
		NodeInfo nodeInfo = Helper.extractNodeInfoFromId(nodeId);
		
		Helper.sendUnicastMessage(node.getSocket(), masterMessage, 
				nodeInfo.getHostname(), nodeInfo.getPort());
	}
	
	/**
	 * @param message
	 * This method is called to set the master node info fro a newly joining 
	 * member and to have it send to the master node its block info
	 */
	private void registerWithMaster(String message){
		updateMaster(message);
		node.initializeSdfsNode();
	}
	
	private void initiateReplication(String message){
		String [] info = message.split(FileUtils.INFO_DELIM);
		
		node.removeNodeFromMetadata(info[1]);
		node.runReplicationTask();
	}
	
	/**
	 * @param message
	 */
	private void saveBlockToMetadata(String message){
		String[] info = message.split(FileUtils.INFO_DELIM);
		
		String nodeId = info[1];
		String sdfsFileName = info[2];
		int chunkIndex = Integer.parseInt(info[3]);
		int chunkCount = Integer.parseInt(info[4]);

		node.addBlockInfo(nodeId, sdfsFileName, chunkIndex,chunkCount);
	}
	
	/**
	 * @param message
	 */
	private void deleteFile(String message){
		String[] info = message.split(FileUtils.INFO_DELIM);
		String filename = info[1];
		
		node.deleteSdfsFile(filename);
		node.log(String.format("Deleted blocks for file %s",filename));
	}
	
	/**
	 * @param message
	 */
	private void deleteFileMetadata(String message){
		String[] info = message.split(FileUtils.INFO_DELIM);
	
		String filename = info[1]; 
		 
		node.deleteSdfsFileMetadata(filename);
		
		node.log(String.format("Deleted blocks metadata for file %s from <%s:%d>",
				filename,
				node.getHostname(),
				node.getNodePortNumber()));
	}
	
	/**
	 * @param message
	 * 
	 * Performs replication of block [or rather shipping from this node to another]
	 */
	private void replicateFile(String message){
		String[] info = message.split(FileUtils.INFO_DELIM);
		
		String destinationId = info[1];
		String filename = info[2];
		int blockIndex = Integer.parseInt(info[3]);
		int numberOfBlock = Integer.parseInt(info[4]);
		
		NodeInfo nodeInfo = Helper.extractNodeInfoFromId(destinationId);
		
		NodeInfo currentNodeInfo = Helper.extractNodeInfoFromId(node.getNodeId());
		
		String filePath = String.format("%s/%s.part%d", FileUtils.getStoragePath(currentNodeInfo),filename,blockIndex);
				
		FileUtils.sendFile(new File(filePath), nodeInfo, filename, blockIndex, numberOfBlock);
		
		//node.log(String.format("Replicating block %d of file %s to <%s:%d>",blockIndex,filename,
		//		nodeInfo.getHostname(),nodeInfo.getPort()));
	}
	
	/**
	 * @param message
	 */
	private void performMaple(String message){
		String[] info = message.split(FileUtils.INFO_DELIM);
		
		int taskId = Integer.parseInt(info[1]);
		String exe = info[2];
		String prefix = info[3];
		String sdfsFileName = info[4];
		
		node.doMapleTask(taskId, exe, prefix, sdfsFileName);
		
		node.log(String.format("Starting maple task %d",taskId));
	}
	
	/**
	 * @param message
	 */
	private void performJuice(String message){
		String[] info = message.split(FileUtils.INFO_DELIM);
		
		int taskId = Integer.parseInt(info[1]);
		String exe = info[2];
		String sourceFile = info[3];
		String destFile = info[4];
		
		node.doJuiceTask(taskId, exe, sourceFile, destFile);	
		node.log(String.format("Starting juice task %d for key %s",taskId,sourceFile));
	}
	
	/**
	 * @param message
	 */
	private void performTaskReport(String message){
		String[] info = message.split(FileUtils.INFO_DELIM);
		
		String nodeSource = info[1];
		int taskId = Integer.parseInt(info[2]); 
		String taskType = info[3]; 
		String status = info[4]; 
		
		node.saveTaskProgress(nodeSource,taskId,taskType, status); 
	}
	
	
	/**
	 * @param message
	 */
	private void electSecondary(String message){
		node.setSecondaryMasterId(node.getNodeId());
		
		String replyMessage = String.format("%s:%s", COORD_SECONDARY_MESSAGE_PREFIX,node.getNodeId());
		
		NodeInfo nodeInfo = Helper.extractNodeInfoFromId(node.getNodeId());
		
		Helper.sendBMulticastMessage(node.getSocket(), replyMessage, 
				node.membershipList);
		
		node.log(String.format("Broadcasting secondary master information to the membership: <%s:%d>",
				nodeInfo.getHostname(),nodeInfo.getPort()));
	}
 
	/**
	 * Updates secondary master information
	 * @param message
	 */
	private void updateSecondaryMaster(String message){
		String[] info = message.split(FileUtils.INFO_DELIM);
		String nodeSource = info[1];
		
		if (nodeSource.equals(node.getSecondaryMasterId()))
			return;
		
		node.setSecondaryMasterId(nodeSource);
		node.log("Updating secondary master information");
	}
	
	private void storeJobInfo(String message){
		node.saveJobInfo(message);
		node.log("Saving job info");
	}
}

