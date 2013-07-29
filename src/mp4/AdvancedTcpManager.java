package mp4;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays; 
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import mp2.GrepHandler;
import mp3.Helper;
import mp3.NodeInfo;
import mp5.JobTracker;

/**
 * handles TCP communication and queries sent to the node
 */
public class AdvancedTcpManager implements Runnable {

	private Socket socket;
	private SdfsNode node;
	private Random rd; 
	
	/**
	 * 
	 */
	public AdvancedTcpManager(Socket theSocket, SdfsNode node) {
		socket = theSocket;
		this.node = node;
		rd = new Random();

		Thread t = new Thread(this);
		t.start();
	}

	/**
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Runnable#run()
	 */
	public void run() {

		try {
			ObjectInputStream ois = new ObjectInputStream(
					socket.getInputStream());

			ObjectOutputStream oos = new ObjectOutputStream(
					socket.getOutputStream());

			Object inputLine;

			inputLine = ois.readObject();

			processMessage((String) inputLine, oos, ois);

			ois.close();
			oos.close();

			// socket.close();

		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	/**
	 * @param message
	 * @param oos
	 * @throws FileNotFoundException
	 */
	public void processMessage(String message, ObjectOutputStream oos,
			ObjectInputStream ois) throws FileNotFoundException {
		if (message.startsWith(ClientProtocol.PUT_OP)) {
			doPut(message, oos, ois);
		} else if (message.startsWith(ClientProtocol.DEL_OP)) {
			doDelete(message, oos, ois);
		} else if (message.startsWith(FileUtils.SENDING_FILE_PREFIX)) {
			doReceiveFile(message, oos, ois);
		} else if (message.startsWith(FileUtils.GREP_PREFIX)) {
			doGrep(message, oos, ois);
		}else if (message.startsWith(ClientProtocol.GET_OP)) {
			doGet(message, oos, ois);
		}else if (message.startsWith(ClientProtocol.BLOCK_REQUEST)){
			doSendBlock(message, oos, ois);
		}else if (message.startsWith(ClientProtocol.MAPLE_OP)){
			doMaple(message, oos, ois);
		}else if (message.startsWith(ClientProtocol.JUICE_OP)){
			doJuice(message, oos, ois);
		}else if (message.startsWith(FileUtils.SENDING_PROGRAM_PREFIX)){
			doSaveProgram(message, oos, ois);
		}else if (message.startsWith(ClientProtocol.JOB_STATUS)){
			doSendJobStatus(message, oos, ois);
		}
	}

	/**
	 * @param message
	 * @param oos
	 * @param ois
	 */
	public void doGrep(String message, ObjectOutputStream oos,
			ObjectInputStream ois) {

		try {

			Object inputLine;

			GrepHandler handler = new GrepHandler();

			String responseHeader = (String) ois.readObject();

			inputLine = ois.readObject();

			handler.grepInline((String) inputLine, responseHeader, oos);
 
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * @param message
	 * @param oos
	 * @param ois
	 * @throws FileNotFoundException
	 */
	public void doReceiveFile(String message, ObjectOutputStream oos,
			ObjectInputStream ois) throws FileNotFoundException {
		
		String info[] = message.split(FileUtils.INFO_DELIM);
		String sdfsFileName = info[1];
		int chunkIndex = Integer.parseInt(info[2]);
		int chunkCount = Integer.parseInt(info[3]);

		String folderPath = FileUtils.getStoragePath(Helper
				.extractNodeInfoFromId(node.getNodeId()));
		File folder = new File(folderPath);
		if (!folder.exists())
			folder.mkdirs();

		String filepath = String.format("%s/%s.part%d", folderPath,
				sdfsFileName, chunkIndex);

		FileOutputStream fos = new FileOutputStream(filepath);

		int donwloadStreamSize = 4096;

		byte[] buffer = new byte[donwloadStreamSize];
		int bytesRead = 0;

		while (bytesRead >= 0) {
			try {
				bytesRead = ois.read(buffer);

				if (bytesRead >= 0) {
					fos.write(buffer, 0, bytesRead);
				}

				if (bytesRead == -1) {

					fos.flush();
					fos.close();

					try {
						String messageOver = (String) ois.readObject();

						if (messageOver.contains(FileUtils.TRANSFER_OVER_PREFIX)) {

							oos.writeObject(FileUtils.TRANSFER_OK_PREFIX);

							ois.close();
							oos.close();
 
						}
					} catch (ClassNotFoundException e){
						
					}
					 catch (IOException e) {
						// TODO Auto-generated catch block
						System.out.println("Error during file block transfer");
						e.printStackTrace();
					}

					this.node.log(String.format(
							"Block Sent: file [%s] - block id %d successful.",
							sdfsFileName, chunkIndex));

					break;
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		node.addBlockInfo(this.node.getNodeId(), sdfsFileName, chunkIndex,
				chunkCount);

		node.notifyChunckReception( sdfsFileName, chunkIndex, chunkCount);
	}

	/**
	 * @param message
	 * @param oos
	 * @param ois
	 */
	public void doPut(String message, ObjectOutputStream oos,
			ObjectInputStream ois) {

		if (!node.getIsMaster())
			return;

		String info[] = message.split(":");

		// String chunkNamesBody = info[1];

		int chunkCount = Integer.parseInt(info[1]);

		// String [] chunkNames = chunkNamesBody.split(FileUtils.LIST_DELIM);
		// chunkCount = chunkNames.length;

		String response = "";
		String delim = "";

		HashSet<Integer> idxSet = new HashSet<Integer>();

		synchronized (this.node.membershipList) {

			int size = this.node.membershipList.size();

			System.out.println("Candidates from membership of size " + size);

			for (int k = 0; k < chunkCount; k++) {

				// int mapId = Mapper.getNodeNumber(chunkNames[k], size);
				int mapId = rd.nextInt(size);
				response += delim + mapId;
				delim = FileUtils.LIST_DELIM;

				idxSet.add(mapId);
			}

			delim = FileUtils.INFOBLOCK_DELIM;

			for (int id : idxSet) {
				response += delim + id + FileUtils.INFO_DELIM
						+ this.node.membershipList.get(id);
				delim = FileUtils.LIST_DELIM;
			}
		}

		try {

			oos.writeObject(response);
			
			sendEndOfCommMessage(oos);

			this.node.log("Sending datanode candidates for file storage");
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * @param message
	 * @param oos
	 * @param ois
	 * 
	 * Gets blocks of a file from sdfs and combine them
	 * This method sends a list of node ordered by block id to download
	 * blocks of the requested file from.
	 */
	public void doGet(String message, ObjectOutputStream oos,
			ObjectInputStream ois) {
		
		if (!node.getIsMaster()){	
			sendEndOfCommMessage(oos);		
			return;
		}
			

		String [] info = message.split(FileUtils.INFO_DELIM);
		
		String filename = info[1];
		
		if (!node.fileMetadata.containsKey(filename)){
			
			System.out.println(String.format("Cannot find file %s in Sdfs",filename));
			
			return;
		}
		
		String response = "";
		HashMap<Integer,String> blockHolders = new HashMap<Integer,String>();

		//find nodes which hold the file we are looking for, download and combine them
		synchronized(node.sdfsMetadata){
			
			int blockCount = node.fileMetadata.get(filename);
					
			for(String nodeId:node.sdfsMetadata.keySet()){
				
				HashMap<String, Set<Integer>> map = node.sdfsMetadata.get(nodeId);
				
				if (!map.containsKey(filename))
					continue;
				
				for(int blockIndex:map.get(filename)){
					if (!blockHolders.containsKey(blockIndex))
						blockHolders.put(blockIndex, nodeId);
				}
				
				if (blockHolders.size() == blockCount)
					break;
			}		
			
			//Downloads blocks and combine them
			
			Map<Integer,String> sortedMap = new TreeMap<Integer, String>(blockHolders);
			 
			response = Helper.join(new ArrayList<String>( sortedMap.values()),FileUtils.LIST_DELIM);	
		}
		
		try {
			oos.writeObject(response);
			
			sendEndOfCommMessage(oos);
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * @param message
	 * @param oos
	 * @param ois
	 */
	public void doDelete(String message, ObjectOutputStream oos,
			ObjectInputStream ois) {
		if (!node.getIsMaster())
			return;

		sendEndOfCommMessage(oos);

		String[] info = message.split(FileUtils.INFO_DELIM);
		String sdfsFileName = info[1];

		synchronized (node.sdfsMetadata) {
			ArrayList<String> recipients = new ArrayList<String>();

			for (String nodeId : node.sdfsMetadata.keySet()) {

				HashMap<String, Set<Integer>> fileMap = node.sdfsMetadata
						.get(nodeId);

				if (fileMap.containsKey(sdfsFileName)) {
					recipients.add(nodeId);
				}
			}

			if (recipients.size() > 0) {
				String deleteMessage = String.format("%s:%s",
						SdfsMessageHandler.DELETE_FILE_PREFIX, sdfsFileName);
				Helper.sendBMulticastMessage(node.getSocket(), deleteMessage,
						recipients);
			}
		}
	}
	
	
	/**
	 * @param message
	 * @param oos
	 * @param ois
	 * 
	 * Initiate a block download
	 */
	public void doSendBlock(String message, ObjectOutputStream oos,
			ObjectInputStream ois){
		
		String [] info = message.split(FileUtils.INFO_DELIM);

		String filename = info[1]; 
		
		NodeInfo currentNodeInfo = Helper.extractNodeInfoFromId(node.getNodeId());
		String filepath = String.format("%s/%s",FileUtils.getStoragePath(currentNodeInfo) ,
												filename);
		try {
			int streamSize = 4096;
			byte[] buffer = new byte[streamSize];
			
			
			File file = new File(filepath);
	
			FileInputStream fileStream = new FileInputStream(file);

			int number;
			while ((number = fileStream.read(buffer)) != -1) { 
				oos.write(buffer,0,number); 
			}
	
			fileStream.close();
	
			// notify of transfer completion
			oos.writeObject(FileUtils.TRANSFER_OVER_PREFIX);
			
	
			String transferOkFailMessage = (String) ois.readObject();

			if (transferOkFailMessage.startsWith(FileUtils.TRANSFER_OK_PREFIX)) {
				// System.out.println(String.format("Block Sent: file [%s] - block id %d successful.",originalFileName,chunkIndex));
				ois.close();
				oos.close();
				socket.close();
		}
		} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}	
	
	/**
	 * @param message
	 * @param oos
	 * @param ois
	 */
	public void doMaple(String message, ObjectOutputStream oos,
			ObjectInputStream ois){
		
		if (!node.getIsMaster())
			return;
		
		sendEndOfCommMessage(oos);
		
		String [] info = message.split(FileUtils.INFO_DELIM);
		
		String exe = info[1];
		String prefix = info[2];
		String [] files = info[3].split(FileUtils.LIST_DELIM);
		
		ArrayList<String> sdfsFiles = new ArrayList<String>(Arrays.asList(files));
		
		//process maple message
		node.initializeMapleJob(exe, prefix, sdfsFiles);
	}
	
	/**
	 * @param message
	 * @param oos
	 * @param ois
	 */
	public void doJuice(String message, ObjectOutputStream oos,
			ObjectInputStream ois){
		
		if (!node.getIsMaster())
			return;
		
		sendEndOfCommMessage(oos);
		
		String [] info = message.split(FileUtils.INFO_DELIM);
		
		String exe = info[1];
		int numberOfJuices = Integer.parseInt(info[2]);
		String prefix = info[3];
		String destinationSdfs = info[4];
		 
		//process juice message
		node.initializeJuiceJob(exe,numberOfJuices, prefix, destinationSdfs);
	}
	
	private void doSaveProgram(String message, ObjectOutputStream oos,
			ObjectInputStream ois) throws FileNotFoundException{
		
		String info[] = message.split(FileUtils.INFO_DELIM);
		String programName = info[1]; 

		String folderPath = FileUtils.getConfigStorageFolder(Helper
				.extractNodeInfoFromId(node.getNodeId()));
		
		File folder = new File(folderPath);
		if (!folder.exists())
			folder.mkdirs();

		String filepath = String.format("%s/%s", folderPath, programName);

		FileOutputStream fos = new FileOutputStream(filepath);

		int donwloadStreamSize = 4096;

		byte[] buffer = new byte[donwloadStreamSize];
		int bytesRead = 0;

		while (bytesRead >= 0) {
			try {
				bytesRead = ois.read(buffer);

				if (bytesRead >= 0) {
					fos.write(buffer, 0, bytesRead);
				}

				if (bytesRead == -1) {

					fos.flush();
					fos.close();

					try {
						String messageOver = (String) ois.readObject();

						if (messageOver.contains(FileUtils.TRANSFER_OVER_PREFIX)) {

							oos.writeObject(FileUtils.TRANSFER_OK_PREFIX);

							ois.close();
							oos.close();

							this.node.log("Program saved successfully");
						}
					} catch (ClassNotFoundException e){
						
					}
					 catch (IOException e) {
						// TODO Auto-generated catch block
						System.out.println("Error during file block transfer");
						e.printStackTrace();
					}

					this.node.log("Program saved successfully");

					break;
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * @param message
	 * @param oos
	 * @param ois
	 */
	public void doSendJobStatus(String message, ObjectOutputStream oos,
			ObjectInputStream ois) {

		if (!node.getIsMaster())
			return;

		String response = "";
		
		JobTracker tracker = node.getJobTracker();		
		
		if (tracker != null){
			if (tracker.completed){
				response = SdfsMessageHandler.TASK_REPORT_COMPLETED_PREFIX;
			}else if (tracker.jobFailed){
				response = SdfsMessageHandler.TASK_REPORT_FAILED_PREFIX;
			}else{
				response = SdfsMessageHandler.TASK_REPORT_BUSY_PREFIX;
			}
		}
		
		try {

			oos.writeObject(response);
			
			sendEndOfCommMessage(oos);

			this.node.log("Sending datanode candidates for file storage");
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * @param oos
	 */
	private void sendEndOfCommMessage(ObjectOutputStream oos){
		
		if (oos == null)
			return;
		
		try {
			oos.writeObject(ClientProtocol.END_OF_COMM);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
