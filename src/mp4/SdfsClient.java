package mp4;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File; 
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader; 
import java.io.SequenceInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap; 

import mp3.Helper;
import mp3.NodeInfo;


/**
 * Client used to perform file operations. All queries are initially handled by the
 * master node at first. Additional gathered info can then be sent to the client in order
 * to complete the file operations
 * 
 * This functionality sits on top of master node
 */
public class SdfsClient implements Runnable{
	
	private ClientProtocol clientProtocol;
	private String masterHost;
	private int masterPort;
	private boolean stop;
	
	/**
	 * @param masterHostname
	 * @param port
	 */
	public SdfsClient(String masterHostname, int port){ 
		masterHost = masterHostname;
		masterPort = port;
		
		clientProtocol = new ClientProtocol();
		
		Thread t = new Thread(this);
		t.start();
	}
	
	/**
	 * @param filename
	 */
	public void get(String filename){
		try {
			
			filename = filename.replace("/", "_");
			
			String result = clientProtocol.getFile(filename);
			
			if (result == null || result.equals("")){
				System.out.println(String.format("Client - The file %s was not found on SDFS",filename));
				return;
			}
			
			String [] nodeIdList = result.split(FileUtils.LIST_DELIM);
			
			if (nodeIdList == null || nodeIdList.length == 0)
				return;
			
			int blockIndex = 0;
			ArrayList<InputStream> streams = new ArrayList<InputStream>();
			
			//Download files to local directory
			for(String nodeId:nodeIdList){
				String partName = String.format("%s.part%d",filename,blockIndex);
				
				//String destinationPath = ClientProtocol.downloadFile(partName,Helper.extractNodeInfoFromId(nodeId));
				
				ByteArrayOutputStream stream = FileUtils.downloadStream(partName, Helper.extractNodeInfoFromId(nodeId));
				
				ByteArrayInputStream inputStream = new ByteArrayInputStream(stream.toByteArray());
				
				blockIndex+=1;
				
				streams.add(inputStream);
			}
			
			SequenceInputStream seqStream = new SequenceInputStream(Collections.enumeration(streams));
			
			// String folder =  "/tmp/ayivigu2_kjustic3/";
			
//			String folder = FileUtils.getStoragePath(new NodeInfo(masterHost, masterPort, null));
//			
//			File folderFile = new File(folder);
//			if (!folderFile.exists()) {
//				folderFile.mkdirs();
//			}
//			if (!folderFile.exists()) {
//				System.out.println("Could not create directory: " + folder);
//			}
			
			//String fullpath = String.format("%s/%s",folder,filename);
			
			FileOutputStream outputStream = new FileOutputStream(filename);
			
			byte[] buffer = new byte[1024];
			int len;
			while ((len = seqStream.read(buffer)) != -1) {
			    outputStream.write(buffer, 0, len);
			}
			
			outputStream.close();
			seqStream.close();
			
			System.out.println(String.format("Client - file retrieved and saved at %s",filename));
			
		} catch (ClassNotFoundException  e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			System.out.println("IO error within get operation");
			e.printStackTrace();
		}
	}
	
	/**
	 * @param filename
	 * @param destination
	 * 
	 * Saves the provided filename under the sdfs filename provided within
	 * the SDFS system.
	 * 
	 * 1- get file
	 * 2- break into chunk
	 * 3- Get destination SdfsNode from master node
	 * 4- send chunks to SdfsNode
	 * 5- notify master of completion (which triggers replication)
	 * @throws ClassNotFoundException 
	 */
	public void put(String filename, String destination){ 
		File f = new File(filename);
		if (!f.exists() || !f.isFile()) {
			System.out.println(filename + ": file not found.");
		}
		
		final ArrayList<File> chunks = FileSplitter.splitByShardSize(f, FileUtils.BLOCK_SIZE);	
		
		if (chunks == null || chunks.size() == 0)
			return;
		
		clientProtocol.initialize(masterHost, masterPort);
		
		try {

			HashMap<Integer,String> map = new HashMap<Integer,String>();
			
			String result = clientProtocol.getDestinationNodes(chunks.size());			
			
			//We expect to get back the following structure:
			// 1;2;0;1;0,0:blabla0;1:blabla1;2:blabla2
			//which should be broken down into two blocks:
			// 1;2;0;1;0  and 0:blabla0;1:blabla1;2:blabla2
			// the first block represent the id of the node to send respective chuncks to
			// the second block is the id to NodeInfo assignment
			
			String [] infoblocks = result.split(FileUtils.INFOBLOCK_DELIM);
			
			String [] ids = infoblocks[0].split(FileUtils.LIST_DELIM);
			String [] nodeIds = infoblocks[1].split(FileUtils.LIST_DELIM);
			
			for(String item: nodeIds){
				String[] infos = item.split(FileUtils.INFO_DELIM);
				map.put(Integer.parseInt(infos[0]), infos[1]);
			}
			
			//now that we have the Id to NodeInfo assignment, connect to chunk and send 
			//the data there. We send the chunk to the node
			
			final String sdfsDestination = destination.replace("/", "_");
			
			System.out.println("File to save to sdfs: " + sdfsDestination);
			
			for(int k = 0; k < chunks.size(); k++){
				 int id = Integer.parseInt(ids[k]);
				 final NodeInfo nodeInfo = Helper.extractNodeInfoFromId(map.get(id));
				 
				 final int idx = k;
				 
				 (new Thread(){
					 @Override
					 public void run(){
						 final File chunk = chunks.get(idx);
						 if (!chunk.exists() || !chunk.isFile()) {
								System.out.println("file chunk could not be found.");
						 }
						 
						 FileUtils.sendFile(chunk, nodeInfo, sdfsDestination, idx, chunks.size());
						 
						 //delete local chunk
						 if (chunks.size() > 1)
							 chunk.delete();
					 }
				 }).start();
				 
			}
			
		} catch (ClassNotFoundException e) { 
			//e.printStackTrace();
		}
	}
	
	/**
	 * @param filename
	 * 
	 * Delete the given sdfs file from the system.
	 */
	public void delete(String filename){
		try {
			clientProtocol.deleteFile(filename);
		} catch (ClassNotFoundException e) {
			System.out.println("Could not delete " + filename);
			e.printStackTrace();
		}
	}
	
	/**
	 * @param commandInfos
	 */
	public void maple(String [] commandInfos){
		
		String exe = commandInfos[1];
		String prefix = commandInfos[2];
		
		ArrayList<String> files = new ArrayList<String>();
		
		for(int k=3; k< commandInfos.length; k++){
			files.add(commandInfos[k].trim());
		}
		
		try {
			
			clientProtocol.sendProgram(exe);
			clientProtocol.sendMapleMessage(exe, prefix, files);
			
			pollJobStatus();
			
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
	}
	
	/**
	 * @param commandInfos
	 */
	public void juice(String [] commandInfos){
		String exe = commandInfos[1];
		String numberOfJuices = commandInfos[2];
		String prefix = commandInfos[3];
		String destinationSdfs = commandInfos[4];
		
		try {
			
			clientProtocol.sendProgram(exe);
			clientProtocol.sendJuiceMessage(exe,
					Integer.parseInt(numberOfJuices),
					prefix, destinationSdfs);
			
			pollJobStatus();
			
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
	}
	
	
	/**
	 * @param commandInfos
	 * Handles interactive command entered at the client
	 */
	public void processInteractiveCommand(String [] commandInfos){		
		if (commandInfos[0].equals("get")){
			if (commandInfos.length > 1)
				get(commandInfos[1]);
		}else if (commandInfos[0].equals("put")){
			if (commandInfos.length > 2)
				put(commandInfos[1],commandInfos[2]);	
		}else if(commandInfos[0].equals("delete")){
			if (commandInfos.length > 1)
				delete(commandInfos[1]);
		}else if(commandInfos[0].equals("maple")){
			if (commandInfos.length > 3)
				maple(commandInfos);
		}else if(commandInfos[0].equals("juice")){
			if (commandInfos.length > 3)
				juice(commandInfos);
		}
	}

	@Override
	public void run() {
		
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		
		while(true){
			try {
				String input = br.readLine();
				String [] info =  input.split(" ");
				
				if (info[0].equals("quit")){
					break;
				}else{
					processInteractiveCommand(info);
				}
			} catch (IOException e) { 
				System.out.println("Input line from prompt could not be read");
				e.printStackTrace();
			}	
		}
	}
	
	
	/**
	 * Polls for job status information
	 */
	private void pollJobStatus(){
		
		final int pollInterval = 2000;
		
		//poll for job status every 2 second
		(new Thread(){
			
			public void run(){
				while(!stop){		
					
					String status = "...";
					
					try {
						status = clientProtocol.getActiveJobStatus();
					} catch (ClassNotFoundException e) { 
						System.out.println("Client - Unable to retrieve job status information");
					}
					
					if (status.equals(SdfsMessageHandler.TASK_REPORT_COMPLETED_PREFIX)){
						System.out.println("Client - Task completed");
						break;
					}else if (status.equals(SdfsMessageHandler.TASK_REPORT_FAILED_PREFIX)) {
						System.out.println("Client - Task execution failed");
						break;
					}else if (status.equals(SdfsMessageHandler.TASK_REPORT_BUSY_PREFIX)){
						
					}
					
					try {
						Thread.sleep(pollInterval);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		}).start();	
	}
	
	public static void main(String[] args){

		if (args.length < 2){
			System.out.println("Usage: SdfsClient masterHostname masterPort");
			System.exit(0);
		}
		
		String hostname = args[0];
		int port = Integer.parseInt(args[1]);
		
		new SdfsClient(hostname,port);	
	}
}
