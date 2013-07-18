package mp4;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
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
			
			String [] nodeIdList = result.split(FileUtils.LIST_DELIM);
			
			int blockIndex = 0;
			ArrayList<InputStream> streams = new ArrayList<InputStream>();
			
			//Download files to local directory
			for(String nodeId:nodeIdList){
				String partName = String.format("%s.part%d",filename,blockIndex);
				String destinationPath = clientProtocol.downloadFile(partName,Helper.extractNodeInfoFromId(nodeId));
				
				blockIndex+=1;
				
				streams.add(new FileInputStream(new File(destinationPath)));
			}
			
			SequenceInputStream seqStream = new SequenceInputStream(Collections.enumeration(streams));
			
			String folder =  "/tmp/ayivigu2_kjustic3/";
			File folderFile = new File(folder);
			if (!folderFile.exists())
				folderFile.mkdirs();
			
			String fullpath = String.format("%s/%s",folder,filename);
			FileOutputStream outputStream = new FileOutputStream(fullpath);
			
			byte[] buffer = new byte[1024];
			int len;
			while ((len = seqStream.read(buffer)) != -1) {
			    outputStream.write(buffer, 0, len);
			}
			
			outputStream.close();
			seqStream.close();
			
			System.out.println(String.format("Client - file retrieved and saved at %s",fullpath));
			
		} catch (ClassNotFoundException  e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
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
		final ArrayList<File> chunks = FileSplitter.splitByShardSize(f, FileUtils.BLOCK_SIZE);	
		
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
			
			for(int k=0; k< chunks.size();k++){
				 int id = Integer.parseInt(ids[k]);
				 final NodeInfo nodeInfo = Helper.extractNodeInfoFromId(map.get(id));
				 
				 final int idx = k;
				 
				 (new Thread(){
					 @Override
					 public void run(){
						 final File chunk = chunks.get(idx);
						 FileUtils.sendFile(chunk, nodeInfo, sdfsDestination, idx, chunks.size());
						 
						 //delete local chunk
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
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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
		}
	}

	@Override
	public void run() {
		
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		
		while(true){
			try {
				String input = br.readLine();
				String [] info =  input.toLowerCase().split(" ");
				
				if (info[0].equals("quit")){
					break;
				}else{
					processInteractiveCommand(info);
				}
			} catch (IOException e) { 
				e.printStackTrace();
			}	
		}
	}
}