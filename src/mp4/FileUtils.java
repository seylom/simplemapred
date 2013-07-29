package mp4;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import mp3.NodeInfo;

public class FileUtils {

	public static final String ValidChars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
	public static int BLOCK_SIZE = 1024 * 1024 * 1;
	public static int NUMBER_OF_REPLICAS = 2;
	public static final String LIST_DELIM = ";";
	public static final String INFOBLOCK_DELIM = ",";
	public static final String INFO_DELIM = ":";

	public static final String SENDING_FILE_PREFIX = "sending_file";
	public static final String TRANSFER_OVER_PREFIX = "transfer_over";
	public static final String TRANSFER_OK_PREFIX = "transfer_ok";
	public static final String GREP_PREFIX = "grep";
	public static final String SENDING_PROGRAM_PREFIX = "sending_program";
	
	public enum TaskType{
		MAPLE,
		JUICE
	}
	
	public enum TaskStatus{
		PAUSED,
		RUNNING,
		SUCCESS,
		FAILURE,	
	}
	
	/**
	 * @param info
	 * @return
	 * 
	 *         Retrieve storage path for Sdfs save/read operations
	 */
	public static String getBasePath(NodeInfo info) {
		return String.format("/tmp/ayivigu2_kjustic3/%s-%d/",
				info.getHostname(), info.getPort());
	}

	/**
	 * @param info
	 * @return
	 * 
	 *         Retrieve storage path for Sdfs save/read operations
	 */
	public static String getStoragePath(NodeInfo info) {
		return String.format("%s/sdfs/",getBasePath(info),
				info.getHostname(), info.getPort());
	}
	

	public static String getConfigStorageFolder(NodeInfo info) {
		return String.format("%s/config/",getBasePath(info),
				info.getHostname(), info.getPort());
	}
	
	public static boolean deleteDirectory(File directory) {
	    if(directory.exists()){
	        File[] files = directory.listFiles();
	        if(null!=files){
	            for(int i=0; i<files.length; i++) {
	                if(files[i].isDirectory()) {
	                    deleteDirectory(files[i]);
	                }
	                else {
	                    files[i].delete();
	                }
	            }
	        }
	    }
	    return(directory.delete());
	}
	
	/**
	 * @param info
	 * @return
	 * 
	 *         Retrieve storage path for maple juice local files
	 */
	public static String getMapleJuiceStoragePath(NodeInfo info) {
		return String.format("%s/maplejuice/",getBasePath(info),
				info.getHostname(), info.getPort());
	}

	/**
	 * @param file
	 * @param nodeInfo
	 * 
	 * send a file to a host
	 */
	public static void sendFile(File file, NodeInfo nodeInfo,
			String originalFileName, int chunkIndex, int numberOfChunks) {
		
		if (!file.exists() || !file.isFile()) {
			System.out.println("File " + file + " could not be transferred because it does not exist.");
			return;
		}

		// connect to node and send file for storage
		try {
			Socket socket = new Socket();
			socket.connect(new InetSocketAddress(nodeInfo.getHostname(),
					nodeInfo.getPort()), 5000);

			socket.setSoTimeout(10000);

			ObjectOutputStream oos = new ObjectOutputStream(
					socket.getOutputStream());

			ObjectInputStream ois = new ObjectInputStream(
					socket.getInputStream());

			int streamSize = 4096;
			byte[] buffer = new byte[streamSize];

			String message = String.format("%s:%s:%s:%s",
					FileUtils.SENDING_FILE_PREFIX, originalFileName,
					chunkIndex, numberOfChunks);

			oos.writeObject(message);

			FileInputStream fileStream = new FileInputStream(file);

			int number;
			while ((number = fileStream.read(buffer)) != -1) { 
				oos.write(buffer,0,number); 
			}

			fileStream.close();

			// notify of transfer completion
			oos.writeObject(FileUtils.TRANSFER_OVER_PREFIX);

			String transferOkFailMessage = (String) ois.readObject();

			if (transferOkFailMessage.startsWith(TRANSFER_OK_PREFIX)) { 
				ois.close();
				oos.close();
				socket.close();
			}
		} catch (UnknownHostException e) {
			System.out.println(String.format("Unable to find host <%s:%d>",
					nodeInfo.getHostname(), nodeInfo.getPort()));
		} catch (IOException e) {
			System.out.println(String.format(
					"Problem with connection to <%s:%d>",
					nodeInfo.getHostname(), nodeInfo.getPort()));
			
		} catch (ClassNotFoundException e) {

		}
	}

	/**
	 * @param sdfsBlockFileName
	 * 
	 *            Deletes the given filename
	 */
	public static void deleteFile(String sdfsBlockFileName) {
		File file = new File(sdfsBlockFileName);

		if (file.exists())
			file.delete();
	}
	
	/**
	 * Returns all files stored in the SDFS system
	 * @param node
	 * @return
	 */
	public static Set<String> getStoredSdfsFiles(SdfsNode node){
		return node.fileMetadata.keySet();
	}
	
	/**
	 * @param node
	 * @return
	 */
	public static synchronized HashMap<String, HashMap<Integer,Set<String>>> 
			getFileToBlocksLocationMap(SdfsNode node){
		
		//given a file as key we have a map which gives for each block of the file
		//the set of nodes which currently store that block
		HashMap<String, HashMap<Integer,Set<String>>> fileToBlockHolders =  
				new HashMap<String, HashMap<Integer,Set<String>> >();
		
		synchronized(node.sdfsMetadata){
			for(String nodeId:node.sdfsMetadata.keySet()){
				 
				 HashMap<String,Set<Integer>> fileBlocks = node.sdfsMetadata.get(nodeId);	 
	
				 for(String file:fileBlocks.keySet()){
					 
					 HashMap<Integer,Set<String>> blockHolders;
					 
					 if (!fileToBlockHolders.containsKey(file)){
						 blockHolders = new HashMap<Integer,Set<String>>();
						 fileToBlockHolders.put(file, blockHolders);
					 }else{
						 blockHolders = fileToBlockHolders.get(file);
					 }
					  
					 for(int blockIndex:fileBlocks.get(file)){
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
		}
		
		return fileToBlockHolders;
	}
	
	/**
	 * Given a file, we retrieve a map of block to SdfsNode
	 * This method is used to locate blocks in the system.
	 * @param node
	 * @param sdfsFileName
	 * @return
	 */
	public static synchronized HashMap<Integer,Set<String>> getSdfsFileBlockLocations(SdfsNode node, String sdfsFileName){
		HashMap<Integer,Set<String>> blockLocationMap = null;
		
		HashMap<String, HashMap<Integer,Set<String>>> map = getFileToBlocksLocationMap(node);
		
		if (map.containsKey(sdfsFileName)){
			blockLocationMap = map.get(sdfsFileName);
		}
		
		return blockLocationMap;
	}
	
	/**
	 * @param file
	 * @param nodeInfo
	 * 
	 *            send a file to a host
	 */
	public static void uploadProgram(File file, String hostname,int port) {
		
		if (!file.exists() || !file.isFile()) {
			System.out.println("File " + file + " could not be transferred because it does not exist.");
			return;
		}

		// connect to node and send file for storage
		try {
			Socket socket = new Socket();
			socket.connect(new InetSocketAddress(hostname,
					port), 5000);

			socket.setSoTimeout(10000);

			ObjectOutputStream oos = new ObjectOutputStream(
					socket.getOutputStream());

			ObjectInputStream ois = new ObjectInputStream(
					socket.getInputStream());

			int streamSize = 4096;
			byte[] buffer = new byte[streamSize];

			String message = String.format("%s:%s",
					FileUtils.SENDING_PROGRAM_PREFIX,file.getName());

			oos.writeObject(message);

			FileInputStream fileStream = new FileInputStream(file);

			int number;
			while ((number = fileStream.read(buffer)) != -1) { 
				oos.write(buffer,0,number); 
			}

			fileStream.close();

			// notify of transfer completion
			oos.writeObject(FileUtils.TRANSFER_OVER_PREFIX);

			String transferOkFailMessage = (String) ois.readObject();

			if (transferOkFailMessage.startsWith(TRANSFER_OK_PREFIX)) { 
				ois.close();
				oos.close();
				socket.close();
			}
		} catch (UnknownHostException e) {
			System.out.println(String.format("Unable to find host <%s:%d>",
					hostname, port));
		} catch (IOException e) {
			System.out.println(String.format(
					"Problem with connection to <%s:%d>",
					hostname, port));
			
		} catch (ClassNotFoundException e) {

		}
	}
	
	/**
	 * @param filename
	 * @param nodeInfo
	 */
	public static ByteArrayOutputStream downloadStream(String filename, NodeInfo nodeInfo) {

		ByteArrayOutputStream fos = null;
		 
		String message = String.format("%s:%s", ClientProtocol.BLOCK_REQUEST, filename);

		try {

			Socket socket = new Socket();

			socket.connect(new InetSocketAddress(nodeInfo.getHostname(),
					nodeInfo.getPort()), 5000);
			socket.setSoTimeout(10000);

			ObjectOutputStream oos = new ObjectOutputStream(
					socket.getOutputStream());

			ObjectInputStream ois = new ObjectInputStream(
					socket.getInputStream());

			// sends our message
			oos.writeObject(message);

			fos = new ByteArrayOutputStream();

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

							if (messageOver
									.contains(FileUtils.TRANSFER_OVER_PREFIX)) {

								oos.writeObject(FileUtils.TRANSFER_OK_PREFIX);

								ois.close();
								oos.close();

								System.out.println(String.format("File block downloaded : file [%s] ",filename));
							}
						} catch (ClassNotFoundException e){
							
						}
						catch (IOException e) {
							// TODO Auto-generated catch block
							System.out
									.println("Error during file block transfer");
							e.printStackTrace();
						}

						break;
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			socket.close();
			
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return fos;
	}
}
