package mp4;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.ArrayList;

import mp3.Helper;
import mp3.NodeInfo;

public class ClientProtocol {

	public static String HELLO_MASTER = "hello_master";
	public static String PUT_OP = "put_op";
	public static String GET_OP = "get_op";
	public static String DEL_OP = "del_op";
	public static String BLOCK_REQUEST = "block_request";
	public static String MAPLE_OP = "maple";
	public static String JUICE_OP = "juice";
	public static String END_OF_COMM = "completed";
	public static String JOB_STATUS = "job_status";

	public static boolean stop = false;

	private String masterHost;
	private int masterPort = 10000;
	
	private String secondHost;
	private int secondPort = 10000;
	
	private boolean attemptedSecondary;

	/**
	 * @param socket
	 *            Initializes a new instance of the client protocol, used to
	 *            handle operations initiated by the client
	 */
	public ClientProtocol() {

	}

	/**
	 * @param hostname
	 * @param port
	 *            initializes the master node information for communications
	 */
	public synchronized void initializePrimary(String hostname, int port) {
		masterHost = hostname;
		masterPort = port;
	}
	
	/**
	 * @param hostname
	 * @param port
	 *            initializes the master node information for communications
	 */
	public synchronized void initializeSecondary(String hostname, int port) {
		secondHost = hostname;
		secondPort = port;
	}

	public String sendMessageToMasterNodeWithResponse(String fileOp,
			String message) throws ClassNotFoundException {

		System.out.println(String.format(
				"Client - Sending %s command to <%s:%d>", fileOp, masterHost,
				masterPort));

		String result = "";

		try {
			Socket socket = new Socket();

			socket.connect(new InetSocketAddress(masterHost, masterPort), 5000);
			socket.setSoTimeout(10000);

			ObjectOutputStream oos = new ObjectOutputStream(
					socket.getOutputStream());

			ObjectInputStream ois = new ObjectInputStream(
					socket.getInputStream());

			// sends our message
			oos.writeObject(message);

			Object serverMessage;

			while (true) {
				serverMessage = ois.readObject();

				if (serverMessage.equals("completed"))
					break;

				result += serverMessage;
			}

			ois.close();
			oos.close();
			socket.close();

		} catch (UnknownHostException e) {
			System.out.println(String.format("The host <%s:%d> cannot be found." +
					" Please check your master hostname and port",masterHost,masterPort));			
		} catch (SocketTimeoutException e){
			result = attempOperationWithSecondary(fileOp,message);
		}
		catch (IOException e) {
			result = attempOperationWithSecondary(fileOp,message);
		}

		return result;
	}
	
	/**
	 * @param fileOp
	 * @param message
	 * @return
	 * @throws ClassNotFoundException
	 */
	private String attempOperationWithSecondary(String fileOp,
			String message) throws ClassNotFoundException{
		
		if (!attemptedSecondary){
			attemptedSecondary = true;
			
			System.out.println("Client - A problem occur during communication with the master node");
			System.out.println("Client - Attempting command with secondary master");
			
			masterHost = secondHost;
			masterPort = secondPort;
			
			return sendMessageToMasterNodeWithResponse(fileOp,message);
		}
		else{
			System.out.println("Client - Unable to communicate with primary or secondary.");
		}
		
		return "";
	}

	/**
	 * @throws ClassNotFoundException
	 * @param numberOfNodes
	 *            : number of chunks to be saved retrieves from the master node
	 *            candidate SdfsNodes selected for file put operation
	 * 
	 */
	public String getDestinationNodes(int chunkCount)
			throws ClassNotFoundException {
		String message = String.format("%s:%d", PUT_OP, chunkCount);
		String result = sendMessageToMasterNodeWithResponse("put", message);

		return result;
	}

	/**
	 * @throws ClassNotFoundException
	 * @param numberOfNodes
	 *            : number of chunks to be saved retrieves from the master node
	 *            candidate SdfsNodes selected for file put operation
	 * 
	 */
	public String deleteFile(String sdfsFileName) throws ClassNotFoundException {

		String fileName = sdfsFileName.replace("/", "_");

		String message = String.format("%s:%s", DEL_OP, fileName);
		String result = sendMessageToMasterNodeWithResponse("delete", message);

		return result;
	}

	/**
	 * @param sdfsFileName
	 * @throws ClassNotFoundException
	 * 
	 *             Gets a file from Sdfs
	 */
	public String getFile(String sdfsFileName) throws ClassNotFoundException {

		String fileName = sdfsFileName.replace("/", "_");

		String message = String.format("%s:%s", GET_OP, fileName);

		String result = sendMessageToMasterNodeWithResponse("get", message);

		return result;
	}

	/**
	 * @param filename
	 * @param nodeInfo
	 */
	public static String downloadFile(String filename, NodeInfo nodeInfo) {

		String filepath = "";
		String message = String.format("%s:%s", BLOCK_REQUEST, filename);

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

			String folderPath = "/tmp/ayivigu2_kjustic3/";

			File folder = new File(folderPath);
			if (!folder.exists())
				folder.mkdirs();

			filepath = String.format("%s/%s", folderPath, filename);

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

							if (messageOver
									.contains(FileUtils.TRANSFER_OVER_PREFIX)) {

								oos.writeObject(FileUtils.TRANSFER_OK_PREFIX);

								ois.close();
								oos.close();

								System.out.println(String.format("Client - File block downloaded : file [%s] ",filename));
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

		return filepath;
	}
	
	/**
	 * @param exe
	 * @param prefix
	 * @param fileListString
	 * @return
	 * @throws ClassNotFoundException
	 */
	public String sendMapleMessage(String exe,String prefix,ArrayList<String> files) throws ClassNotFoundException{
	 
		String fileListString =  Helper.join(files,FileUtils.LIST_DELIM); 
		
		String message = String.format("%s:%s:%s:%s", MAPLE_OP, exe,prefix,fileListString);
		String result = sendMessageToMasterNodeWithResponse("maple", message);

		return result;
	}
	
	/**
	 * @param exe
	 * @param prefix
	 * @param fileListString
	 * @return
	 * @throws ClassNotFoundException
	 */
	public String sendJuiceMessage(String exe,int numberOfJuices,String prefix,String destination) throws ClassNotFoundException{
	 
		String message = String.format("%s:%s:%d:%s:%s", JUICE_OP, exe,
				numberOfJuices,prefix,destination);
		
		String result = sendMessageToMasterNodeWithResponse("juice", message);

		return result;
	}

	/**
	 * Send an executable program to the master node
	 * @param exe
	 */
	public void sendProgram(String exe) {	
		File exeFile = new File(exe);
		FileUtils.uploadProgram(exeFile, this.masterHost,this.masterPort);
	}

	/**
	 * @return
	 * @throws ClassNotFoundException
	 */
	public String getActiveJobStatus() throws ClassNotFoundException {
	 
		String message = String.format("%s", JOB_STATUS);
		String result = sendMessageToMasterNodeWithResponse("job status request", message);
		
		return result;
	}
	
	
	public String helloMaster() throws ClassNotFoundException{
		String message = String.format("%s", HELLO_MASTER);
		String result = sendMessageToMasterNodeWithResponse("Request for secondary master", message);
		
		return result;
	}
}
