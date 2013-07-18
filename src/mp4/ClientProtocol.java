package mp4;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;

import mp3.NodeInfo;

public class ClientProtocol {

	public static String PUT_OP = "put_op";
	public static String GET_OP = "get_op";
	public static String DEL_OP = "del_op";
	public static String BLOCK_REQUEST = "block_request";

	public static boolean stop = false;

	private String masterHost;
	private int masterPort = 10000;

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
	public synchronized void initialize(String hostname, int port) {
		masterHost = hostname;
		masterPort = port;
	}

	public String sendMessageToMasterNodeWithResponse(String fileOp,
			String message) throws ClassNotFoundException {

		System.out.println(String.format(
				"Client - Sending %s message to <%s:%d>", fileOp, masterHost,
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
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return result;
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
	public String downloadFile(String filename, NodeInfo nodeInfo) {

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
}
