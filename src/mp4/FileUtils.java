package mp4;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;

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

	/**
	 * @param info
	 * @return
	 * 
	 *         Retrieve storage path for Sdfs save/read operations
	 */
	public static String getStoragePath(NodeInfo info) {
		return String.format("/tmp/ayivigu2_kjustic3/%s-%d",
				info.getHostname(), info.getPort());
	}

	/**
	 * @param file
	 * @param nodeInfo
	 * 
	 *            send a file to a host
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
}
