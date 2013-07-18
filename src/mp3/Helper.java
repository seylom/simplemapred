package mp3;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketTimeoutException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList; 
import java.util.List;

public class Helper {

	public static final DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
	
	/*
	 * 
	 */
	public static String byteArrayToHexString(byte[] b) {
		String result = "";
		for (int i = 0; i < b.length; i++) {
			result += Integer.toString((b[i] & 0xff) + 0x100, 16).substring(1);
		}
		return result;
	}

	/*
	 * creates a string identifier for the node
	 */
	public static String generateNodeId(String hostname, int port) {

		String dateString = Long.toString(System.currentTimeMillis());
		String idString = dateString + "|" +  hostname +  "|" + Integer.toString(port);
		idString = idString.replace(" ", "");
		
		return idString;
	}
	
	/*
	 * creates a string identifier hash for the node
	 */
	public static String generateNodeHash(String hostname, int port,int length) {

		String dateString = Long.toString(System.currentTimeMillis());

		String idString = dateString + hostname + Integer.toString(port);

		MessageDigest md = null;

		try {
			md = MessageDigest.getInstance("SHA-1");
		} catch (NoSuchAlgorithmException ex) {
			ex.printStackTrace();
		}

		String id = byteArrayToHexString(md.digest(idString.getBytes()));
		
		if (id.length() > length)
			id = id.substring(0, length);

		return id;
	}
	
	/*
	 * 
	 */
	public static NodeInfo extractNodeInfoFromId(String nodeId){
		
		//System.out.println(nodeId);
		
		String [] info = nodeId.split("\\|",3);
		
		String timestamp = info[0];
		String hostname = info[1];
		int port = Integer.parseInt(info[2]);
		
		return new NodeInfo(hostname, port, timestamp);
	}
	
	/**
	 * 
	 */
	public static String join(List<String> list,String separator) {
	    String delim = "";
	    StringBuilder sb = new StringBuilder();
	    for (String i : list) {
	        sb.append(delim).append(i);
	        delim = separator;
	    }
	    
	    return sb.toString();
	}
	
	/**
	 * 
	 */
	public static String join(List<String> list) {
	    return join(list,",");
	}
	
	/**
	 * Sends unicast message to the specified host and port
	 */
	public static void sendUnicastMessage(DatagramSocket socket,final String message, String hostname, int port){
		try{
			byte [] outBuf = message.getBytes();
				
			DatagramPacket outPacket = new DatagramPacket(outBuf, 0, outBuf.length, 
					InetAddress.getByName(hostname), port);
			
			socket.send(outPacket); 
		}catch(SocketTimeoutException ex){
			System.out.println("Unable to contact the introducer node.");
		}catch(IOException ex){
			ex.printStackTrace();
		}
	}
	
	/**
	 * Sends a B multicast message to listed recipients
	 */
	public synchronized static void sendBMulticastMessage(final DatagramSocket socket,final String message, ArrayList<String> recipients){
			for(String memberId:recipients){
				final String id = memberId;
				
				(new Thread(){
					public void run(){
						NodeInfo memberInfo = Helper.extractNodeInfoFromId(id);
						
						Helper.sendUnicastMessage(socket, message,
								memberInfo.getHostname(), memberInfo.getPort()) ;
						
				}}).start();		 
		}
	}
}
