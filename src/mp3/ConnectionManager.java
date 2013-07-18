package mp3;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketTimeoutException;
import java.util.Random;

/**
 * Manages incoming connections to 
 */
public class ConnectionManager implements Runnable {

	private DatagramSocket socket;
	public volatile boolean stop;
	private ClientNode client;	
	private final int MESSAGE_LOSS_RATE;
	//private static final int seed = 1234;
	private final Random random;
	
	/**
	 * Initializes a new instance of the connection manager
	 */
	public ConnectionManager(DatagramSocket socket,ClientNode client){
		this(socket,client,0);
	}
	
	/**
	 * Initializes a new instance of the connection manager
	 */
	public ConnectionManager(DatagramSocket socket,ClientNode client,int lossRate){
		this.socket = socket;
		this.client = client;
		this.MESSAGE_LOSS_RATE = lossRate;
		random = new Random();
		
		Thread t = new Thread(this);
		t.start();
	}
	
	/**
	 * 
	 */
	@Override
	public void run() {
		try{
			while(!stop){
	
				byte[] inBuf = new byte[1024];
				DatagramPacket inPacket = new DatagramPacket(inBuf, inBuf.length);
				socket.receive(inPacket);
				
				int rd = random.nextInt(101); 
			 
				if (rd < MESSAGE_LOSS_RATE) {
					System.out.println("Packet loss - ");
				}else{
					//new MessageHandler(socket,inPacket,client);	
					client.getMessageHandler(socket, inPacket, client);
				}	
			} 
		}catch(SocketTimeoutException ex){
			System.out.println(String.format("Connection to %s:%d timed out",client.getHostname(), 
					client.getNodePortNumber()));
		} 
		catch(IOException ex){
			System.out.println(String.format("Unable to receive messages on the port %d",
					client.getNodePortNumber()));
		} 
	}
}
