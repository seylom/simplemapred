package mp3;
import java.io.IOException;
import java.net.*;

public class Ping implements Runnable {

	private DatagramSocket socket;
	private InetAddress address;
	private int port;
	private boolean failure;
	
	private final static int PING_FREQ = 2000; 		// time between successive pings
	private final static int TIMEOUT_TIME = 3000;	// limit on how long to wait for ack response
	
	public volatile boolean stop = false;
	private ClientNode client;
	
	
	/**
	 * Set up connection info for machine to be pinged	
	 * @param hostname	hostname of machine to be pinged
	 * @param port		port of machine to be pinged
	 */
	public Ping(String hostname,int port) {
		failure = false;
		
		try {
			address = InetAddress.getByName(hostname);
			this.port = port;
			socket = new DatagramSocket();
			socket.setSoTimeout(TIMEOUT_TIME);	
			
			//start();			
		}
		
		catch (IOException e) {
			System.out.println("Ping initiation error");
		}
	}
		
	/**
	 * @param socket
	 * @param client
	 */
	public Ping(DatagramSocket socket,ClientNode client) {
		failure = false;
		this.socket = socket;
		this.client = client;
	}
	
	/**
	 * @param hostname
	 * @param port
	 */
	public void Initialize(String hostname, int port){
		//failure = false;
		stop = false;
		try{	
			address = InetAddress.getByName(hostname);
			this.port = port;		
		}catch(IOException ex){
			System.out.println("Ping initialization error -- ");
		}
	}
	
	public void run() {
		pingLoop();
		
		(new Thread(){
			@Override
			public void run(){
				pingLoop();
			}
		}).start();
	}	
	
	/**
	 * Sends a ping to target machine every PING_FREQ ms, then waits for ack response.
	 * Upon waiting for more than TIMEOUT_TIME ms, failure is indicated.
	 */
	private void pingLoop() {
		while (!stop) {
			// send ping
			// wait PING_FREQ then repeat			
					 
			client.resetAckReceived();
			Helper.sendUnicastMessage(socket, MessageHandler.PING_MESSAGE, address.getHostName(), port);
			
			try {
				Thread.sleep(PING_FREQ);
			} catch (InterruptedException e) {
				System.out.println("Sleep interrupted");
			}
			 
			if (!client.getAckReceived()){
				failure = true;
				stop = true;
				client.notifyUnreachable();
			}
		}
	}
	
	/**
	 * Return failure status
	 * @return	true upon failure, false otherwise
	 */
	public boolean getFailure() {
		return failure;
	}
	
}
