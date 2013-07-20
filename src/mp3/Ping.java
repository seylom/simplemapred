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
	private Thread currentThread;
	
	
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
			
			currentThread = new Thread(this);
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
		
		currentThread = new Thread(this);
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
	
	public void autoRun(){
		currentThread.run();
	}
	
	public void stop(){
		currentThread.interrupt();
	}
	
	/**
	 * Sends a ping to target machine every PING_FREQ ms, then waits for ack response.
	 * Upon waiting for more than TIMEOUT_TIME ms, failure is indicated.
	 */
	@Override
	public void run(){
		while (!stop && !Thread.interrupted()) {
			// send ping
			// wait PING_FREQ then repeat			
					 
			client.resetAckReceived();
			Helper.sendUnicastMessage(socket, MessageHandler.PING_MESSAGE, address.getHostName(), port);
			
			try {
				Thread.sleep(PING_FREQ);
				
				if (!client.getAckReceived() && !stop){
					failure = true;
					stop = true;
					client.notifyUnreachable();
				}		
			} catch (InterruptedException e) {
				System.out.println("Sleep interrupted");
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

	public void waitStop() {
		try {
			currentThread.join();
		} catch (InterruptedException e) {
		}
	}
	
}
