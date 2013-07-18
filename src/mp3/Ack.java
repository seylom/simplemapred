package mp3;
import java.io.IOException;
import java.net.*; 

public class Ack implements Runnable{
	
	//private InetAddress addr;
	private int port;
	private DatagramSocket socket;
	private DatagramPacket inPacket, outPacket;
	private byte[] inBuf, outBuf;
	private InetAddress sourceAddr;
	private int sourcePort;
	private String msg;
	
	public volatile boolean stop = false;
	

	/**
	 * Set up connection information
	 */
	public Ack(int port) {
		try { 
			this.port = port;
			socket = new DatagramSocket(this.port);		 		
		}
		
		catch (IOException e) {
			System.out.println("Ack intitiation error");
			e.printStackTrace();
		}
		
		Thread t = new Thread(this);
		t.start();
	}
	
	/**
	 * Set up connection information
	 */
	public Ack(DatagramSocket socket) {
			this.socket = socket;
			 
			Thread t = new Thread(this);
			t.start();
	}
	
	/**
	 * Wait for ping request and deliver ack response once received
	 */
	@Override
	public void run() {
		try {
			while (!stop) {
				// wait for ping
				inBuf = new byte[256];
				inPacket = new DatagramPacket(inBuf, inBuf.length);
				socket.receive(inPacket);
				
				// get client address info
				sourcePort = inPacket.getPort();
		        sourceAddr = inPacket.getAddress();
				
				// check for message receipt
				msg = new String(inPacket.getData(), 0, inPacket.getLength());
				if (msg.trim().equals("p")) {
					outBuf = new String("a").getBytes();
					outPacket = new DatagramPacket(outBuf, 0, outBuf.length,
							sourceAddr, sourcePort);
					socket.send(outPacket);
					System.out.println("ack sent to " + sourceAddr.getHostName() + ":" + sourcePort);
					msg = new String("invalid");
				}

				// announce corrupted packet
				else {
					System.out.println("packet corrupted.");
					continue;
				}				
			}
		}
		
		catch (IOException e) {
			System.out.println("Ack loop error");
		}
	}
}
