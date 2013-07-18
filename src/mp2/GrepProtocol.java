package mp2;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;


/*
 * Protocol used to communicate back and forth between client and a single server for
 * a grep operation, from the initialization of a connection to 
 * the display of grep result for the server machine to which we are connected
 */
public class GrepProtocol implements Runnable {

	private MachineInfo machine;
	private String grepStringToMatch;
	public Thread taskThread; 
	public ArrayList<String> grepResults;
	
	/*
	 * Constructor for the protocol
	 */
	public GrepProtocol(MachineInfo theMachine, String grepString) {
		machine = theMachine;
		grepStringToMatch = grepString;

		taskThread = new Thread(this);
		taskThread.start();
	}
	
	/*
	 * Protocol implementation
	 * @see java.lang.Runnable#run()
	 */
	public void run() {
		
		grepResults = new ArrayList<String>();

		// start a thread for each machine instead. otherwise this is a
		// blocking call
		try {

			String header = String.format("%s:%d", machine.hostName,
					machine.port);

			Socket socket = new Socket();
			
			//5 seconds timeout for connections
			socket.connect(new InetSocketAddress(machine.hostName, machine.port),5000) ;
			
			//10 seconds for timeout of read operations between client and server
			socket.setSoTimeout(10000);
			
			//System.out.println(String.format("Successfull connection to %s:%d",
			//		machine.hostName, machine.port));

			// Send message to server
			ObjectOutputStream oos = new ObjectOutputStream(
					socket.getOutputStream());

			// Read and display the response message sent by server
			// application
			ObjectInputStream ois = new ObjectInputStream(
					socket.getInputStream());
 
			//sends to server the header to prefix grep results
			oos.writeObject(header);

			//sends our grep command
			oos.writeObject(String.format("grep %s %s", grepStringToMatch,
					machine.logName));

			Object serverMessage;
 
			while(true){
				serverMessage = ois.readObject();
				
				if (serverMessage.equals("completed"))
					break;
				
				System.out.println((String)serverMessage);
			}		
			
			grepResults.addAll(Arrays.asList(((String)serverMessage).split("\\n")));

			ois.close();
			oos.close();

			socket.close();
		} catch (UnknownHostException e) {
			//e.printStackTrace();
			System.out.println(String.format(
					"Unable to connect to %s for log %s", machine.hostName,
					machine.logName));
		} catch (SocketTimeoutException e) {
			System.out.println(String.format("Timeout occured for %s",machine.hostName));
		}catch (IOException e) {
			//e.printStackTrace();
			System.out.println(String.format(
					"Unable to connect to port %d on machine %s", machine.port,
					machine.hostName));
		} catch (ClassNotFoundException e) {
			//e.printStackTrace();
		}
		finally{
			
		}
	}
}
