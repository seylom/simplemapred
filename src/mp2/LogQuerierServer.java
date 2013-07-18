package mp2;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

/*
 * Server code
 */
public class LogQuerierServer {
	
	private ServerSocket serverSocket; 
	
	/**
	 * default constructor on port 10000
	 */
	public LogQuerierServer(String machineName,String logname){
		//default port: 10000
		this(machineName,10000,logname);
	}
	
	/**
	 * This constructor is mostly used for testing purposes when simulating
	 * multiple servers on localhost
	 * 
	 */
	public LogQuerierServer(String machineName,int port, String logname){	
 
		try{
			serverSocket = new ServerSocket(port);
		}catch(IOException e){
			System.out.println(String.format("Error listening on port: %d", port));
			e.printStackTrace();
		}
	}
	
	/**
	 * Start listening to incoming connections
	 */
	public void listen(){
		while(true){
			try{
				Socket socket = serverSocket.accept();
				new TcpConnectionManager(socket);
			}catch(IOException e){ 
				//e.printStackTrace();
				System.out.println("Unable to accept to the incoming connection request");
			}		
		}
	}
}
