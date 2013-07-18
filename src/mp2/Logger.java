package mp2;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException; 
import java.util.Date;

import mp3.Helper;

/**
 * basic logger class
 */
public class Logger {

	private final String hostname;
	private final int port;
	private final static String pathPrefix = "/tmp/ayivigu2_kjustic3/";
	private final String filename;	
	public volatile boolean DEBUG = true;

	/**
	 * 
	 * @param hostname
	 * @param port
	 */
	public Logger(String hostname,int port){
		this(hostname,port,
				pathPrefix + hostname + "-" + port + ".log");
	}
	
	/**
	 * 
	 * @param hostname
	 * @param port
	 * @param logname
	 */
	public Logger(String hostname,int port, String logname){
		this.hostname = hostname;
		this.port = port;
		
		filename = pathPrefix + logname;
		
		File f = new File(filename);
		
		// sets up log file streams
		try {
		    if (!f.getParentFile().exists())
		     	f.getParentFile().mkdirs();
		     
		    BufferedWriter writer = new BufferedWriter(new FileWriter(new File(filename)));
		    writer.close();
			
		} catch (IOException e) {
			System.out.println("Log file could not be created, using standard out instead."); 
		}
	}
	
	/**
	 * logs messages
	 */
	public synchronized void log(String message){
		 
		String result = log_info(message);
		
		try {
			BufferedWriter writer = new BufferedWriter(new FileWriter(new File(filename),true));
			writer.write(result + "\n");
			writer.flush();
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		if (DEBUG){
			System.out.println(result);
		}
	}
	
	/**
	 * @param message
	 * @return a logged message
	 */
	public synchronized String log_info(String message){
		Date date = new Date();
		String currentDate =  Helper.dateFormat.format(date);
		
		return String.format("%s - %s:%d> %s",currentDate,hostname,port,message);
	}
}
