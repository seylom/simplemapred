package mp2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader; 
import java.io.ObjectOutputStream;
import java.util.ArrayList; 

/*
 * handles all grep command invocations
 */
public class GrepHandler {
	
	public ArrayList<String> results = new ArrayList<String>();
	
	/**
	 * 
	 */
	public GrepHandler()
	{
		
	}

	/**
	 * Runs grep and sends back all results as a single output
	 */
	public String grep(String command,String header,ObjectOutputStream oos) throws IOException {
		
		results.clear();
		
		//return "grep command received! Processing...";
		if (command.startsWith("grep")){
			
			String[] args = command.split(" ");
			
			if (args.length < 3){
				return String.format("%s - invalid grep syntax",header);
			}
				
			
			String pattern = args[1].replace("'","").replace("\"","");
			String logName = args[2];
			
			String fileToSearch = String.format("/tmp/ayivigu2_kjustic3/%s",logName);
			
			BufferedReader reader = null;
			
			try {
			    // Execute a command without arguments
			    String[] commandArgs = new String[] {"grep",pattern,fileToSearch};
			    
			    ProcessBuilder builder = new ProcessBuilder(commandArgs);
			    builder.redirectErrorStream(true);
			    Process process = builder.start();
			    		
			    InputStream stdout = process.getInputStream ();		    
			    reader = new BufferedReader (new InputStreamReader(stdout));
			    
			    String line;
			    String output = "";

			    while ((line = reader.readLine ()) != null) { 
			    	String grepResult = String.format("%s - %s\n",header, line);
			    	output += String.format("%s - %s\n",header, line);

			    	results.add(grepResult);
			    }
			    
			    return output;
			    
			} catch (IOException e) {
				return String.format("%s - Unable to execute grep on the specified file",header);
			}
			finally{
				if (reader!=null){
					reader.close();
				}			
			}
		}
		//return "Completed.";		
		return String.format("%s - Completed",header);
	}

	public void grepInline(String command, String header,
			ObjectOutputStream oos) throws IOException {
		 
		results.clear();
		
		//return "grep command received! Processing...";
		if (command.startsWith("grep")){
			
			String[] args = command.split(" ");
			
			if (args.length < 3){
				oos.writeObject(String.format("%s - invalid grep syntax",header));
				oos.writeObject("completed");
				
				return;
			}		
			
			String pattern = args[1].replace("'","").replace("\"","");
			String logName = args[2];
			
			String fileToSearch = String.format("/tmp/ayivigu2_kjustic3/%s",logName);
			
			BufferedReader reader = null;
			
			try {
			    // Execute a command without arguments
				    
			    String[] commandArgs = new String[] {"grep",pattern,fileToSearch};
			    
			    ProcessBuilder builder = new ProcessBuilder(commandArgs);
			    builder.redirectErrorStream(true);
			    Process process = builder.start();
			    		
			    InputStream stdout = process.getInputStream ();		    
			    reader = new BufferedReader (new InputStreamReader(stdout));
			    
			    String line; 
			    
			    String dataBuffer = "";
			    
			    //send data every multiple of 5*4K bytes
			    
			    while ((line = reader.readLine ()) != null) { 
			    	String grepResult = String.format("%s - %s\n",header, line); 
			    	dataBuffer += String.format("%s - %s\n",header, line);
			    	
			    	byte[] bytes = dataBuffer.getBytes("UTF-8");
			    	if (bytes.length*8 > 4096*5){
			    		
			    		//flush
			    		oos.writeObject(dataBuffer);
			    		
			    		dataBuffer = "";
			    	}
			    	
			    	results.add(grepResult);
			    }
			    
			    //flush the remaining data
			    if (dataBuffer.length() > 0)
			    	oos.writeObject(dataBuffer);
			    
			    oos.writeObject("completed");
			    
			    return;
			    
			} catch (IOException e) {
				System.out.println(String.format("%s - Unable to execute grep on the specified file",header));
			}
			finally{
				if (reader!=null){
					reader.close();
				}			
			}
		}
		
		oos.writeObject("completed");
		
		return;
	}
}
