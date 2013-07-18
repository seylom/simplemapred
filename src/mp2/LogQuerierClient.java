package mp2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;


public class LogQuerierClient {
	
	public HashMap<String, MachineInfo> logFilesToMachineMap;
	public ArrayList<GrepProtocol> grepThreads;
	
	
	private final String log1 = "machine.1.log";
	private final String log2 = "machine.2.log";
	private final String log3 = "machine.3.log";
	
	private final String linux5 = "linux5.ews.illinois.edu";
	private final String linux6 = "linux6.ews.illinois.edu";
	private final String linux7 = "linux7.ews.illinois.edu";

	/**
	 * Default querier constructor
	 */
	public LogQuerierClient() {
		this(false);
	}

	/**
	 * local: a boolean indicating if the client and servers are deployed
	 * locally
	 */
	public LogQuerierClient(boolean local,int port) {
		 
		if (local) {
			
			String lg1 = "localhost-" + port + ".log";
			String lg2 = "localhost-" + (port+1) + ".log";
			String lg3 = "localhost-" + (port+2) + ".log";
			
			// initialize log file mapping
			logFilesToMachineMap = new HashMap<String, MachineInfo>();
			logFilesToMachineMap.put(lg1, new MachineInfo("localhost", port,
					lg1));
			logFilesToMachineMap.put(lg2, new MachineInfo("localhost", (port+1),
					lg2));
			logFilesToMachineMap.put(lg3, new MachineInfo("localhost", (port+2) ,
					lg3));
		} else {
			
			// mapping on real server
			logFilesToMachineMap = new HashMap<String, MachineInfo>();
			logFilesToMachineMap.put(log1, new MachineInfo(
					linux5, port, log1));
			logFilesToMachineMap.put(log2, new MachineInfo(
					linux6, port, log2));
			logFilesToMachineMap.put(log3, new MachineInfo(
					linux7, port, log3));
		}
	}
	
	/**
	 * local: a boolean indicating if the client and servers are deployed
	 * locally
	 */
	public LogQuerierClient(boolean local) {
		this(local,10000);
	}


	/**
	 * @param machineList
	 */
	public LogQuerierClient(ArrayList<MachineInfo> machineList){
		logFilesToMachineMap = new HashMap<String, MachineInfo>();
		
		for(MachineInfo machine: machineList){
			logFilesToMachineMap.put(machine.logName, machine);
		}
	}

	/**
	 * Reads user input on the terminal, breaks it down into grep commands to be
	 * executed on machines and submit requests to server for respective grep
	 * operations on log files
	 */
	public void processInput() {

		//read terminal input
		Scanner scan = new Scanner(System.in);
		String[] grepArgs;
		ArrayList<MachineInfo> machinesToGrep = new ArrayList<MachineInfo>();

		do {
			machinesToGrep.clear();

			System.out.println("Enter a distributed grep command:");
			System.out.println("syntax: grep 'text to find' logfilenames");

			String input = scan.nextLine();
			grepArgs = input.split(" ");
			
			if (grepArgs.length == 0){
				System.out.println("Invalid grep command syntax.");
			}
			
			if (grepArgs[0].equals("exit")){
				break;
			}
			
			if (grepArgs.length <3  || 
			    (!grepArgs[0].equals("grep"))) {
				System.out.println("Invalid grep command syntax.");
			}

			if (grepArgs.length == 1 && (grepArgs[0].equals("exit"))) {
				break;
			}else if (grepArgs.length < 3) {
				System.out.println("Incorrect nummber of argument provided.");
			}

			if (grepArgs.length > 5) {
				System.out.println("Too many argument provided."
						+ " please only enter a maximum of three log files");
			}

			// modify this line to add more checks of the input
			// verify log file names provided
			if (grepArgs.length > 2 && grepArgs.length <= 5
					&& grepArgs[0].equals("grep")) {

				for (int i = 2; i < grepArgs.length; i++) {

					if (logFilesToMachineMap.containsKey(grepArgs[i]))
						machinesToGrep.add(logFilesToMachineMap
								.get(grepArgs[i]));
				}

				if (machinesToGrep.size() == 0) {
					System.out
							.println("No machine found with specified log files");
				} else {
					initiateGrepRequest(grepArgs[1], machinesToGrep);
				}
			}
		} while (true);

		scan.close();
	}

	/**
	 * Starts the grep request accross specified machines
	 */
	public void initiateGrepRequest(String grepStringToMatch,
			ArrayList<MachineInfo> machinesToQuery) {

		long start = System.nanoTime();
		grepThreads = new ArrayList<GrepProtocol>();
		
		for (MachineInfo machine : machinesToQuery) {
			 GrepProtocol t = new GrepProtocol(machine,grepStringToMatch);
		     grepThreads.add(t);
		}
		
		for (GrepProtocol t : grepThreads) {
			try{
				t.taskThread.join();
			}catch(InterruptedException e){
				//e.printStackTrace();
			}	
		}
		
		long elapsedTime = System.nanoTime() - start;
		System.out.println(String.format("Elapsed time:  %d milliseconds",
				elapsedTime / (1000 * 1000)));
		
		System.out.println("----------------------------------------------");
	}

	/**
	 * Entry point of the client.
	 * 
	 * When this method is invoked, servers should already be up and running
	 */
	public static void main(String[] args) {
		LogQuerierClient client = null;
		
		if (args.length == 0){
			client = new LogQuerierClient();
		}
		else{
			
			int port  = 10000;
			boolean local = false;
			
			if (args.length >= 1 && args[0].equals("local")){
				local = true;
			}
			
			if (args.length >= 2){
				port = Integer.parseInt(args[1]);
			}

			client = new LogQuerierClient(local,port); 
		}

		client.processInput();
	}
}
