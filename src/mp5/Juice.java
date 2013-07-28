package mp5;

import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.HashMap;


public class Juice {
	
	private int fileCount;
	private int numJuices;
	private String prefix;
	private String destPath;
	private ArrayList<String> fileList;
	// Mapping of juice task number to keys it is responsible for
	private static HashMap<Integer, ArrayList<String>> jobMap;
	
	/** This will need to be changed - used for local testing  **/
	private static String localPath = "/tmp/ayivigu2_kjustic3/maplejuice/map_keyfiles/"; //"C:/Users/pie/workspace/mp5";
	
	
	/**
	 * @param numJuices
	 * @param prefix
	 * @param destPath
	 */
	public Juice(int numJuices,String prefix,String destPath){
		this.numJuices =numJuices;
		this.prefix = prefix;
		this.destPath = destPath;
		
		fileCount = countFiles();
		
		divideJobs();
	}
	
	/**
	 * Class entry point
	 * @param args	command line arguments
	 */
	public static void main(String args[]) {
		// Command line checks
		if (args.length != 4) {
			System.out.println("Usage: java Juice <juice_exe> <num_juices>" +
					" <file_prefix> <destination_file>");
			return;
		}
		
		if (Integer.parseInt(args[1]) < 1) {
			System.out.println("Number of juice tasks cannot be less than 1.");
			return;
		}
		
		// Initialization of key variables
		int numJuices = Integer.parseInt(args[1]);
		String prefix = args[2];
		String destPath = args[3];
		if (new File(destPath).exists()) {
			new File(destPath).delete();	// Because WordCount appends to destination file
		}
		
				
		// System.out.println("Files starting with prefix " + prefix + ": " + fileCount + "\n");
		
		
		/*for (int i = 0; i < jobMap.size(); i++) {
			System.out.println("Key[" + i + "]:");
			for (int j = 0; j < jobMap.get(i).size(); j++) {
				System.out.println("\tArrayList[" + j + "]: " + jobMap.get(i).get(j));
			}
		}*/
		
		Juice jc = new Juice(numJuices, prefix, destPath);
		jc.doJuice();
	}

	/**
	 * Counts number of files beginning with SDFS prefix
	 * @return number of files beginning with SDFS prefix
	 */
	private int countFiles() {
		File directory = new File(localPath);
		
		// Filter for files beginning with prefix
		FilenameFilter fileNameFilter = new FilenameFilter() {			   
            @Override
            public boolean accept(File dir, String name) {
            	if (name.startsWith(prefix)) {
            		return true;
            	}
               return false;
            }
		};
		
		// Add each file to an array list
		fileList = new ArrayList<String>();
		String[] fileListArray = directory.list(fileNameFilter);
		int i = 0;
		for(String str : fileListArray) {
			fileList.add(str);
			// System.out.println("fileListArray[" + i + "]:" + fileList.get(i));
			i++;
		}
		
		return fileList.size();
	}
	
	/**
	 * Assigns keys to juice tasks, placing results in jobMap
	 */
	private void divideJobs() {
		jobMap = new HashMap<Integer, ArrayList<String>>();
		int jobsPerJuice = (int) Math.ceil((double) fileCount / numJuices);
		ArrayList<String> tempKeyList;
		
		for (int i = 0; i < numJuices; i++) {
			tempKeyList = new ArrayList<String>();
			for (int j = 0; j < jobsPerJuice; j++) {
				if ((i * jobsPerJuice + j) >= fileList.size())
					break;
				tempKeyList.add(fileList.get(i*jobsPerJuice + j));
			}
			if (tempKeyList.isEmpty())
				break;
			jobMap.put(i, tempKeyList);
		}
	}
	
	/**
	 * Execute the given juice task (in this case, call WordCount)
	 * for each 
	 */
	public void doJuice() {
		for (int i = 0; i < jobMap.size(); i++) {
			for (int j = 0; j < jobMap.get(i).size(); j++) {
				new WordCountJuice(String.format("%s/%s",localPath,jobMap.get(i).get(j)), destPath);
			}
		}	
	}
}
