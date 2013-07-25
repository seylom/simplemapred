package mp5;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class WordCountJuice {
	
	private static File inFile;
	private static File outFile;
	private static HashMap<String, Integer> map;

	/**
	 * Class entry point
	 * @param args command line arguments
	 */
	public static void main(String args[]) {
		// Syntax check
		if (args.length != 2) {
			System.out.println("Usage: WordCount <in_file> <out_file>");
			return;
		}
		
		inFile = new File(args[0]);
		outFile = new File(args[1]);	
		map = new HashMap<String, Integer>();
		
		// Make sure file is valid
		if (inFile.length() < 1 || !inFile.exists() || !inFile.isFile()) {
			System.out.println(args[0] + " is not a valid file.");
			return;
		}
		
		doJob();
	}
	
	// For testing purposes only - main should be the entry point since this
	// will be run as an executable.
	public WordCountJuice(String inFile, String outFile) {
		this.inFile = new File(inFile);
		this.outFile = new File(outFile);
		map = new HashMap<String, Integer>();
		doJob();
	}
	
	/**
	 * Sums up occurrences of keys and outputs totals to destination file
	 */
	public static void doJob() {
		
		try {
			BufferedReader br = new BufferedReader(new FileReader(inFile));
			BufferedWriter bw = new BufferedWriter(new FileWriter(outFile, true));
			String line;

			while ((line = br.readLine()) != null) {
				String results[] = line.split(",");
				if (results.length != 2) {
					System.out.println("Invalid formatting of intermediate file");
					return;
				}
				String key = results[0].substring(1).trim();
				String toValue = results[1].replace(')', ' ').trim();
				Integer value = Integer.parseInt(toValue);
				
				// System.out.println(key + "," + value);				
				if (map.containsKey(key)) {
					map.put(key, map.get(key) + 1);
				}
				else {
					map.put(key, 1);
				}
			}
			br.close();
			
			Iterator it = map.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry pairs = (Map.Entry)it.next();
				// System.out.println("h: " + pairs.getKey() + ": " + pairs.getValue());
				bw.write("(" + pairs.getKey() + "," + pairs.getValue() + ")\n");
			}
			bw.flush();
			bw.close();
		} 
		
		catch (IOException e) {
			e.printStackTrace();
			System.out.println("File reading error.");
		}
		
		return;
	}
	
}
