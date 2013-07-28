package mp5;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringTokenizer;

public class WordCountMaple {

	String prefix;
	String defaultFileName;
	
	public WordCountMaple(String filename,String prefix){
		
		this.defaultFileName = filename;
		this.prefix = prefix;
		
		File file = new File(filename);
		
		if (!file.exists()){
			System.out.println( String.format("The file [%s] does not exist, please check your filename",filename));
			return;
		}
	}
	
	/**
	 * @param file
	 */
	public void doMaple(){		
		
		File file = new File(defaultFileName);
		
		try {

			HashMap<String,ArrayList<String>> keyValuesMap = new HashMap<String,ArrayList<String>>();
			
			BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
			String line = "";
					 
			while((line = reader.readLine())!=null){
				
				System.out.println("Tokenizing file...");
				
				StringTokenizer tokenizer = new StringTokenizer(line, " \t\n\r\f,.:;?![]'");
				
				while(tokenizer.hasMoreTokens()){
					
					String key = tokenizer.nextToken();
					
					String value = String.format("(%s, 1)",key);
					
					if (keyValuesMap.containsKey(key)){
						keyValuesMap.get(key).add(value);
					}else{
						ArrayList<String> values = new ArrayList<String>();
						values.add(value);
						
						keyValuesMap.put(key, values);
					}
				}
			}
			
			for(String key:keyValuesMap.keySet()){
				
				String keyFileName = String.format("%s_%s",prefix, key);
				
				String fullPath = String.format("%s", keyFileName);
				
				StringBuilder builder = new StringBuilder();
				
				for(String value:keyValuesMap.get(key)){
					builder.append(value + "\n");
				}
				
				FileWriter writer = new FileWriter(fullPath);
				BufferedWriter bw = new BufferedWriter(writer);
				bw.write(builder.toString());
				bw.close();		
			}
			
			reader.close();

		}catch (IOException e) {
			System.out.println(String.format("Unable to tokenize file %s",file));
			e.printStackTrace();
		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args){
		if (args.length < 2){
			System.out.println("Usage: java WordCountMaple sdfsfilename prefix");
		}
		
		WordCountMaple wcm = new WordCountMaple(args[0],args[1]);
		wcm.doMaple();
	}
}
