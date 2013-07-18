package mp2;
/**
 * 
 */
public class LogInfo {
	
	public int rareCount;
	public int commonCount;
	public int mediumCount;
	
	public String rareWord;
	public String commonWord;
	public String mediumWord;
	
	public LogInfo( 
					String rareWord,
					String mediumWord,
					String commonWord,
					int rare,
					int medium,
					int common){
		
			rareCount =  rare;
			commonCount = common;
			mediumCount = medium;
			
			this.rareWord = rareWord;
			this.commonWord = commonWord;
			this.mediumWord = mediumWord;	
	}

}
