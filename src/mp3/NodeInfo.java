package mp3;
/*
 * 
 */
public class NodeInfo {

	private int port;
	private String hostname;
	private String timestamp;
	 
	/*
	 * 
	 */
	public NodeInfo(String hostname,int port, String timestamp){
		this.hostname = hostname;
		this.port  = port;
		this.timestamp = timestamp;
	}
	
	/*
	 * 
	 */
	public int getPort(){
		return this.port;
	}
	
	/*
	 * 
	 */
	public String getHostname(){
		return this.hostname;
	}
	
	/*
	 * 
	 */
	public String getTimestamp(){
		return this.timestamp;
	}
}
