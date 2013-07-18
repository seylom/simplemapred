package mp2;

/*
 * A wrapper object for machine information
 */
public class MachineInfo {

	public final String hostName;
	public int port;
	public String logName;
	
	/*
	 * Initializes new instances of MachineInfo class
	 */
	public MachineInfo(String theHost,int thePort,String theLogName) {
		port = thePort;
		hostName = theHost;
		logName = theLogName;
	}
}