package mp3;
 
/**
 * 
 */
public class IntroducerNode extends ClientNode {

	/**
	 * A basic introducer
	 */
	public IntroducerNode() {
		super("localhost", 10000);
	}
	
	/**
	 * A basic introducer
	 */
	public IntroducerNode(String hostName, int port) {
		super(hostName, port);
	}
	
	/**
	 * An introducer which crashes after lifespan
	 */
	public IntroducerNode(String hostName, int port,int lifespan, boolean crash,int lossrate) {
		super(hostName, port,lifespan, crash,hostName,port,lossrate);
	}
	
	/**
	 * Class entry point
	 */
	public static void main(String[] args){
		
		if (args.length < 2){
			System.out.println("Usage: IntroducerNode hostname port [lifespan] [crash] [loss_rate]");
			System.exit(0);
		}
			
		String hostName = args[0];
		int port = Integer.parseInt(args[1]);
		int lifespan = -1;	
		boolean crash = false;
		int lossrate = 0;
		 
		if(args.length > 2){
			lifespan = Integer.parseInt(args[2]);
		}
		
		if (args.length > 3){
			crash =  Integer.parseInt(args[3]) == 1;
		}
		
		if (args.length > 4){
			lossrate =  Integer.parseInt(args[4]);
		}
		
		//an automatic group join is initiated here.
		new IntroducerNode(hostName,port,lifespan,crash,lossrate); 
	}
}
