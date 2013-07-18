package mp3;

public class PingAckMain {
	
	/**
	 * Class to test Ack and Ping functionality.
	 * 
	 * To use ack, use '1' as first argument, port number for the 2nd:
	 * 		java PingAckMain 1 <portnum>
	 * 
	 * To use ping, use '2' as first argument, port number for 2nd,
	 *  and target machine hostname (on which ack is running) as the 3rd:
	 *  	java PingAckMain 2 <portnum> <hostname>
	 */
	
	public static void main(String[] args) {
		
		for (int i = 0; i < args.length; i++)
			System.out.println("args[" + i + ": " + args[i]);
		
		if (args.length < 2) {
			System.out.println("Usage: PingAckMain 1 <port>");
			System.out.println("or");
			System.out.println("Usage: PingAckMain 2 <port> <hostname>");
		}
		
		if (args[0].equals("1")) {
			System.out.println("Starting ack...");
			new Ack(Integer.parseInt(args[1]));
			System.out.println("ack exit");
			return;
		}
		
		else if (args[0].equals("2")) {
			if (args.length != 3) {
				System.out.println("Provide a hostname argument");
				return;
			}
			
			System.out.println("Starting ping...");
			
//			Ping ping  = new Ping();
//			ping.Initialize(args[2], Integer.parseInt(args[1]));
//			ping.run();
			
			System.out.println("ping exit");
		}
		
		else {
			System.out.println("main initiation error");
			return;
		}
	}	
}
