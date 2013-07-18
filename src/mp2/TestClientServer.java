package mp2;
/**
 * Test class for testing client server interaction.
 * This is a local test which runs on a single machine but simulates interaction with different
 * servers using a different port for each server instance.
 * Threads are used here to prevent blocking
 */
public class TestClientServer {
	
	/**
	 * 
	 */
	public static void main(String [] args){
		
		//Start three servers
		Thread serverThread1 = new Thread() {
            public void run() {
        		LogQuerierServer querier = new LogQuerierServer("localhost",10000,"machine.1.log");
        		querier.listen();
            }
            
        };
        
        Thread serverThread2 = new Thread() {
            public void run() {
        		LogQuerierServer querier = new LogQuerierServer("localhost",10001,"machine.2.log");
        		querier.listen();
            }
        };
       
        Thread serverThread3 = new Thread() {
            public void run() { 
        		LogQuerierServer querier = new LogQuerierServer("localhost",10002,"machine.3.log");
        		querier.listen();
            }
        };
        
        Thread clientThread = new Thread() {
            public void run() {
            	LogQuerierClient client = new LogQuerierClient(true);
            	client.processInput();
            }
        };

        serverThread1.start();
        serverThread2.start();
        serverThread3.start();

        //Give time to servers to initialize their states before launching the client.
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        clientThread.start() ;
	}
}
