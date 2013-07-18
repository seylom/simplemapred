package mp3;
/**
 * 
 * A simple class to locally test the group membership
 */
public class TestGroupMembership {
	
	
	/**
	 * @throws InterruptedException
	 */
	public void normal_execution() throws InterruptedException{
		Thread introducerThread = new Thread(){
			public void run(){
				new IntroducerNode(); //assumes localhost:10000
			}
		};
		
		introducerThread.start();
		
		Thread.sleep(1000);

		for(int i = 0; i< 2; i++){
			final int idx = i;
			(new Thread(){
				public void run(){
					new ClientNode("localhost", 7770 + idx,0,false,"localhost",10000,0);				 
			 }
			}).start();
			
			Thread.sleep(1000);
		} 
	}
	
	/**
	 * @throws InterruptedException
	 */
	public void voluntary_leave_and_crash_should_be_supported() throws InterruptedException{
		Thread introducerThread = new Thread(){
			public void run(){
				new IntroducerNode(); //assumes localhost:10000
			}
		};
		
		introducerThread.start();
		
		Thread.sleep(1000);

		for(int i = 0; i< 3; i++){
			final int idx = i;
			(new Thread(){
				public void run(){
					new ClientNode("localhost", 7770 + idx,0,false,"localhost",10000,1);				 
			 }
			}).start();
			
			Thread.sleep(1000);
		} 
		
		//creates a node which joins and voluntarily leaves after 10 seconds
		(new Thread(){
			public void run(){
				new ClientNode("localhost", 8888,10000,false);				 
		 }
		}).start();
		
		//creates a node which crashes after 4 seconds
		(new Thread(){
			public void run(){
				new ClientNode("localhost", 8889,4000,true);				 
		 }
		}).start();
	}
	
	/**
	 * @throws InterruptedException
	 */
	public void introducer_failure_8889_cannot_join() throws InterruptedException{
		Thread introducerThread = new Thread(){
			public void run(){
				new IntroducerNode("localhost", 10000, 4000,false,0);
			}
		};
		
		introducerThread.start();
		
		Thread.sleep(2000);

		for(int i = 0; i< 3; i++){
			final int idx = i;
			(new Thread(){
				public void run(){
					new ClientNode("localhost", 7770 + idx);				 
			 }
			}).start();
			
			Thread.sleep(100);
		} 
		
		//give time for the introducer to crash
		Thread.sleep(6000);
		
		//creates a node which crashes after 4 seconds
		(new Thread(){
			public void run(){
				new ClientNode("localhost", 8889);				 
		 }
		}).start();
	}
	
	/**
	 * @throws InterruptedException
	 */
	public void ping_ack_for_two_node_ring_should_cycle_properly() throws InterruptedException{
		Thread introducerThread = new Thread(){
			public void run(){
				new IntroducerNode("localhost",10000);
			}
		};
		
		introducerThread.start();
		
		Thread.sleep(1000);

		for(int i = 0; i< 1; i++){
			final int idx = i;
			(new Thread(){
				public void run(){
					new ClientNode("localhost", 10001 + idx);				 
			 }
			}).start();
			
			Thread.sleep(1000);
		} 
	}
	
	/**
	 * @throws InterruptedException
	 */
	public void consecutive_crashes_should_be_correclty_handled() throws InterruptedException{
		Thread introducerThread = new Thread(){
			public void run(){
				new IntroducerNode("localhost", 10000, 3000,false,0);
			}
		};
		
		introducerThread.start();
		
		Thread.sleep(1000);

		for(int i = 0; i< 3; i++){
			final int idx = i;
			(new Thread(){
				public void run(){
					new ClientNode("localhost", 7770 + idx);				 
			 }
			}).start();
			
			Thread.sleep(100);
		} 
		
		//creates nodes which crashes after 4 seconds
		for(int i = 0; i< 2; i++){
			final int idx = i;
			(new Thread(){
				public void run(){
					new ClientNode("localhost", 8880 + idx,4000,true);				 
			 }
			}).start();
		} 
	}
	
	/**
	 * @throws InterruptedException
	 */
	public void from_failure_to_membership_updates_all_around() throws InterruptedException{
		Thread introducerThread = new Thread(){
			public void run(){
				new IntroducerNode("localhost", 10000, 3000,false,0);
			}
		};
		
		introducerThread.start();
		
		Thread.sleep(1000);

		for(int i = 0; i< 3; i++){
			final int idx = i;
			(new Thread(){
				public void run(){
					new ClientNode("localhost", 7770 + idx);				 
			 }
			}).start();
			
			Thread.sleep(100);
		} 
		
		//creates a node which crashes after 4 seconds
		(new Thread(){
			public void run(){
				new ClientNode("localhost", 8880,4000,true);				 
		 }
		}).start();
	}
	
	/**
	 * entry point for the class
	 */
	public static void main(String[] args) throws InterruptedException{
		TestGroupMembership group = new TestGroupMembership();
		
		group.normal_execution();
	}
}
