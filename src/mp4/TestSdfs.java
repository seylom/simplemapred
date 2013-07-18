package mp4;

public class TestSdfs {
	/**
	 * @throws InterruptedException
	 */
	public void single_node_master_introducer() throws InterruptedException{
		Thread introducerThread = new Thread(){
			public void run(){
				new SdfsNode("localhost", 10000,"localhost", 10000,true);
			}
		};
		
		introducerThread.start();
		
		Thread.sleep(1000);

		for(int i = 0; i< 3; i++){
			final int idx = i;
			(new Thread(){
				public void run(){
					new SdfsNode("localhost", 7770 + idx);				 
			 }
			}).start();
			
			Thread.sleep(100);
		}
	}
	
	public void single_node_master_introducer_with_crash_handling() throws InterruptedException{
		Thread introducerThread = new Thread(){
			public void run(){
				new SdfsNode("localhost", 10000,"localhost", 10000,true);
			}
		};
		
		introducerThread.start();
		
		Thread.sleep(1000);

		for(int i = 0; i< 3; i++){
			final int idx = i;
			(new Thread(){
				public void run(){
					new SdfsNode("localhost", 7770 + idx);				 
			 }
			}).start();
			
			Thread.sleep(100);
		}
		
		//creates data nodes which crashes after 4 seconds
		for(int i = 0; i< 2; i++){
			final int idx = i;
			(new Thread(){
				public void run(){
					new SdfsNode("localhost", 8880 + idx,false, 4000,true);				 
			 }
			}).start();
		} 
	}
	
	
	public void start_nodes_and_transfer_a_file() throws InterruptedException{
		Thread introducerThread = new Thread(){
			public void run(){
				new SdfsNode("localhost", 10000,"localhost", 10000,true);
			}
		};
		
		introducerThread.start();
		
		Thread.sleep(1000);

		for(int i = 0; i< 1; i++){
			final int idx = i;
			(new Thread(){
				public void run(){
					new SdfsNode("localhost", 7770 + idx);				 
			 }
			}).start();
			
			Thread.sleep(100);
		}
		
		Thread.sleep(3000);
		
		//initiate a transfer
		SdfsClient client = new SdfsClient("localhost", 10000);
		client.put("/tmp/ayivigu2_kjustic3/log_10", "/temp/log1");
		
		Thread.sleep(5000);
		
		client.get("/temp/log1");
	}
	
	public void start_6_nodes_and_transfer_a_file_then_delete() throws InterruptedException{
		Thread introducerThread = new Thread(){
			public void run(){
				new SdfsNode("localhost", 10000,"localhost", 10000,true);
			}
		};
		
		introducerThread.start();
		
		Thread.sleep(1000);

		for(int i = 0; i< 5; i++){
			final int idx = i;
			(new Thread(){
				public void run(){
					new SdfsNode("localhost", 7770 + idx);				 
			 }
			}).start();
			
			Thread.sleep(100);
		}
		
		Thread.sleep(3000);
		
		//initiate a transfer
		SdfsClient client = new SdfsClient("localhost", 10000);
		client.put("/tmp/ayivigu2_kjustic3/log_10", "/temp/log1");
		
		//Thread.sleep(4000);
		
		//client.delete("/temp/log1");
	}
	
	public void start_20_nodes_and_transfer_a_file_then_delete() throws InterruptedException{
		Thread introducerThread = new Thread(){
			public void run(){
				new SdfsNode("localhost", 10000,"localhost", 10000,true);
			}
		};
		
		introducerThread.start();
		
		Thread.sleep(1000);

		for(int i = 0; i< 19; i++){
			final int idx = i;
			(new Thread(){
				public void run(){
					new SdfsNode("localhost", 7770 + idx);				 
			 }
			}).start();
			
			Thread.sleep(100);
		}
		
		Thread.sleep(3000);
		
		//initiate a transfer
		SdfsClient client = new SdfsClient("localhost", 10000);
		client.put("/tmp/ayivigu2_kjustic3/log_100", "/temp/log1");
		
		Thread.sleep(4000);
		
		client.delete("/temp/log1");
	}
	
	public void start_7_nodes_and_transfer_a_file_then_delete() throws InterruptedException{
		Thread introducerThread = new Thread(){
			public void run(){
				new SdfsNode("localhost", 10000,"localhost", 10000,true);
			}
		};
		
		introducerThread.start();
		
		Thread.sleep(1000);

		for(int i = 0; i< 4; i++){
			final int idx = i;
			(new Thread(){
				public void run(){
					new SdfsNode("localhost", 7770 + idx);				 
			 }
			}).start();
			
			Thread.sleep(100);
		}

		Thread.sleep(1000);
		
		(new Thread(){
			public void run(){
				new SdfsNode("localhost",8000 ,"localhost", 10000,false,10000,true);				 
		 }
		}).start();
		
		//initiate a transfer
		SdfsClient client = new SdfsClient("localhost", 10000);
		client.put("/tmp/ayivigu2_kjustic3/log_10", "/temp/log1");
		
		//Thread.sleep(5000);
		//client.delete("/temp/log1");
	}
	
	/**
	 * entry point for the class
	 */
	public static void main(String[] args) throws InterruptedException{
		TestSdfs test = new TestSdfs();
		
		test.start_7_nodes_and_transfer_a_file_then_delete();
	}
}
