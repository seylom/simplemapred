package mp5;

public class TaskHearbeat implements Runnable {
	
	private String taskType;
	private int taskId;
	private String taskName;
	
	public volatile boolean stop;
	public volatile boolean heartbeatReceived = true;
	private int HEARTBEAT_INTERVAL = 4000;
	private JobTracker jobTracker;
	
	public TaskHearbeat(int taskId, String taskType,JobTracker jobTracker){
		this.taskId = taskId;
		this.taskType = taskType;
		this.jobTracker = jobTracker;
		this.taskName = String.format("%s_%d", taskType,taskId);
		
		Thread t = new Thread(this);
		t.start();
	}
	
	public String getTaskName(){
		return taskName;
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		
		while(!stop && !Thread.interrupted()){
			
			if (heartbeatReceived){
				heartbeatReceived = false;
				
				try {
					Thread.sleep(HEARTBEAT_INTERVAL);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}				
			}else{
				stop = true;
				
				jobTracker.notifyFailedTask(taskId,taskType);
			}
		}
	}
}
