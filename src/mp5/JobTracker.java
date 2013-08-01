package mp5;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.regex.Pattern;
import mp3.Helper;
import mp3.NodeInfo;
import mp4.FileUtils;
import mp4.SdfsMessageHandler;
import mp4.SdfsNode;

public class JobTracker {

    private final SdfsNode node;
    private final HashMap<String,String> taskToStatusMap;
    private HashMap<String,TaskHearbeat> taskToHeartbeatMap;
    public volatile boolean completed;
    public volatile boolean jobRunning;
    private HashSet<String> mapleTaskNodes;
    private HashSet<String> juiceTaskNodes;
    public volatile boolean jobFailed;
    private int numberOfTasks = 0;
    private volatile int numberOfJuiceSlots = 1;
    private int maxSlots ;
    public volatile int totalNeededMapleTasks;
    public volatile int totalNeededJuiceTasks;
    public int remainingTasks;

    /**
     * @param node
     */
    public JobTracker(SdfsNode node){
        this.node = node;
        taskToStatusMap = new HashMap<String,String>();
    }

    /**
     * @param exe
     * @param prefix
     * @param sdfsFiles
     */
    public void startMapleJob(String exe,String prefix,ArrayList<String> sdfsFiles){
        jobRunning = true;
        completed = false;
        jobFailed = false;
        mapleTaskNodes = new HashSet<String>();
        taskToHeartbeatMap = new HashMap<String,TaskHearbeat>();
        Random rd = new Random();

        int taskId = 1;

        ///// count number of needed maple tasks
        for(String file: sdfsFiles){

            System.out.println(file);

            if (!node.fileMetadata.keySet().contains(file)){
                System.out.println(String.format("%s not found in SDFS system",file));
                continue;
            }

            HashMap<Integer,Set<String>> blockLocations = FileUtils.getSdfsFileBlockLocations(node,file);

            totalNeededMapleTasks += blockLocations.keySet().size();
        }
        
        remainingTasks = totalNeededMapleTasks;
        ///////////////////////////////////////////////

        //1- first locate node which have the data
        for(String file: sdfsFiles){

            System.out.println(file);

            if (!node.fileMetadata.keySet().contains(file)){
                System.out.println(String.format("%s not found in SDFS system",file));
                continue;
            }

            HashMap<Integer,Set<String>> blockLocations = FileUtils.getSdfsFileBlockLocations(node,file);

            //for each block of a file, run the map task
            for(int blockId:blockLocations.keySet()){

                ArrayList<String> locations = new ArrayList<String>(blockLocations.get(blockId));
                String candidateNodeId = locations.get(rd.nextInt(locations.size()));

                sendProgramToNode(candidateNodeId,exe,mapleTaskNodes);

                String sdfsFileName = String.format("%s.part%d",file, blockId);

                String message = String.format("%s:%s:%s:%s:%s", SdfsMessageHandler.MAPLE_PREFIX,taskId, exe,prefix,sdfsFileName);

                assignTaskToNode(candidateNodeId,message,SdfsMessageHandler.MAPLE,taskId);

                taskId+=1;

                numberOfTasks+=1;
            }
        }
    }

    /**
     * @param exe
     * @param numberOfJuices
     * @param prefix
     * @param destinationSdfs
     * 
     * Limiting the juice operation to throttle the amount of concurent sub juice tasks started at once
     */
    public void startJuiceJob(String exe,int numberOfJuices, String prefix, String destinationSdfs){
        jobRunning = true;
        completed = false;
        jobFailed = false;
        juiceTaskNodes = new HashSet<String>();
        taskToHeartbeatMap = new HashMap<String,TaskHearbeat>();
        maxSlots = numberOfJuices;
        numberOfJuiceSlots = numberOfJuices;

        int taskId = 0;

        Random rd = new Random();

        //HashMap<String,String> fileKeysToJuiceNodeMap  = new HashMap<String,String>();

        //identify nodes which hold files with a particular prefix have a given prefix
        //and do not currently run a maple or juice task
        //each file should be attributed to a single reduce node, although multiple keys can all be
        //assign to the same node

        synchronized(node.fileMetadata){

            ArrayList<String> targetSdfsFiles = new ArrayList<String>();

            for(String sdfsFile:node.fileMetadata.keySet()){
                if (sdfsFile.startsWith(prefix)){
                    targetSdfsFiles.add(sdfsFile);
                }
            }

            Pattern p = Pattern.compile("_task(\\d+)");

            HashSet<String> mapKeys = new HashSet<String>();

            //Sort keys in order to process and output results in order
            Collections.sort(targetSdfsFiles);

            //compute the number of needed juice tasks
            for(String sdfsFile:targetSdfsFiles){
            	
            	String keyString = sdfsFile.replaceAll(p.pattern(), "");
            	
            	  if (mapKeys.contains(keyString))
                      continue;
            	  
            	  mapKeys.add(keyString);
            	  
            	totalNeededJuiceTasks+= 1;
            }
             
            remainingTasks = totalNeededJuiceTasks;

            //SDFS map files follow the template prefix_key_taskId
            //we need to remove the taskId part from the name in order to allow
            //juice nodes to aggregate prefix_key file blocks together.
            for(String sdfsFile:targetSdfsFiles){

                while (numberOfJuiceSlots == 0){
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }

                String keyString = sdfsFile.replaceAll(p.pattern(), "");

//                if (mapKeys.contains(keyString))
//                    continue;

//                mapKeys.add(keyString);

                HashMap<Integer,Set<String>> blockLocations = FileUtils.getSdfsFileBlockLocations(node,sdfsFile);

                //get the locations of the first block
                ArrayList<String> locations = new ArrayList<String>(blockLocations.get(0));

                //get a random candidate which will fetch all blocks of the current targetfile
                String candidateNodeId = locations.get(rd.nextInt(locations.size()));

                sendProgramToNode(candidateNodeId,exe,juiceTaskNodes);

                //send juice message to candidate node in order to fetch blocks and juice them
                String message = String.format("%s:%s:%s:%s:%s", SdfsMessageHandler.JUICE_PREFIX,
                        taskId, exe, keyString, destinationSdfs);

                assignTaskToNode(candidateNodeId,message,SdfsMessageHandler.JUICE,taskId);

                taskId+=1;

                numberOfTasks+=1;

                numberOfJuiceSlots -=1;
            }
        }
    }

    /**
     * @param candidateNodeId
     * @param exe
     * @param taskNodes
     */
    private void sendProgramToNode(String candidateNodeId, String exe, HashSet<String> taskNodes){

        NodeInfo currentNodeInfo = Helper.extractNodeInfoFromId(node.getNodeId());

        synchronized(taskNodes){
            if (!taskNodes.contains(candidateNodeId)){
                //send program to node
                NodeInfo candidateNodeInfo = Helper.extractNodeInfoFromId(candidateNodeId);
                String configPath = FileUtils.getConfigStorageFolder(currentNodeInfo);
                String programPath = String.format("%s/%s", configPath,exe);

                File prog = new File(programPath);
                if (prog.exists()){
                    FileUtils.uploadProgram(prog,
                            candidateNodeInfo.getHostname(),
                            candidateNodeInfo.getPort());

                    taskNodes.add(candidateNodeId);
                }else{
                    node.log(String.format("Unable to find the program %s in the master node",exe));
                    System.out.println(String.format("File not found in master node: %s",exe));
                }
            }
        }
    }


    /**
     * @param nodeId
     * @param status
     */
    public void notifyTaskProgress(String nodeId,int taskId,String taskType, String status) {

        boolean allCompleted = false;

        synchronized (taskToStatusMap) {

            String taskName = String.format("%s_%d",taskType, taskId);

            taskToStatusMap.put(taskName, status);

            if(taskType.equals(SdfsMessageHandler.JUICE)){
                //synchronized (numberOfJuiceSlots){
                if(status.equals(SdfsMessageHandler.TASK_REPORT_COMPLETED_PREFIX)) {

                    numberOfJuiceSlots+=1;
                    numberOfJuiceSlots = Math.min(maxSlots, numberOfJuiceSlots);

                    totalNeededJuiceTasks-=1;
                    remainingTasks = totalNeededJuiceTasks;

                    if (totalNeededJuiceTasks == 0)
                        allCompleted = true;
                }
                // }
            }
            else{
                if(status.equals(SdfsMessageHandler.TASK_REPORT_COMPLETED_PREFIX)) {
                    totalNeededMapleTasks-=1;
                    remainingTasks = totalNeededMapleTasks;
                    if (totalNeededMapleTasks == 0)
                        allCompleted = true;
                }
            }
        }

        if (allCompleted){
            completed = true;
            jobRunning = false;
        }
    }

    /**
     * @param nodeId
     * @param message
     */
    public void assignTaskToNode(String nodeId,String message,String taskType,int taskId){
        NodeInfo nodeInfo = Helper.extractNodeInfoFromId(nodeId);
        Helper.sendUnicastMessage(node.getSocket(), message, nodeInfo.getHostname(), nodeInfo.getPort());

        //add task to heartbeat monitor list.
        String taskName = String.format("%s_%d",taskType,taskId);

        synchronized(taskToHeartbeatMap){
            if(!taskToHeartbeatMap.containsKey(taskName)){
                taskToHeartbeatMap.put(taskName, new TaskHearbeat(taskId, taskType, this));
            }
        }
    }

    public void notifyFailedTask(int taskId, String taskType) {
        //restart task
        synchronized(taskToHeartbeatMap){
            String taskName = String.format("%s_%d",taskType,taskId);

            if(taskToHeartbeatMap.containsKey(taskName)){
                taskToHeartbeatMap.remove(taskName);
            }
        }

        //restart the failed task
    }

    public static void main(String[] args){

    }
}
