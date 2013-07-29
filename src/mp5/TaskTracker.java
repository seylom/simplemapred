package mp5;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream; 
import java.io.SequenceInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Random;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import mp3.Helper;
import mp3.NodeInfo; 
import mp4.FileUtils;
import mp4.SdfsMessageHandler;
import mp4.SdfsNode;

public class TaskTracker {

	private SdfsNode node;
	private int taskId;
	private String taskType;
	private volatile boolean completed;
	private volatile boolean failed;

	public static int REPORT_INTERVAL = 3000;

	public TaskTracker(SdfsNode node, int taskId) {
		this.node = node;
		this.taskId = taskId;
	}

	/**
	 * @param exe
	 * @param prefix
	 * @param filename
	 */
	public void startMapleJob(final String exe, final String prefix,
			final String filename) {

		taskType = SdfsMessageHandler.MAPLE;

		final NodeInfo nodeInfo = Helper.extractNodeInfoFromId(node.getNodeId());
		final String mjStorageFolder = FileUtils
				.getMapleJuiceStoragePath(nodeInfo) + "_task" + taskId;
		
		File mjFolderFile = new File(mjStorageFolder);
		if (!mjFolderFile.exists());
			mjFolderFile.mkdirs();

		String sdfsFolder = FileUtils.getStoragePath(nodeInfo);
		final String targetFileName = String.format("%s/%s", sdfsFolder,
				filename);

		String programFolder = FileUtils.getConfigStorageFolder(nodeInfo);
		final String exePath = String.format("%s/%s", programFolder, exe);
		
		//String fullPrefix = prefix + "_task" +  taskId;

		(new Thread() {
			public void run() {
				ProcessBuilder pb = new ProcessBuilder("java", "-jar", exePath,
						targetFileName, prefix);
				pb.directory(new File(mjStorageFolder));
				pb.redirectOutput(ProcessBuilder.Redirect.INHERIT);

				try {

					launchTaskHeartbeat();
					Process pc = pb.start();
					pc.waitFor();

					// save results to HDFS - ?
					File directory = new File(mjStorageFolder);
					if (directory.exists()) {
						File[] files = directory.listFiles(new FileFilter() {
							@Override
							public boolean accept(File pathname) {
								return pathname.getName().startsWith(prefix);
							}
						});

						if (files.length > 0) {
							
							Pattern p = Pattern.compile(".part(\\d+)$");
							Matcher matcher = p.matcher(filename);
							
							int index = 0;
							
							if (matcher.find()){
								String idx = matcher.group(1);
								
								index = Integer.parseInt(idx);
							}
								
							for (int k = 0; k < files.length; k++) {
								String name = files[k].getName();	
								String sdfsname = name + "_task" + taskId;
								
								FileUtils.sendFile(files[k], nodeInfo, sdfsname, index, 0);
							}	
						}
						
						//cleanup after maple task
//						if (mjFolderFile.exists());
//							FileUtils.deleteDirectory(mjFolderFile);
					}

					completed = true;

				} catch (IOException e) {
					node.log("An error occur during the maple task execution");
					failed = true;
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					//e.printStackTrace();
				}
			}
		}).start();
	}

	/**
	 * @param exe
	 * @param sourceFile
	 * @param destinationFile
	 * @throws IOException 
	 */
	public void startJuiceJob(final String exe, final String sourceFile,
			final String destinationFile) throws IOException {

		Random rd = new Random();
		taskType = SdfsMessageHandler.JUICE;

		NodeInfo nodeInfo = Helper.extractNodeInfoFromId(node.getNodeId());
		final String mjStorageFolder = FileUtils
				.getMapleJuiceStoragePath(nodeInfo);
		
		final String sdfsStorageFolder = FileUtils
				.getStoragePath(nodeInfo);

		File mjFolderFile = new File(mjStorageFolder);
		if (!mjFolderFile.exists());
			mjFolderFile.mkdirs();
			
		////////////////////////////////////////////////////////////////////////
		//Retrieve file names of map results with the same key
		ArrayList<String> matchingFiles = new ArrayList<String>();
		
		synchronized (node.fileMetadata) {
			for(String mapFile: node.fileMetadata.keySet()){
				
				//System.out.println(mapFile);
				
				if (mapFile.startsWith(sourceFile + "_")){
					matchingFiles.add(mapFile);
				}
			}
		}
		
		////////////////////////////////////////////////////////////////////////
		//request map blocks we don't have 

		ArrayList<InputStream> streams =  new ArrayList<InputStream>();
		
		for(String mapFile:matchingFiles){
			
			HashMap<Integer,Set<String>> blocksLocationMap =  FileUtils.getSdfsFileBlockLocations(node, mapFile);
			
			ArrayList<Integer> blockIndexForFile = new ArrayList<Integer>();
			
			for(int blockIndex:blocksLocationMap.keySet()){
				
				blockIndexForFile.add(blockIndex);
				
				Set<String> locations = blocksLocationMap.get(blockIndex);
				
				if (locations.contains(node.getNodeId()))
					continue;
				
				ArrayList<String> locationCandidates = new ArrayList<String>(locations);
				
				String sourceId = locationCandidates.get(rd.nextInt(locations.size()));
				
				NodeInfo sourceInfo = Helper.extractNodeInfoFromId(sourceId);

				String downloadFileName  = String.format("%s.part%d", mapFile,blockIndex);
					
				ByteArrayOutputStream baos = FileUtils.downloadStream(downloadFileName, sourceInfo);
				
				String downloadPath = String.format("%s/%s", sdfsStorageFolder,downloadFileName);
				
				FileOutputStream fos = new FileOutputStream(new File(downloadPath));  
				fos.write(baos.toByteArray()); 
				fos.flush(); 
				fos.close();
				
				//add block to SDFS metadata
				node.addBlockInfo(node.getNodeId(), mapFile, blockIndex, 0);
				
				//node.notifyChunckReception(mapFile, blockIndex, 0);
			}
			
			//Save block files to local directory
			for(int blockIndex:blockIndexForFile){
				String partName = String.format("%s.part%d",mapFile,blockIndex);
				
				String blockPath = sdfsStorageFolder;
				
				if (!blockPath.equals("")){
					blockPath =  String.format("%s/%s", blockPath,partName);
				}else{
					blockPath = partName;
				} 
				
				File blockFile = new File(blockPath);
				
				if (!blockFile.exists()){
					node.log(String.format("The block file %s was not found on local storage",blockPath));
					continue;
				}
							
				try {
					streams.add(new FileInputStream(blockFile));
				} catch (FileNotFoundException e) {
					node.log(String.format("The file %s was not found in local storage", blockPath));
				}
			}
		}	
		
		SequenceInputStream seqStream = new SequenceInputStream(Collections.enumeration(streams));
		
		final String finalSourcePath = String.format("%s/%s.tmp",mjStorageFolder,sourceFile);
		FileOutputStream outputStream = new FileOutputStream(finalSourcePath);
		
		byte[] buffer = new byte[1024];
		int len;
		while ((len = seqStream.read(buffer)) != -1) {
		    outputStream.write(buffer, 0, len);
		}
		
		outputStream.close();
		seqStream.close();
		
		String sdfsFolder = FileUtils.getStoragePath(nodeInfo);

		final String destinationPath = String.format("%s/%s.part%d", sdfsFolder,
				destinationFile,taskId);

		String programFolder = FileUtils.getConfigStorageFolder(nodeInfo);
		final String exePath = String.format("%s/%s", programFolder, exe);

		
		/////////////////////////////////////////////////////////////////////////
		// EXECUTE the actual Juice task
		(new Thread() {
			public void run() {
				ProcessBuilder pb = new ProcessBuilder("java", "-jar", exePath,
						finalSourcePath, destinationPath);
				pb.directory(new File(mjStorageFolder));
				pb.redirectOutput(ProcessBuilder.Redirect.INHERIT);

				try {

					launchTaskHeartbeat();
					Process pc = pb.start();
					pc.waitFor();
					
					node.addBlockInfo(node.getNodeId(), destinationFile, taskId, 0); 
					node.notifyChunckReception(destinationFile, taskId, 0);

					completed = true;

				} catch (IOException e) {
					node.log("An error occur during the juice task execution");
					failed = true;
				} catch (InterruptedException e) { 
					//e.printStackTrace();
				}
			}
		}).start();
	}

	/**
	 * 
	 */
	private void launchTaskHeartbeat() {
		(new Thread() {
			@Override
			public void run() {
				while (!completed && !failed) {

					node.reportTaskProgress(taskId, taskType,
							SdfsMessageHandler.TASK_REPORT_BUSY_PREFIX);

					try {
						Thread.sleep(REPORT_INTERVAL);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						// e.printStackTrace();
					}
				}

				if (completed) {
					node.reportTaskProgress(taskId, taskType,
							SdfsMessageHandler.TASK_REPORT_COMPLETED_PREFIX);
				} else if (failed) {
					node.reportTaskProgress(taskId, taskType,
							SdfsMessageHandler.TASK_REPORT_FAILED_PREFIX);
				}
			}
		}).start();
	}

	public static void main(String[] args) {

	}
}
