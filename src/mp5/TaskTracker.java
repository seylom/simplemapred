package mp5;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;

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
				.getMapleJuiceStoragePath(nodeInfo);
		
		File mjFolderFile = new File(mjStorageFolder);
		if (!mjFolderFile.exists());
			mjFolderFile.mkdirs();

		String sdfsFolder = FileUtils.getStoragePath(nodeInfo);
		final String targetFileName = String.format("%s/%s", sdfsFolder,
				filename);

		String programFolder = FileUtils.getConfigStorageFolder(nodeInfo);
		final String exePath = String.format("%s/%s", programFolder, exe);

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
							for (int k = 0; k < files.length; k++) {
								String sdfsname = files[k].getName();						
								FileUtils.sendFile(files[k], nodeInfo, sdfsname, 1, 0);
							}
						}
					}

					completed = true;

				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}).start();
	}

	/**
	 * @param exe
	 * @param sourceFile
	 * @param destinationFile
	 */
	public void startJuiceJob(final String exe, final String sourceFile,
			final String destinationFile) {

		taskType = SdfsMessageHandler.JUICE;

		NodeInfo nodeInfo = Helper.extractNodeInfoFromId(node.getNodeId());
		final String mjStorageFolder = FileUtils
				.getMapleJuiceStoragePath(nodeInfo);

		File mjFolderFile = new File(mjStorageFolder);
		if (!mjFolderFile.exists());
			mjFolderFile.mkdirs();

			
		String sdfsFolder = FileUtils.getStoragePath(nodeInfo);
			
		final String sourcePath = String
				.format("%s/%s", sdfsFolder, sourceFile);
		final String destinationPath = String.format("%s/%s", sdfsFolder,
				destinationFile);

		String programFolder = FileUtils.getConfigStorageFolder(nodeInfo);
		final String exePath = String.format("%s/%s", programFolder, exe);

		(new Thread() {
			public void run() {
				ProcessBuilder pb = new ProcessBuilder("java", "-jar", exePath,
						sourcePath, destinationPath);
				pb.directory(new File(mjStorageFolder));
				pb.redirectOutput(ProcessBuilder.Redirect.INHERIT);

				try {

					launchTaskHeartbeat();
					Process pc = pb.start();
					pc.waitFor();

					completed = true;

				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
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
						e.printStackTrace();
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
