package org.apache.hama.bsp;

import java.io.*;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.myhama.util.TaskReportContainer;

public class BSPPeerForJob implements BSPPeerInterface {
	  
	public static final Log LOG = LogFactory.getLog(BSPPeerForJob.class);
	private int localTaskNumber;
	private Integer counter;
	private Map<TaskAttemptID, TaskStatus> runningTaskStatuses = 
		new LinkedHashMap<TaskAttemptID, TaskStatus>();

	public BSPPeerForJob(Configuration conf,BSPJobID jobId,BSPJob jobConf ) 
			throws IOException {
		this.localTaskNumber = 0;
		this.counter = 0;
	}
	  
	public boolean increaseSuperStep(BSPJobID jobId, TaskAttemptID taskId){
		try{
			runningTaskStatuses.get(taskId).incrementSuperstepCount();
			return true;
		}catch(Exception e){
			LOG.error("[indreaseSuperStep]", e);
			return false;
		}
	}
	 
	public void clear(BSPJobID jobId, TaskAttemptID taskId) {
		synchronized (this.counter) {
			this.counter++;
			if (this.counter == this.localTaskNumber) {
				deleteLocalFiles();
			}
		}
	}
	  
	public void deleteLocalFiles() {
		try {
			File rootDir = new File(new HamaConfiguration().get("bsp.local.dir"));
			File[] localDirs = rootDir.listFiles();
			for (int i = 0; i < localDirs.length; i++) {
				deleteLocalDir(localDirs[i]);
			}
		} catch (Exception e) {
			LOG.error("[deleteLocalFiles]", e);
		}
	}

	public void deleteLocalDir(File dir) {
		if (dir == null || !dir.exists() || !dir.isDirectory()) {
			return;
		}
		  
		for (File file : dir.listFiles()) {
			if (file.isFile()) {
				file.delete(); // delete the file
			} else if (file.isDirectory()) {
				deleteLocalDir(file); // recursive delete the subdir
			}
		}
		
		dir.delete();// delete the root dir
	}
	  
	@Override
	public long getProtocolVersion(String arg0, long arg1) throws IOException {
		return BSPPeerInterface.versionID;
	}
	  
	public void addRunningTaskStatuses(TaskAttemptID taskattemptid,TaskStatus taskstatus) {
		runningTaskStatuses.put(taskattemptid, taskstatus);
		localTaskNumber++;
	}

	public int getLocalTaskNumber(BSPJobID jobId) {
		return localTaskNumber;
	}
	  
	public long getSuperstepCount(BSPJobID jobId, TaskAttemptID taskId) {
		return runningTaskStatuses.get(taskId).getSuperStepCounter();
	}

	public void setJobConf(BSPJob jobConf) {
		
	}

	@Override
	public void close() throws IOException {
		clear(null, null);
	}

	@Override
	public void reportProgress(BSPJobID jobId, TaskAttemptID taskId,
			TaskReportContainer taskReportContainer) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void reportException(BSPJobID jobId, TaskAttemptID taskId,
			Exception e) {
	}
}