/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hama.bsp;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.RPC;
import org.apache.hama.Constants;
import org.apache.hama.Constants.CommandType;
import org.apache.hama.Constants.FaultTolerancePolicy;
import org.apache.hama.bsp.BSPJobClient.RawSplit;
import org.apache.hama.Constants.TaskState;
import org.apache.hama.ipc.CommunicationServerProtocol;
import org.apache.hama.monitor.JobInformation;
import org.apache.hama.monitor.JobMonitor;
import org.apache.hama.monitor.TaskInformation;
import org.apache.hama.myhama.api.GraphRecord;
import org.apache.hama.myhama.comm.SuperStepCommand;
import org.apache.hama.myhama.comm.SuperStepReport;
import org.apache.hama.myhama.graph.CheckPointStatus;
import org.apache.hama.myhama.util.AggregatorOfKmeans;
import org.apache.hama.myhama.util.CenterSetOfKmeans;
import org.apache.hama.myhama.util.IntervalAdjuster;
import org.apache.hama.myhama.util.JobLog;
import org.apache.hama.Constants.CheckPoint.CheckPointType;
import org.apache.hama.Constants.SyncModel;


/**
 * JobInProgress maintains all the info for keeping a Job on the straight and
 * narrow. It keeps its JobProfile and its latest JobStatus, plus a set of
 * tables for doing bookkeeping of its Tasks.ss
 */
class JobInProgress {
	/**
	 * Used when the a kill is issued to a job which is initializing.
	 */
	static class KillInterruptedException extends InterruptedException {
		private static final long serialVersionUID = 1L;

		public KillInterruptedException(String msg) {
			super(msg);
		}
	}
	
	 public class SemiAsynTimer extends Thread {
	      public void run() {
	    	  LOG.info("SemiAsyn Model => the inverval of inserting a barrier is " 
	    			  + intervalOfSync + " milliseconds");
	           while (true) {
				   	if(afterComputeInterval) {
		        	  try {
				            Thread.sleep(intervalOfSync);
				            waitComputeInterval();
			        		
			              } catch (Exception e) {
			                  LOG.error("[SemiAsynTimer]", e);
			              }
		            }//if
					   
		            if (isSemiAsyn()) {
		            	 syncTime = System.currentTimeMillis();
		            	 for (CommunicationServerProtocol comm : comms.values()) {
		     				try {
		     					comm.initiateBarrier();
		     				} catch (Exception e) {
		     					LOG.error("[SemiAsynTimer->initiateBarrier]", e);
		     				}
		     			} //for
		            } //if
		            
	          } //while
	      } //run
	  } //SemiAsynTimer

	JobLog MyLOG;
	private static final Log LOG = LogFactory.getLog(JobInProgress.class);
	boolean tasksInited = false, jobInited = false;

	JobProfile profile;
	Path jobFile = null, localJobFile = null, localJarFile = null;
	LocalFileSystem localFs;
	
	private Path ckDir;
	private int ckInterval = Integer.MAX_VALUE;
	private int[] taskCheckPointNum;
	private String fcPolicy; //traditional checkpoint by default
	
	Configuration conf;
	BSPJob job;
	volatile JobStatus status;
	BSPJobID jobId;
	final BSPMaster master;
	TaskInProgress tips[] = new TaskInProgress[0];

	private int taskNum = 0, maxIteNum = 1, curIteNum = 0;
	private String priority = Constants.PRIORITY.NORMAL;//default
	
	private HashMap<Integer, CommunicationServerProtocol> comms = 
		new HashMap<Integer, CommunicationServerProtocol>();
	private AtomicInteger reportCounter;

	private JobInformation jobInfo;
	private JobMonitor jobMonitor;
	private String[] taskToWorkerName;

	/** how long of scheduling, loading graph, saving result */
	private double scheTaskTime, loadDataTime, saveDataTime;
	/** the time of submitting, scheduling and finishing time */
	private long submitTime, startTime, finishTime;
	private long startTimeIte = 0;

	private ConcurrentHashMap<TaskAttemptID, Float> Progress = 
		new ConcurrentHashMap<TaskAttemptID, Float>();
	
	private CenterSetOfKmeans csk;
	private AggregatorOfKmeans[] aggregators; //[NumOfCentroids]
	private AggregatorOfKmeans[][] aggregatorsOfTasks; 
	private int numOfCentroids, numOfDimensions;
	//[NumOfTasks][NumOfCentroids]
	private volatile Boolean semiFlag = false;
	private int syncModel = SyncModel.Concurrent;
	private int intervalOfSync = 100; //milliseconds
	private SemiAsynTimer semiTimer;
	//just for testing
	private long aggregatorTime = 0L;
	
	private IntervalAdjuster interAdj;
	private Long syncTime = 0L;
	
	private long[] syncTimeOfTasks; //first iteration
	
	private boolean afterComputeInterval = true;
	
	public JobInProgress(BSPJobID _jobId, Path _jobFile, BSPMaster _master,
			Configuration _conf) throws IOException {
		
		jobId = _jobId; master = _master; MyLOG = new JobLog(jobId);
		localFs = FileSystem.getLocal(_conf); jobFile = _jobFile;
		localJobFile = master.getLocalPath(BSPMaster.SUBDIR + "/" + jobId + ".xml");
		localJarFile = master.getLocalPath(BSPMaster.SUBDIR + "/" + jobId + ".jar");
		Path jobDir = master.getSystemDirectoryForJob(jobId);
		FileSystem fs = jobDir.getFileSystem(_conf);
		fs.copyToLocalFile(jobFile, localJobFile);
		job = new BSPJob(jobId, localJobFile.toString());
		conf = job.getConf();
		
		priority = job.getPriority();
		profile = new JobProfile(job.getUser(), jobId, 
				jobFile.toString(), job.getJobName());
		String jarFile = job.getJar();
		if (jarFile != null) {
			fs.copyToLocalFile(new Path(jarFile), localJarFile);
		}
		
		curIteNum = 0; 
		maxIteNum = job.getNumSuperStep(); 
		taskNum = job.getNumBspTask();
		jobMonitor = new JobMonitor(maxIteNum, taskNum);
		initialize();
		
		status = new JobStatus(jobId, profile.getUser(),
				new float[] { 0.0f, 0.0f }, new int[] {-1, -1}, 0, JobStatus.PREP);
		status.setTotalSuperStep(maxIteNum); status.setTaskNum(taskNum);
		status.setUsername(job.getUser());
		submitTime = System.currentTimeMillis();
		status.setSubmitTime(submitTime);
		
		this.reportCounter = new AtomicInteger(0);
		
		taskCheckPointNum = new int[taskNum];
		ckInterval = job.getCheckPointInterval();
		fcPolicy = job.getFaultTolerancePolicy();
		
		this.numOfCentroids = job.getNumOfCenters();
		this.numOfDimensions = job.getNumOfDimensions();
		this.csk = new CenterSetOfKmeans();
		this.aggregatorsOfTasks = 
			new AggregatorOfKmeans[taskNum][this.numOfCentroids];
		this.aggregators = new AggregatorOfKmeans[this.numOfCentroids];
		for (int i = 0; i < this.numOfCentroids; i++) {
			this.aggregators[i] = 
				new AggregatorOfKmeans(this.numOfDimensions, job.isGMM());
		}
		this.syncModel = job.getSyncModel();
		this.semiFlag = false;
		this.intervalOfSync = 100; //job.getBarrierInterval(); //millisecond
		this.semiTimer = new SemiAsynTimer();
		
		this.syncTimeOfTasks = new long[this.taskNum];
	}

	private void initialize() {
		jobInfo = new JobInformation(this.job, this.taskNum);
		taskToWorkerName = new String[this.taskNum];
	}

	public JobProfile getProfile() {
		return profile;
	}

	public JobStatus getStatus() {
		return status;
	}

	public long getSubmitTime() {
		return submitTime;
	}

	public long getStartTime() {
		return startTime;
	}

	/**
	 * This job begin to be scheduled.
	 * Record the startTime;
	 * Compute the waitScheTime; 
	 */
	public void setStartTime() {
		this.startTime = System.currentTimeMillis();
		this.status.setStartTime(this.startTime);
		this.scheTaskTime = this.startTime - this.submitTime;
	}

	public String getPriority() {
		return priority;
	}

	public int getNumBspTask() {
		return this.taskNum;
	}

	public TaskInProgress[] getTaskInProgress() {
		return tips;
	}

	public long getFinishTime() {
		return finishTime;
	}

	/**
	 * @return the number of desired tasks.
	 */
	public int desiredBSPTasks() {
		return this.taskNum;
	}

	/**
	 * @return The JobID of this JobInProgress.
	 */
	public BSPJobID getJobID() {
		return jobId;
	}

	public synchronized TaskInProgress findTaskInProgress(TaskID id) {
		if (areTasksInited()) {
			for (TaskInProgress tip : tips) {
				if (tip.getTaskId().equals(id)) {
					return tip;
				}
			}
		}
		return null;
	}

	public synchronized boolean areTasksInited() {
		return this.tasksInited;
	}

	public String toString() {
		return "jobName:" + profile.getJobName() + "\n" + "submit user:"
				+ profile.getUser() + "\n" + "JobId:" + jobId + "\n"
				+ "JobFile:" + jobFile + "\n";
	}
	
	public void updateWorker(int i, String _taskId, String _worker) {
		this.taskToWorkerName[i] = _taskId + "==" + _worker;
	}

	public synchronized void initTasks() throws IOException {
		if (tasksInited) {
			return;
		}
		//change in version=0.2.3 read the input split info from HDFS
		Path sysDir = new Path(this.master.getSystemDir());
		FileSystem fs = sysDir.getFileSystem(conf);
		DataInputStream splitFile = fs.open(new Path(conf.get("bsp.job.split.file")));
		RawSplit[] splits;
		try {
			splits = BSPJobClient.readSplitFile(splitFile);
		} finally {
			splitFile.close();
		}
		// adjust number of map tasks to actual number of splits
		this.tips = new TaskInProgress[this.taskNum];
		for (int i = 0; i < this.taskNum; i++) {
			if (i < splits.length) {
				tips[i] = new TaskInProgress(getJobID(), this.jobFile.toString(), 
						this.master, this.conf, this, i, splits[i]);
			} else {
				//change in version=0.2.6 create a disable split. this only happen in Hash.
				RawSplit split = new RawSplit();
				split.setClassName("no");
				split.setDataLength(0);
				split.setBytes("no".getBytes(), 0, 2);
				split.setLocations(new String[] { "no" });
				//this task will not load data from DFS
				tips[i] = new TaskInProgress(getJobID(), this.jobFile.toString(), 
						this.master, this.conf, this, i, split);
			}
		}

		this.status.setRunState(JobStatus.PREP);
		tasksInited = true;
	}

	public void completedJob() {
		this.finishTime = System.currentTimeMillis();
		this.status.setProgress(new float[] {1.0f, 1.0f}, new int[]{-1, -1});
		this.status.setSuperStepCounter(curIteNum);
		this.status.setRunState(JobStatus.SUCCEEDED);
		this.status.setFinishTime(this.finishTime);
		garbageCollect();
		MyLOG.info("Job successfully done.");
		MyLOG.close();
		LOG.info(jobId.toString() + " aggregating time is: " 
				+ this.aggregatorTime/1000.0 + " seconds.");
		LOG.info(jobId.toString() + " is done successfully.");
	}

	public synchronized void failedJob() {
		this.finishTime = System.currentTimeMillis();
		this.status.setProgress(new float[] {1.0f, 1.0f}, new int[]{-1, -1});
		this.status.setSuperStepCounter(curIteNum);
		this.status.setRunState(JobStatus.FAILED);
		this.status.setFinishTime(this.finishTime);
		garbageCollect();
		MyLOG.close();
		LOG.info(jobId.toString() + " is failed.");
	}

	public void killJob() {
		this.finishTime = System.currentTimeMillis();
		this.status.setProgress(new float[] {1.0f, 1.0f}, new int[]{-1, -1});
		this.status.setSuperStepCounter(curIteNum);
		this.status.setRunState(JobStatus.KILLED);
		this.status.setFinishTime(this.finishTime);
		garbageCollect();
		for (int i = 0; i < tips.length; i++) {
			tips[i].kill();
		}
		MyLOG.close();
		LOG.info(jobId.toString() + " is killed.");
	}

	/**
	 * The job is dead. We're now GC'ing it, getting rid of the job from all
	 * tables. Be sure to remove all of this job's tasks from the various tables.
	 */
	synchronized void garbageCollect() {
		try {
			
			if (this.syncModel == SyncModel.SemiAsyn) {
				this.semiTimer.stop();
			}
			// Definitely remove the local-disk copy of the job file
			if (localJobFile != null) {
				localFs.delete(localJobFile, true);
				localJobFile = null;
			}
			if (localJarFile != null) {
				localFs.delete(localJarFile, true);
				localJarFile = null;
			}
			// JobClient always creates a new directory with job files
			// so we remove that directory to cleanup
			FileSystem fs = FileSystem.get(conf);
			fs.delete(new Path(profile.getJobFile()).getParent(), true);

			if (fs.delete(ckDir, true)) {
				LOG.info(jobId.toString() + " cleans up checkpoint dir:" +
						ckDir.toString());
			} else {
				LOG.error("failed to operate checkpoint dir:" + ckDir.toString());
			}
			
			writeJobInformation();
		} catch (IOException e) {
			LOG.error("Error cleaning up " + profile.getJobID(), e);
		}
	}

	public void updateJobStatus() {
		float minPro = 1.0f, maxPro = 0.0f;
		int minTid = -1, maxTid = -1;
		if (Progress.size() != 0) {
			for (Entry<TaskAttemptID, Float> e: Progress.entrySet()) {
				float pro = e.getValue();
				if (minPro > pro) {
					minPro = pro;
					minTid = e.getKey().getIntegerId();
				}
				if (maxPro < pro) {
					maxPro = pro;
					maxTid = e.getKey().getIntegerId();
				}
			}
		} else {
			minPro = 0.0f;
			minTid = -1;
			maxTid = -1;
		}

		this.status.setProgress(new float[] {minPro, maxPro}, 
				new int[] {minTid, maxTid});
		this.status.setCurrentTime(System.currentTimeMillis());
	}

	public void updateTaskStatus(TaskAttemptID taskId, TaskStatus ts) {
		Progress.put(taskId, (float)ts.getProgress());
		this.status.updateTaskStatus(taskId, ts);
		if (ts.getRunState() == TaskState.FAILED && 
				this.status.getRunState() == JobStatus.RUNNING) {
			this.status.setRunState(JobStatus.FAILED);
		}
	}
	
	/** Just synchronize, do nothing. */
	public void sync(int parId) {
		//LOG.info("[sync] taskId=" + parId);
		int finished = this.reportCounter.incrementAndGet();
		if (finished == this.taskNum) {
			this.reportCounter.set(0);

			for (CommunicationServerProtocol comm : this.comms.values()) {
				try {
					comm.quitSync();
				} catch (Exception e) {
					LOG.error("[sync:quitSync]", e);
				}
			}
		}
	}
	
	/**
	 * Synchronize archiving operation used in checkpoint.
	 * @param parId
	 */
	public void syncArchiveData(int parId, int _arcNum) {
		//LOG.info("[sync] taskId=" + parId);
		this.taskCheckPointNum[parId] = _arcNum;
		int finished = this.reportCounter.incrementAndGet();
		if (finished == this.taskNum) {
			this.reportCounter.set(0);
			
			int jobArcNum = 0;
			for (int i = 0; i < this.taskNum; i++) {
				jobArcNum += this.taskCheckPointNum[i];
				this.taskCheckPointNum[i] = 0;
			}
			double arcTime = (System.currentTimeMillis()-this.startTimeIte) / 1000.0;
			CheckPointStatus cks = 
				new CheckPointStatus(CheckPointType.Archive, curIteNum, jobArcNum, arcTime);
			this.jobInfo.recordCheckPointInfo(cks);
			//LOG.info("checkpoint-archive: " + curIteNum + ", " + jobArcNum + ", " + arcTime);
			
			for (CommunicationServerProtocol comm : this.comms.values()) {
				try {
					comm.quitSync();
				} catch (Exception e) {
					LOG.error("[sync:quitSync]", e);
				}
			}
		}
	}
	
	/**
	 * Synchronize loading operation used in checkpoint.
	 * @param parId
	 */
	public void syncLoadData(int parId, int _arcNum) {
		//LOG.info("[sync] taskId=" + parId);
		this.taskCheckPointNum[parId] = _arcNum;
		int finished = this.reportCounter.incrementAndGet();
		if (finished == this.taskNum) {
			this.reportCounter.set(0);
			
			int jobLoadNum = 0;
			for (int i = 0; i < this.taskNum; i++) {
				jobLoadNum += this.taskCheckPointNum[i];
				this.taskCheckPointNum[i] = 0;
			}
			double loadTime = (System.currentTimeMillis()-this.startTimeIte) / 1000.0;
			CheckPointStatus cks = 
				new CheckPointStatus(CheckPointType.Load, curIteNum, jobLoadNum, loadTime);
			this.jobInfo.recordCheckPointInfo(cks);
			//LOG.info("checkpoint-load: " + curIteNum + ", " + jobLoadNum + ", " + loadTime);
			
			for (CommunicationServerProtocol comm : this.comms.values()) {
				try {
					comm.quitSync();
				} catch (Exception e) {
					LOG.error("[sync:quitSync]", e);
				}
			}
		}
	}
	
	/** Build route-table by loading the first record of each task */
	public void buildRouteTable(TaskInformation tif) {
		/*LOG.info("[ROUTETABLE] tid=" + s.getTaskId() + "\t verMinId=" + s.getVerMinId());*/
		this.jobInfo.buildInfo(tif.getTaskId(), tif);
		InetSocketAddress address = new InetSocketAddress(tif.getHostName(), tif.getPort());
		try {
			CommunicationServerProtocol comm = 
				(CommunicationServerProtocol) RPC.waitForProxy(
						CommunicationServerProtocol.class,
							CommunicationServerProtocol.versionID, address, conf);
			comms.put(tif.getTaskId(), comm);
		} catch (Exception e) {
			LOG.error("[buildRouteTable:save comm]", e);
		}
		
		int finished = this.reportCounter.incrementAndGet();
		if (finished == taskNum) {
			//for checkpoint
			try {
				ckDir = master.getCheckPointDirectoryForJob(jobId);
				FileSystem fs_ck = ckDir.getFileSystem(this.conf);
				if (fs_ck.mkdirs(ckDir)) {
					LOG.info(jobId.toString() + " creates checkpoint dir:" +
							ckDir.toString());
					LOG.info(jobId.toString() + " fault-tolerance policy:" +
							this.fcPolicy + ", ckInterval=" + this.ckInterval);
				} else {
					LOG.error("failed to operate checkpoint dir:" + ckDir.toString());
				}
			} catch (Exception e) {
				LOG.error("failed to operate on bsp.checkpoint.dir", e);
			}
			
			this.reportCounter.set(0);
			this.jobInfo.initAftBuildingInfo(this.job.getNumTotalVertices(), 
					ckDir.toString());
			for (CommunicationServerProtocol comm : comms.values()) {
				try {
					comm.buildRouteTable(jobInfo);
				} catch (Exception e) {
					LOG.error("[buildRouteTable:buildRouteTable]", e);
				}
			}
		}
	}
	
	/** Register after loading graph data and building VE-Block */
	public void registerTask(TaskInformation tif) {
		//LOG.info("[REGISTER] tid=" + statis.getTaskId());
		this.jobInfo.registerInfo(tif.getTaskId(), tif);
		
		if (tif.getTaskId() == 0) {
			if (this.job.isFCM()) {
				GraphRecord[] centroids = new GraphRecord[numOfCentroids];
				for (int i = 0; i < numOfCentroids; i++) {
					centroids[i] = new GraphRecord();
					double[] dims = new double[numOfDimensions];
					Arrays.fill(dims, (i+1.0));
					centroids[i].setDimensions(dims);
				}
				this.csk.set(centroids, null, null); //centroids are not input points
			} else if (this.job.isGMM()) {
				GraphRecord[] sigmas = new GraphRecord[this.numOfCentroids];
				double[] weights = new double[this.numOfCentroids];
				for (int i = 0; i < this.numOfCentroids; i++) {
					sigmas[i] = new GraphRecord();
					sigmas[i].setVerId(i);
					double[] dims = new double[this.numOfDimensions];
					Arrays.fill(dims, 1);
					sigmas[i].setDimensions(dims);
					weights[i] = 1.0/this.numOfCentroids;
				}
				
				this.csk.set(tif.getInitCentroids(), sigmas, weights); //sigms and weights for GMM
			} else {
				this.csk.set(tif.getInitCentroids(), null, null); //initiate centroids from the first task
			}
		}
		
		int finished = this.reportCounter.incrementAndGet();
		if (finished == this.taskNum) {
			this.reportCounter.set(0);
			
			SuperStepCommand ssc = new SuperStepCommand();
			ssc.setCommandType(CommandType.START);
			ssc.setJobAgg(0.0f);
			ssc.setGlobalParameters(this.csk);
			//LOG.info(ssc.printCentroids());

			for (CommunicationServerProtocol comm : this.comms.values()) {
				try {
					comm.setPreparation(this.jobInfo, ssc);
				} catch (Exception e) {
					LOG.error("[registerTask:setPreparation]", e);
				}
			}
			
			this.loadDataTime = 
				System.currentTimeMillis() - this.startTime;
			this.status.setRunState(JobStatus.RUNNING);
			
			switch(this.syncModel) {
			case SyncModel.Concurrent: 
				LOG.info("synchronization model => Concurrent");
				break;
			case SyncModel.Block:
				LOG.info("synchronization model => Block-centric");
				break;
			case SyncModel.SemiAsyn:
				LOG.info("synchronization model => SemiAsyn");
			}
//			if (this.syncModel == SyncModel.SemiAsyn) {
//				this.semiTimer.start();
//			}
			
			LOG.info(jobId.toString() + " loads over, and then starts computations, "
							+ "please wait... (#max_iteration=" + this.maxIteNum + ")");
			this.startTimeIte = System.currentTimeMillis();
			
			curIteNum++;
			this.status.setSuperStepCounter(curIteNum);
			Progress.clear();
		}
	}

	private void openSemiAsyn() {
		synchronized(this.semiFlag) {
			if (this.curIteNum>1 && 
					this.syncModel==SyncModel.SemiAsyn) {
				this.semiFlag = true;
			}
		}
	}
	
	private void closeSemiAsyn() {
		synchronized(this.semiFlag) {
			this.semiFlag = false;
		}
	}
	
	private Boolean isSemiAsyn() {
		synchronized(this.semiFlag) {
			return this.semiFlag;
		}
	}
	
	/** Prepare over before running an iteration */
	public void beginSuperStep(int partitionId) {
		int finished = this.reportCounter.incrementAndGet();
		//LOG.info("[PREPROCESS] tid=" + partitionId + "\tOVER!, finished=" + finished);
		if (finished == this.taskNum) {
			this.reportCounter.set(0);
			for (CommunicationServerProtocol comm : this.comms.values()) {
				try {
					comm.startNextSuperStep();
				} catch (Exception e) {
					LOG.error("[beginSuperStep:startNextSuperStep]", e);
				}
			}
			
			
			openSemiAsyn();
			//LOG.info("superstep-" + curIteNum);
		}
	}
	
	/** Clean over after one iteraiton */
	public void finishSuperStep(int parId, SuperStepReport ssr) {
		long finishTime = System.currentTimeMillis();
		long startSSTime = 0L;
		if (this.job.isGMM()) {
			this.jobMonitor.updateMonitor(
					curIteNum, parId, ssr.getTaskAgg()/this.job.getNumTotalVertices(), ssr.getCounters());
		} else {
			this.jobMonitor.updateMonitor(
					curIteNum, parId, ssr.getTaskAgg(), ssr.getCounters());
		}
		this.aggregatorsOfTasks[parId] = ssr.getAggregatorSet().getAggregators();
		//LOG.info("[SUPERSTEP] taskID=" + parId);
		int finished = this.reportCounter.incrementAndGet();
		this.syncTimeOfTasks[parId] = System.currentTimeMillis() - this.startTimeIte;
		if (finished == this.taskNum) {
			startSSTime = System.currentTimeMillis();
			closeSemiAsyn();
			this.jobMonitor.computeAgg(curIteNum);
			
			this.reportCounter.set(0);
			double timeOfCompute = (System.currentTimeMillis() - this.startTimeIte) / 1000.0;
			//must be invoked before getNextSuperStepCommand, because 
			//computing recovery-time needs current runtime.
			if (this.curIteNum == 1) {
				/*
				//one case that uses median.
				long median = this.getMedianOfSyncTimeOfTasks();
				this.interAdj = new IntervalAdjuster(median*1.0);
				this.intervalOfSync = (int)(median*1.0);*/
				
				
				//one case that uses runtime of the first iteration as input
				this.interAdj = new IntervalAdjuster(timeOfCompute*1000.0);
				this.intervalOfSync = (int)(timeOfCompute*1000.0);
				
				
				
				/*this.intervalOfSync = 
					(int)(time*1000.0*Double.parseDouble(this.job.getBarrierIntervalRatio()));*/
				//LOG.info("full_time=" + time + " sec, ratio=" + this.job.getBarrierIntervalRatio() + ", interval=" + this.intervalOfSync);
			}

			
			SuperStepCommand ssc = getNextSuperStepCommand();
			this.jobInfo.recordIterationCommand(ssc.toString());
			
			for (Entry<Integer, CommunicationServerProtocol> entry: this.comms.entrySet()) {
				try {
					entry.getValue().setNextSuperStepCommand(ssc);
				} catch (Exception e) {
					LOG.error("[finishSuperStep:setNextSuperStepCommand]", e);
				}
			}
			
			if (ssc.getCommandType() == CommandType.STOP) {
				this.status.setRunState(JobStatus.SAVE);
			}
			this.aggregatorTime += (System.currentTimeMillis()-startSSTime);
			
			//record one Iteration's time ,include compute and communitation
			double timeOfOneIte = 
				(System.currentTimeMillis() - this.startTimeIte) / 1000.0;
			this.jobInfo.recordIterationRuntime(timeOfOneIte); 
			this.startTimeIte = System.currentTimeMillis();
			
			this.jobInfo.recordIterationInterval(this.intervalOfSync);
			
			this.interAdj.updateProg(this.jobMonitor.getDeltaAgg(curIteNum));
			
			//compute next interval
			long timeOfCommunication = System.currentTimeMillis() - finishTime;
			if (this.syncTime > 0) {
				this.interAdj.updateSyncTime(timeOfCommunication);
			}
			this.intervalOfSync = this.interAdj.computeNewInterval(
							this.jobMonitor.getDeltaAgg(curIteNum-1), 
							this.jobMonitor.getDeltaAgg(curIteNum), 
							this.curIteNum, this.job.isGMM());
			this.afterComputeInterval = true;
			
			
			curIteNum++;
			this.status.setSuperStepCounter(curIteNum);
			Progress.clear();
			
			
			
			this.openSemiAsyn();
			
			if (this.curIteNum == 2 && this.syncModel == SyncModel.SemiAsyn) {
				this.semiTimer.start();
			}
			
			//LOG.info("===**===Finish the SuperStep-" + this.curIteNum + " ===**===, agg=" + ssc.getJobAgg());
		}
	}

	private long getMedianOfSyncTimeOfTasks() {
		if (this.taskNum == 1) {
			return this.syncTimeOfTasks[0];
		} else {
			for (int i = 0; i < this.taskNum; i++) {
				for (int j = (i+1); j < this.taskNum; j++) {
					if (this.syncTimeOfTasks[i] > this.syncTimeOfTasks[j]) {
						long tmp = this.syncTimeOfTasks[i];
						this.syncTimeOfTasks[i] = this.syncTimeOfTasks[j];
						this.syncTimeOfTasks[j] = tmp;
					}
				}
			}
			
			return this.syncTimeOfTasks[this.taskNum/2];
		}
	}
	
	private SuperStepCommand getNextSuperStepCommand() {
		SuperStepCommand ssc = new SuperStepCommand();
		ssc.setJobAgg(this.jobMonitor.getAgg(curIteNum));
		
		boolean converge = false;
		if (this.job.isGMM()) {
			if (ssc.getJobAgg() > Double.parseDouble(this.job.getConvergeValue())) {
				converge = true;
			}
		} else {
			if (ssc.getJobAgg() < Double.parseDouble(this.job.getConvergeValue())) {
				converge = true;
			}
		}
		
		/*double lastError = 
			curIteNum>1? this.jobMonitor.getAgg(curIteNum-1):Double.MAX_VALUE;
		double curError = this.jobMonitor.getAgg(curIteNum);*/
		
		if ((curIteNum==maxIteNum) || converge) {
			ssc.setCommandType(CommandType.STOP);
		} /*else if (isRecovery()) {
			ssc.setCommandType(CommandType.RECOVERY);
		} else if (isCheckPoint()) {
			ssc.setCommandType(CommandType.CHECKPOINT);
		}*/ else {
			ssc.setCommandType(CommandType.START);
		}
		
		updateCenters();
		ssc.setGlobalParameters(this.csk);
		
		return ssc;
	}
	
	private void updateCenters() {
		for (int centerIdx = 0; centerIdx < this.numOfCentroids; centerIdx++) {
			for (int taskIdx = 0; taskIdx < this.taskNum; taskIdx++) {
				this.aggregators[centerIdx].add(
						this.aggregatorsOfTasks[taskIdx][centerIdx].getSumOfDists(), 
						this.aggregatorsOfTasks[taskIdx][centerIdx].getNumOfPoints());
				if (this.job.isGMM()) {
					this.aggregators[centerIdx].addAggS(
							this.aggregatorsOfTasks[taskIdx][centerIdx].getAggS());
				}
			}

		    double[] centroid = new double[this.numOfDimensions];
		    double[] sigma = new double[this.numOfDimensions];
		    double numOfPoints = this.aggregators[centerIdx].getNumOfPoints();
		    for (int dimIdx = 0; dimIdx < this.numOfDimensions; dimIdx++) {
		    	double x = this.aggregators[centerIdx].getSumOfDists()[dimIdx];
			    centroid[dimIdx] = x/numOfPoints;
			    if (this.job.isGMM()) {
			    	double s = this.aggregators[centerIdx].getAggS()[dimIdx];
			    	sigma[dimIdx] = s/numOfPoints - ((x*x)/(numOfPoints*numOfPoints));
			    }
		    }
		    //LOG.info(Integer.toString(numOfPoints));
		    
		    double weight = numOfPoints/this.job.getNumTotalVertices();
		    
		    this.csk.update(centerIdx, centroid, sigma, weight);
	    }
	}
	
	public void saveResultOver(int parId, int saveRecordNum) {
		int finished = this.reportCounter.incrementAndGet();
		if (finished == this.taskNum) {
			this.reportCounter.set(0);
			this.saveDataTime = System.currentTimeMillis() - this.startTimeIte;
			this.completedJob();
		}
	}
	
	private boolean isCheckPoint() {
		double avgArcVertTime = this.jobInfo.getAvgCheckPointArcVertTime();
		int lastLoc = this.jobInfo.getLastCheckPointArcLocation();
		double recoveryTime = this.jobInfo.computeRecoveryTime(lastLoc);
		boolean smart = avgArcVertTime <= recoveryTime;
		boolean baseline = false;
		if (lastLoc <= 0) {
			baseline = curIteNum%ckInterval == 0;
		} else {
			baseline = (curIteNum-lastLoc) >= ckInterval;
		}
		
		/*
		LOG.info("avgTime=" + avgArcVertTime 
				+ ", lastLocation=" + lastLoc 
				+ ", recoveryTime=" + recoveryTime 
				+ ", smart=" + smart 
				+ ", baseline=" + baseline);
				*/
		boolean ck = false;
		if (this.fcPolicy.equals(FaultTolerancePolicy.Dynamic) 
				|| this.fcPolicy.equals(FaultTolerancePolicy.Priority)) {
			ck = baseline&&smart;
		} else {
			ck = (curIteNum%ckInterval) == 0; //default
		}
		return ck;
	}
	
	private boolean isRecovery() {
		return curIteNum==this.job.getFailedIteration(); //false by default
	}

	private void waitComputeInterval() {
		this.afterComputeInterval = false;
	}
	
	
	private void writeJobInformation() {
		StringBuffer sb = new StringBuffer(
				"\n=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=");
		if (this.status.getRunState() == JobStatus.SUCCEEDED) {
			sb.append("\n    Job has been completed successfully!");
		} else {
			sb.append("\n       Job has been quited abnormally!");
		}
		
		sb.append("\n=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=");
		sb.append("\n              STATISTICS DATA");
		sb.append("\nMaxIterator:  " + this.maxIteNum);
		sb.append("\nAllIterator:  " + this.curIteNum);
		sb.append("\nScheTaskTime: " + this.scheTaskTime / 1000.0 + " seconds");
		sb.append("\nJobRunTime:  " + this.status.getRunCostTime()
				+ " seconds");
		sb.append("\nLoadDataTime: " + this.loadDataTime / 1000.0 + " seconds");
		sb.append("\nIteCompuTime: " + (this.status.getRunCostTime()*1000.0f-
				this.loadDataTime-this.saveDataTime) / 1000.0 + " seconds");
		sb.append("\nSaveDataTime: " + this.saveDataTime / 1000.0 + " seconds");
		
		sb.append(this.jobInfo.toString());
		sb.append(this.jobMonitor.printJobMonitor(this.curIteNum));
		
		sb.append("\nOther Information:");
		sb.append("\n    (1)JobID: " + jobId.toString());
		sb.append("\n    (2)#total_vertices: " + this.jobInfo.getVerNum());
		sb.append("\n    (3)#total_edges: " + this.jobInfo.getEdgeNum());
		sb.append("\n    (4)TaskToWorkerName:");
		for (int index = 0; index < taskToWorkerName.length; index++) {
			sb.append("\n              " + taskToWorkerName[index]);
		}
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		sb.append("\n=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=");
		sb.append("\nlog time: " + sdf.format(new Date()));
		sb.append("\nauthor: HybridGraph");

		MyLOG.info(sb.toString());
		this.jobInfo = null;
		this.jobMonitor = null;
	}
}
