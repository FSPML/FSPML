/**
 * copyright 2012-2010
 */
package org.apache.hama.bsp;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.util.ReflectionUtils;

import org.apache.hama.Constants;
import org.apache.hama.ipc.MasterProtocol;
import org.apache.hama.monitor.TaskInformation;
import org.apache.hama.myhama.api.BSP;
import org.apache.hama.myhama.api.GraphRecord;
import org.apache.hama.myhama.comm.CommunicationServer;
import org.apache.hama.myhama.comm.SuperStepCommand;
import org.apache.hama.myhama.comm.SuperStepReport;
import org.apache.hama.myhama.graph.GraphDataServer;
import org.apache.hama.myhama.graph.GraphDataServerMem;
import org.apache.hama.myhama.util.AggregatorSetOfKmeans;
import org.apache.hama.myhama.util.GradientOfSGD;
import org.apache.hama.myhama.util.GraphContext;
import org.apache.hama.myhama.util.TaskReportTimer;

import org.apache.hama.myhama.util.TaskReportContainer;
import org.apache.hama.Constants.SyncModel;

/**
 * Base class for task. 
 *
 * @author 
 * @version 0.1
 */
public class BSPTask<V, W, M, I> extends Task {
	private static final Log LOG = LogFactory.getLog(BSPTask.class);
	
	private String rootDir;
	private BytesWritable rawSplit = new BytesWritable();
	private String rawSplitClass;
	
	private MasterProtocol reportServer;
	private CommunicationServer<V, W, M, I> commServer;
	private GraphDataServer<V, W, M, I> graphDataServer;
	
	private ExecutorService idleService;
	
	private int iteNum = 0;
	private boolean conExe;
	private BSP<V, W, M, I> bsp;
	private SuperStepCommand ssc;
	private TaskInformation taskInfo;
	private double taskAgg = 0.0; //the local aggregator
	
	//private Counters counters; //count some variables
	private int fulLoad = 0; //its value is equal with #local buckets
	private int hasPro = 0; //its value is equal with #processed buckets
	//java estimated, just accurately at the sampling point.
	private float totalMem, usedMem; 
	//self-computed in bytes. maximum value. 
	//record the memUsage of previous iteration.
	private float last, cur;
	private TaskReportTimer trt;
	
	private int numOfCentroids, numOfDimensions;
	private GraphRecord<V, W, M, I>[][] points; 
	private GraphRecord<V, W, M, I>[] centroids; //[numOfCentroids], global centroids
	private GraphRecord<V, W, M, I>[] sigmas; //[numOfCentroids], global sigmas, used in GMM
	private double[] weights; //[numOfCentroids], global weights, used in GMM
	private double[] theta; 
	private AggregatorSetOfKmeans aggSet; //[numOfCentroids], local aggregators
	private GradientOfSGD gradSet;
	private int syncModel = SyncModel.Concurrent;
	
	//simulate the slowest task, only works for tasks with this.parId<idleTaskNum.
	private int idleTime = -1;
	private int idleThreshold = 1000;
	private int idleTaskNum = 0;
	
	//just for testing
	private long taskRunTime = 0L;
	private long computeTime = 0L;
	private long blkSyncTime = 0L;

	private int intervalOfSyn = 1000;
	private boolean isSemiAsynBarrier = false;
	private SemiAsynTimer semiTimer;
	
	private int[] stragglerInfo = new int[2];

	private int basrHasPro; //ite2 和ite3 处理的数据量

	//用于保存ite1 的计算环境
	private double[] baseTheta;

	private GraphRecord<V, W, M, I>[] baseCentroids;

	private GraphRecord<V, W, M, I>[] baseSigmas;

	private double[] baseWeights;

	private int startRepeatIte = 1;

	private int baseIdx = -1;
	

	class SemiAsynTimer extends Thread {
		public void run() {
			while (true) {
				synchronized (job) {
					while (isSemiAsynBarrier) {
						try {
							job.wait();
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
					try {
						Thread.sleep(intervalOfSyn);
						isSemiAsynBarrier = true;
					} catch (Exception e) {
						LOG.error("[SemiAsynTimer]", e);
					}
				}

			}
		}
	}
	
	
	
	public BSPTask() {
		
	}
	
	public BSPTask(BSPJobID jobId, String jobFile, TaskAttemptID taskid, int parId, String splitClass,
			BytesWritable split) {
		this.jobId = jobId;
		this.jobFile = jobFile;
		this.taskId = taskid;
		this.parId = parId;
		this.rawSplitClass = splitClass;
		this.rawSplit = split;
	}
	


	@Override
	public BSPTaskRunner createRunner(GroomServer groom) {
		return new BSPTaskRunner(this, groom, this.job);
	}
	
	/**
	 * Get progress of this task, including progress and memory information.
	 * @return
	 * @throws Exception
	 */
	public TaskReportContainer getProgress() throws Exception {
		this.cur = iteNum==0? 
				graphDataServer.getProgress():(float)hasPro/fulLoad;
		if (last != cur) { 
			//update and send current progress only when the progress is changed
			TaskReportContainer taskRepCon = 
				new TaskReportContainer(cur, usedMem, totalMem);
			last = cur;
			return taskRepCon;
		} else {
			return null;
		}
	}

	/**
	 * Initialize all variables before executing the computation.
	 * @param job
	 * @param host
	 */
	private void initialize(BSPJob job, String hostName) throws Exception {
		this.job = job;
		this.job.set("host", hostName);
		
		this.rootDir = this.job.get("bsp.local.dir") + "/" + this.jobId.toString()
				+ "/task-" + this.parId;
		this.graphDataServer = 
			new GraphDataServerMem<V, W, M, I>(this.parId, this.job, 
				this.rootDir+"/"+Constants.Graph_Dir);
	
		this.commServer = 
			new CommunicationServer<V, W, M, I>(this.job, this.parId, this.taskId);
		//使用静态方法getProxy或waitForProxy构造客户端代理对象，直接通过代理对象调用远程端的方法
		this.reportServer = (MasterProtocol) RPC.waitForProxy(
				MasterProtocol.class, 
				MasterProtocol.versionID,
				BSPMaster.getAddress(this.job.getConf()), 
				this.job.getConf());
		
		//this.counters = new Counters();
		this.iteNum = 0;
		this.conExe = true;
		this.bsp = (BSP<V, W, M, I>) 
			ReflectionUtils.newInstance(this.job.getConf().getClass(
				"bsp.work.class", BSP.class), this.job.getConf());
		this.trt = new TaskReportTimer(this.jobId, this.taskId, this, 3000);
		
		this.numOfDimensions = this.job.getNumOfDimensions();
		if (this.job.isLOGR()) {
			this.gradSet = new GradientOfSGD(this.numOfDimensions-1);
		}else {
			this.numOfCentroids = this.job.getNumOfCenters();
			this.aggSet = new AggregatorSetOfKmeans(this.numOfCentroids, this.numOfDimensions, job.isGMM());
		}
		
		this.syncModel = this.job.getSyncModel();
		this.idleTime = this.job.getIdleTime();
		this.idleTaskNum = this.job.getNumOfIdleTasks();
		LOG.info("syncModel=" + this.syncModel 
				+ ", idleTime=" + this.idleTime + " millseconds, idleTaskNum=" + this.idleTaskNum);
		this.semiTimer = new SemiAsynTimer();
		
	}
	
	/**
	 * Build route table and get the global information 
	 * about real and virtual hash buckets.
	 * First read only one {@link GraphRecord} and get the min vertex id.
	 * Second, report {@link TaskInformation} to the {@link JobInProgress}.
	 * The report information includes: verMinId, parId, 
	 * RPC server port and hostName.
	 * This function should be invoked before load().
	 * @throws Exception
	 */
	private void buildRouteTable(BSPPeerProtocol umbilical) throws Exception {
		int verMinId = this.graphDataServer.getVerMinId(this.rawSplit, 
				this.rawSplitClass);
		this.taskInfo = new TaskInformation(this.parId, verMinId, 
				this.commServer.getPort(), this.commServer.getAddress());
		
		LOG.info("task enter the buildRouteTable() barrier");
		this.reportServer.buildRouteTable(this.jobId, this.taskInfo);
		this.commServer.barrier();
		LOG.info("task leave the buildRouteTable() barrier");
		
		this.taskInfo.init(this.commServer.getJobInformation());
		this.trt.setAgent(umbilical);
		this.trt.start();
	}
	
	/**
	 * Load data from HDFS, build VE-Block, 
	 * and then save them on the local disk.
	 * After that, begin to register to the {@link JobInProgress} to 
	 * report {@link TaskInformation}.
	 * The report information includes: #edges, 
	 * the relationship among virtual buckets.
	 */
	private void loadData() throws Exception {
		this.graphDataServer.initialize(this.taskInfo, 
				this.commServer.getCommRouteTable(), taskId);
		this.graphDataServer.initMemOrDiskMetaData();
		this.graphDataServer.loadGraphData(taskInfo, this.rawSplit, 
				this.rawSplitClass);
		this.commServer.bindGraphData(this.graphDataServer);
		
		if (this.parId == 0 && !this.job.isLOGR()) {
			this.taskInfo.setInitCentroids(
					this.graphDataServer.getInitCentroids(numOfCentroids));
		}
		
		//用于模拟倾斜
//		if(this.parId == 3) {
//			//使用多线程计算，用于模拟倾斜
//			idlethread();
//		}
		
		this.points = this.graphDataServer.getPoints();//加载过后的数据传过来
		this.fulLoad = this.points[0].length;
		
		LOG.info("task enter the registerTask() barrier");
		this.reportServer.registerTask(this.jobId, this.taskInfo);
		this.commServer.barrier();
		LOG.info("task leave the registerTask() barrier");
		if (this.job.isLOGR()) {
			this.theta = this.commServer.getNextSuperStepCommand().getGlobalParameters().getTheta();
			
			this.baseTheta = this.theta;
		}else {
			this.centroids = this.commServer.getNextSuperStepCommand().getGlobalParameters().getCentroids();
			this.sigmas = this.commServer.getNextSuperStepCommand().getGlobalParameters().getSigmas();
			this.weights = this.commServer.getNextSuperStepCommand().getGlobalParameters().getWeights();
			
			//保存最初的质心，用于重复计算
			this.baseCentroids = new GraphRecord[this.centroids.length];
			for (int i = 0; i < baseCentroids.length; i++) {
				GraphRecord<V, W, M, I> centroid = new GraphRecord<>();
				centroid.setVerId(i);
				double[] dim = new double[this.centroids[i].getDimensions().length];
				for (int j = 0; j < dim.length; j++) {
					dim[j] = this.centroids[i].getDimensions()[j];
				}
				centroid.setDimensions(dim);
				baseCentroids[i] = centroid;
			}
			if(this.job.isGMM()) {
				this.baseSigmas = new GraphRecord[this.sigmas.length];
				for (int i = 0; i < sigmas.length; i++) {
					GraphRecord<V, W, M, I> sigma = new GraphRecord<>();
					sigma.setVerId(i);
					double[] dim = new double[this.sigmas[i].getDimensions().length];
					for (int j = 0; j < dim.length; j++) {
						dim[j] = this.sigmas[i].getDimensions()[j];
					}
					sigma.setDimensions(dim);
					baseSigmas[i] = sigma;
				}
				this.baseWeights = new double[this.weights.length];
				for(int i=0; i<baseWeights.length; i++) {
					this.baseWeights[i] = this.weights[i];
				}
			}
		}
		this.intervalOfSyn  = this.commServer.getNextSuperStepCommand().getIntervalOfSyn();//获取第一步迭代的Interval时间；


			
		
	}
	
	/**
	 * Collect the task's information, update {@link SuperStepReport}, 
	 * and then send it to {@link JobInProgress}.
	 * After that, the local task will block itself 
	 * and wait for {@link SuperStepCommand} from 
	 * {@link JobInProgress} for the next SuperStep.
	 */
	private void finishIteration(int dataProCounters) throws Exception {
		long startTime = System.currentTimeMillis();
		SuperStepReport ssr = new SuperStepReport(); //collect local information
		
		ssr.setTaskAgg(this.taskAgg);
		ssr.setHasPro(this.hasPro);
		if (dataProCounters >= 2) {
			ssr.setdataProCounters(dataProCounters);
			
		}
		if (this.job.isLOGR()) {
			ssr.setGradientAgg(this.gradSet);
		}else {
			ssr.setAggregatorSet(this.aggSet);
		}
		
		//LOG.info("the local information is as follows:\n" + ssr.toString());
		
		//LOG.info("task enter the finishSuperStep() barrier");
		this.reportServer.finishSuperStep(this.jobId, this.parId, ssr);
		this.commServer.barrier();
		//LOG.info("task leave the finishSuperStep() barrier");
		
		// Get the command from JobInProgress.
		this.ssc = this.commServer.getNextSuperStepCommand();
		if (this.job.isLOGR()) {
			this.theta = this.ssc.getGlobalParameters().getTheta();
		}else {
			int centerIdx = 0;
			for (GraphRecord<V, W, M, I> centroid: this.ssc.getGlobalParameters().getCentroids()) {
				this.centroids[centerIdx++].setDimensions(centroid.getDimensions());
			}
			this.sigmas = this.ssc.getGlobalParameters().getSigmas();
			this.weights = this.ssc.getGlobalParameters().getWeights();
		}
		this.intervalOfSyn  = ssc.getIntervalOfSyn();
		if(this.parId < 4) {
			this.intervalOfSyn = (int)(0.66*this.intervalOfSyn);
		}
		if(this.ssc.getSecStartIte() > 0) {
			this.startRepeatIte  = this.ssc.getSecStartIte();
		}
		if (this.iteNum == 1) {
			this.stragglerInfo = this.ssc.getStragglerInfo();
		}
		
		if(this.iteNum == this.startRepeatIte - 1) {//保存ite 1 结束后的计算环境，即：质心…… 用于ite4重写计算
			this.baseTheta = this.theta;
			if(!this.job.isLOGR()) {
				this.baseCentroids = new GraphRecord[this.centroids.length];
				for (int i = 0; i < baseCentroids.length; i++) {
					GraphRecord<V, W, M, I> centroid = new GraphRecord<>();
					centroid.setVerId(i);
					double[] dim = new double[this.centroids[i].getDimensions().length];
					for (int j = 0; j < dim.length; j++) {
						dim[j] = this.centroids[i].getDimensions()[j];
					}
					centroid.setDimensions(dim);
					baseCentroids[i] = centroid;
				}
				if(this.job.isGMM()) {
					this.baseSigmas = new GraphRecord[this.sigmas.length];
					for (int i = 0; i < sigmas.length; i++) {
						GraphRecord<V, W, M, I> sigma = new GraphRecord<>();
						sigma.setVerId(i);
						double[] dim = new double[this.sigmas[i].getDimensions().length];
						for (int j = 0; j < dim.length; j++) {
							dim[j] = this.sigmas[i].getDimensions()[j];
						}
						sigma.setDimensions(dim);
						baseSigmas[i] = sigma;
					}
					this.baseWeights = new double[this.weights.length];
					for(int i=0; i<baseWeights.length; i++) {
						this.baseWeights[i] = this.weights[i];
					}
				}
			}
			
		}else if(this.iteNum == this.startRepeatIte + 1) {
			this.theta = this.baseTheta;
			this.centroids = this.baseCentroids;
			this.sigmas = this.baseSigmas;
			this.weights = this.baseWeights;
		}
		
		switch (this.ssc.getCommandType()) {
	  	case CHECKPOINT:
	  	case RECOVERY:
	  	case START:
	  		this.conExe = true;
	  		break;
	  	case STOP:
	  		this.conExe = false;
	  		break;
	  	default:
	  		throw new Exception("[Invalid Command Type] " 
	  				+ this.ssc.getCommandType());
		}
		LOG.info("superstep-" + iteNum + " is done, this superstep conpute --" + this.hasPro + " --points");
		this.blkSyncTime += (System.currentTimeMillis()-startTime);
	}
	
	/**
	 * Save local results onto distributed file system, such as HDFS.
	 * @throws Exception
	 */
	private void saveResult() throws Exception {
		int num = this.graphDataServer.saveAll(taskId, iteNum);
        LOG.info("task enter the saveResultOver() barrier");
		this.reportServer.saveResultOver(jobId, parId, num);
		LOG.info("task leave the saveResultOver() barrier");
	}
	
	/**
	 * Simulate the slowest task. 
	 * Only work for tasks with this.parId<this.idleTaskNum 
	 * when iteNum>1 & idleTime>0 & every 1000 data points.
	 */
	private void idle() {
		if (this.parId<this.idleTaskNum && this.idleTime>0 && this.iteNum>0  
				&& (this.hasPro%this.idleThreshold)==0) {
			try {
				Thread.sleep(this.idleTime);
			} catch (InterruptedException e) {
				LOG.error("idle()", e);
			}
		}
	}
	private int loadBalancing() {
		if (this.stragglerInfo[0] > 0) {
			LOG.info("**************stragglerInfo*********" + Arrays.toString(stragglerInfo));
			return (((stragglerInfo[1] - 1)*points[0].length)/(3*stragglerInfo[1] + 1))*3; //Total number of migrations
		}else {
			return 0;
		}
	}
	/**
	 * 
	 */
	private void idlethread() {
		this.idleService = Executors.newCachedThreadPool();
		for (int i = 0; i < 5; i++) {
			this.idleService.execute(new Thread(() -> {
					Long num1 = System.currentTimeMillis();
					while(true) {
						for (int k = 0; k < 1000; k++) {
							for (int j = 0; j < 10000; j++) {
								num1 += j;
							}
							if (!conExe) {
								break;
							}
							for (int j = 0; j < 10000; j++) {
								num1 = num1 - j;
							}
						}
						if (!conExe) {
							break;
						}
					}
			}));
		}
	}
	
	//用于保持ite3和ite4和ite2单位时间计算数据量相同
	private boolean isBarrier(int hasPro) {
		
		if(this.iteNum == startRepeatIte+2) {
			this.isSemiAsynBarrier = true; 
			return this.basrHasPro == hasPro;
		}else {
			return true;
		}
		
//		if(this.iteNum <= startRepeatIte+1) {
//			this.isSemiAsynBarrier = true; 
//			return 100000 == hasPro;
//		}else if(this.iteNum == startRepeatIte+2) {
//			this.isSemiAsynBarrier = true; 
//			return 200000 == hasPro;
//		}else{
//			return true;
//		}
	}
	

	
	/**
	 * Run and control all work of this task.
	 */
	@Override
	public void run(BSPJob job, Task task, BSPPeerProtocol umbilical, String host) {
		Exception exception = null;
		try {
			initialize(job, host);
			buildRouteTable(umbilical); //get the locMinVerId of each task
			loadData();
			
			if(this.job.getIdleTime() == -2) {
				if(this.parId<4) {
					idlethread();
				}
			}
			GraphContext<V, W, M, I> context = 
				new GraphContext<V, W, M, I>(this.parId, job, -1, null, null, null, null, null, this.startRepeatIte);
			this.bsp.taskSetup(context);
			this.iteNum = 1; //#iteration starts from 1, not 0.
			
			/** run the job iteration by iteration */
			long taskStartTime = System.currentTimeMillis();
			long compTime = System.currentTimeMillis();
			this.hasPro = 0;
			this.taskAgg = 0.0f; //clear the local aggregator
			if (this.job.isLOGR()) {
				this.gradSet.reset();
				context = 
						new GraphContext<V, W, M, I>(this.parId, this.job, this.iteNum, this.gradSet ,this.theta, this.startRepeatIte);
			}else {
				this.aggSet.reset();
				context = 
					new GraphContext<V, W, M, I>(this.parId, this.job, this.iteNum, 
							this.centroids, this.aggSet.getAggregators(), this.sigmas, this.weights, this.theta, this.startRepeatIte);
			}

			this.bsp.superstepSetup(context);
			int num = 0;
			
			//开始Timer
			this.semiTimer.start();

			
			long computeStartTime = System.currentTimeMillis();
			int dataProCounters = 0; //数据被处理了多少遍
			while (this.conExe) {
				dataProCounters++;
				for (int i = 0; i < points.length; i++) {
					if (!this.conExe) {
						break;
					}
					//一轮迭代
					for (int idx = 0; idx < this.points[i].length; idx++) {
						
						if (this.points[i][idx]==null) {
							if(idx>=this.graphDataServer.getTotalOfRecive()) {
								break;
							}else {
								while(this.points[i][idx]==null){
									Thread.sleep(5);
								}
							}
						}
						//判断是否数据已经发走，已发不计算
						if (this.points[i][idx].getIsSend()) {
							break;
						}
						context.reset();
						context.initialize(this.points[i][idx], -1);
						this.bsp.update(context); //execute the local computation
						this.taskAgg += context.getVertexAgg();
						this.hasPro++;
						num++;
						//simulate the slowest task.
						idle();
						
						//自动识别慢的task
						//记录1000条数据计算时间，并发送至master对比
						if(this.iteNum == 2 && (this.parId < 4) && this.job.getIsMigration()>0) {
//						if(this.iteNum == 2 && (this.parId < this.stragglerInfo[0])) {	
							int totalOfSend = 330000;
							if (totalOfSend > 0 ) {
								this.commServer.sendPoints(totalOfSend, idx, this.job.getIsMigration());
								LOG.info("will sent *************" +  totalOfSend);
							}
							Arrays.fill(this.stragglerInfo, 0);
						}
						

						
						
						//for semi-asyn method
						if (this.syncModel==SyncModel.SemiAsyn && isBarrier(hasPro) && this.isSemiAsynBarrier ) {
							if(this.iteNum <= this.startRepeatIte+1) this.basrHasPro += hasPro;
							long iterationTime = System.currentTimeMillis()-compTime;
							LOG.info("System.currentTimeMillis()-compTime is " + iterationTime);
							this.computeTime += (iterationTime);
							
							finishIteration(dataProCounters); //syn, report counters and get the next command
							this.bsp.superstepCleanup(context);
							umbilical.increaseSuperStep(jobId, taskId);
							iteNum++;
							if (!this.conExe) {
								break;
							}
							if (this.iteNum == this.startRepeatIte) {
								this.baseIdx = idx;
								this.basrHasPro = 0;
							}else if(this.iteNum == this.startRepeatIte+2) {
								idx = this.baseIdx;
							}
							
							compTime = System.currentTimeMillis();
							this.hasPro = 0;
							this.taskAgg = 0.0f; //clear the local aggregator
							if (this.job.isLOGR()) {
								this.gradSet.reset();
								context = 
										new GraphContext<V, W, M, I>(this.parId, this.job, this.iteNum, this.gradSet ,this.theta, this.startRepeatIte);
							}else {
								this.aggSet.reset();
								context = 
									new GraphContext<V, W, M, I>(this.parId, this.job, this.iteNum, 
											this.centroids, this.aggSet.getAggregators(), this.sigmas, this.weights, this.theta, this.startRepeatIte);
							}
							this.bsp.superstepSetup(context);
							
							synchronized (job) {
								this.isSemiAsynBarrier = false;
								job.notify();
							}
							
						}
					}
				}
				
//				if (this.iteNum==1 || this.syncModel==SyncModel.Concurrent) {
//					LOG.info("all point compute time is " + (System.currentTimeMillis()-computeStartTime));
//					this.computeTime += (System.currentTimeMillis()-compTime);
//					finishIteration(); //syn, report counters and get the next command
//					this.bsp.superstepCleanup(context);
//					umbilical.increaseSuperStep(jobId, taskId);
//					iteNum++;
//					if (!this.conExe) {
//						break;
//					}
//					
//					compTime = System.currentTimeMillis();
//					this.hasPro = 0;
//					this.taskAgg = 0.0f; //clear the local aggregator
//					if (this.job.isLOGR()) {
//						this.gradSet.reset();
//						context = 
//								new GraphContext<V, W, M, I>(this.parId, this.job, this.iteNum, this.gradSet ,this.theta);
//					}else {
//						this.aggSet.reset();
//						context = 
//							new GraphContext<V, W, M, I>(this.parId, this.job, this.iteNum, 
//									this.centroids, this.aggSet.getAggregators(), this.sigmas, this.weights, this.theta);
//					}
//					this.bsp.superstepSetup(context);
//					computeStartTime = System.currentTimeMillis();
//					this.semiTimer.start();
//					
//				}
				LOG.info("----------------this is " + this.iteNum + " diedai and this verBuf has " + num + " points can compute");
				num = 0;
			}//全部迭代步结束
			
			//结束模拟
			if(this.job.getIdleTime() == -2) {
				if(this.parId<4) {
					this.idleService.shutdown();
				}
			}
			
			
			this.computeTime += (System.currentTimeMillis()-compTime);
			this.taskRunTime = System.currentTimeMillis() - taskStartTime;
			
			saveResult(); //save results and prepare to end
			umbilical.clear(this.jobId, this.taskId);
			LOG.info("task is completed successfully!");
			LOG.info("for testing, taskRunTime= " + this.taskRunTime/1000.0 + " seconds.");
			LOG.info("for testing, computeTime= " + this.computeTime/1000.0 + " seconds.");
			LOG.info("for testing, blkSyncTime= " + this.blkSyncTime/1000.0 + " seconds.");
		} catch (Exception e) {
			exception = e;
			LOG.error("task is failed!", e);
		} finally {
			GraphContext<V, W, M, I> context = 
				new GraphContext<V, W, M, I>(this.parId, job, -1, null, null, null, null, null, this.startRepeatIte);
			this.bsp.taskCleanup(context);
			//umbilical.clear(this.jobId, this.taskId);
			try {
				clear();
				done(umbilical);
			} catch (Exception e) {
				//do nothing
			}
			umbilical.reportException(jobId, taskId, exception);
		}
	}
	
	@SuppressWarnings("deprecation")
	private void clear() throws Exception {
		this.graphDataServer.close();
		this.commServer.close();
		this.trt.stop();
		this.semiTimer.stop();
	}
	
	@Override
	public void setBSPJob(BSPJob job) {
		this.job = job;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		Text.writeString(out, rawSplitClass);
		rawSplit.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		rawSplitClass = Text.readString(in);
		rawSplit.readFields(in);
	}
}
