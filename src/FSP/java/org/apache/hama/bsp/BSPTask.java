/**
 * copyright 2012-2010
 */
package org.apache.hama.bsp;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
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
	private GraphRecord<V, W, M, I>[] points; 
	private GraphRecord<V, W, M, I>[] centroids; //[numOfCentroids], global centroids
	private GraphRecord<V, W, M, I>[] sigmas; //[numOfCentroids], global sigmas, used in GMM
	private double[] weights; //[numOfCentroids], global weights, used in GMM
	private AggregatorSetOfKmeans aggSet; //[numOfCentroids], local aggregators
	private int syncModel = SyncModel.Concurrent;
	
	//simulate the slowest task, only works for tasks with this.parId<idleTaskNum.
	private int idleTime = -1;
	private int idleThreshold = 1000;
	private int idleTaskNum = 0;
	
	//just for testing
	private long taskRunTime = 0L;
	private long computeTime = 0L;
	private long blkSyncTime = 0L;
	

	
	public BSPTask() {
		
	}
	
	public BSPTask(BSPJobID jobId, String jobFile, TaskAttemptID taskid,
			int parId, String splitClass, BytesWritable split) {
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
		
		this.numOfCentroids = this.job.getNumOfCenters();
		this.numOfDimensions = this.job.getNumOfDimensions();
		this.aggSet = new AggregatorSetOfKmeans(this.numOfCentroids, this.numOfDimensions, job.isGMM());
		this.syncModel = this.job.getSyncModel();
		this.idleTime = this.job.getIdleTime();
		this.idleTaskNum = this.job.getNumOfIdleTasks();
		LOG.info("syncModel=" + this.syncModel 
				+ ", idleTime=" + this.idleTime + " millseconds, idleTaskNum=" + this.idleTaskNum);
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
		
		if (this.parId == 0) {
			this.taskInfo.setInitCentroids(
					this.graphDataServer.getInitCentroids(numOfCentroids));
		}
		
		this.points = this.graphDataServer.getPoints();//加载过后的数据传过来
		this.fulLoad = this.points.length;
		
		LOG.info("task enter the registerTask() barrier");
		this.reportServer.registerTask(this.jobId, this.taskInfo);
		this.commServer.barrier();
		LOG.info("task leave the registerTask() barrier");
		this.centroids = this.commServer.getNextSuperStepCommand().getGlobalParameters().getCentroids();
		this.sigmas = this.commServer.getNextSuperStepCommand().getGlobalParameters().getSigmas();
		this.weights = this.commServer.getNextSuperStepCommand().getGlobalParameters().getWeights();
	}
	
	/**
	 * Collect the task's information, update {@link SuperStepReport}, 
	 * and then send it to {@link JobInProgress}.
	 * After that, the local task will block itself 
	 * and wait for {@link SuperStepCommand} from 
	 * {@link JobInProgress} for the next SuperStep.
	 */
	private void finishIteration() throws Exception {
		long startTime = System.currentTimeMillis();
		SuperStepReport ssr = new SuperStepReport(); //collect local information
		
		ssr.setTaskAgg(this.taskAgg);
		ssr.setAggregatorSet(this.aggSet);
		//LOG.info("the local information is as follows:\n" + ssr.toString());
		
		//LOG.info("task enter the finishSuperStep() barrier");
		this.reportServer.finishSuperStep(this.jobId, this.parId, ssr);
		this.commServer.barrier();
		//LOG.info("task leave the finishSuperStep() barrier");
		
		// Get the command from JobInProgress.
		this.ssc = this.commServer.getNextSuperStepCommand();
		int centerIdx = 0;
		for (GraphRecord<V, W, M, I> centroid: this.ssc.getGlobalParameters().getCentroids()) {
			this.centroids[centerIdx++].setDimensions(centroid.getDimensions());
		}
		this.sigmas = this.ssc.getGlobalParameters().getSigmas();
		this.weights = this.ssc.getGlobalParameters().getWeights();
		
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
		if (this.parId<this.idleTaskNum && this.idleTime>0 && this.iteNum>1  
				&& (this.hasPro%this.idleThreshold)==0) {
			try {
				Thread.sleep(this.idleTime);
			} catch (InterruptedException e) {
				LOG.error("idle()", e);
			}
		}
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
			GraphContext<V, W, M, I> context = 
				new GraphContext<V, W, M, I>(this.parId, job, -1, null, null, null, null);
			this.bsp.taskSetup(context);
			this.iteNum = 1; //#iteration starts from 1, not 0.
			
			/** run the job iteration by iteration */
			long taskStartTime = System.currentTimeMillis();
			long compTime = System.currentTimeMillis();
			this.hasPro = 0;
			this.taskAgg = 0.0f; //clear the local aggregator
			this.aggSet.reset();
			context = 
				new GraphContext<V, W, M, I>(this.parId, this.job, this.iteNum, 
						this.centroids, this.aggSet.getAggregators(), this.sigmas, this.weights);
			this.bsp.superstepSetup(context);
			while (this.conExe) {
				//一轮迭代
				for (int idx = 0; idx < this.points.length; idx++) {
					context.reset();
					context.initialize(this.points[idx], -1);
					this.bsp.update(context); //execute the local computation
					this.taskAgg += context.getVertexAgg();
					this.hasPro++;
					
					//simulate the slowest task.
					idle();
					
					//for semi-asyn method
					if (this.syncModel==SyncModel.SemiAsyn && this.commServer.isSemiAsynBarrier()) {
						this.computeTime += (System.currentTimeMillis()-compTime);
						
						finishIteration(); //syn, report counters and get the next command
						this.bsp.superstepCleanup(context);
						umbilical.increaseSuperStep(jobId, taskId);
						iteNum++;
						if (!this.conExe) {
							break;
						}
						
						compTime = System.currentTimeMillis();
						this.hasPro = 0;
						this.taskAgg = 0.0f; //clear the local aggregator
						this.aggSet.reset();
						context = 
							new GraphContext<V, W, M, I>(this.parId, this.job, this.iteNum, 
									this.centroids, this.aggSet.getAggregators(), this.sigmas, this.weights);
						this.bsp.superstepSetup(context);
					}
				}
				if (this.iteNum==1 || this.syncModel==SyncModel.Concurrent) {
					this.computeTime += (System.currentTimeMillis()-compTime);
					finishIteration(); //syn, report counters and get the next command
					this.bsp.superstepCleanup(context);
					umbilical.increaseSuperStep(jobId, taskId);
					iteNum++;
					if (!this.conExe) {
						break;
					}
					
					compTime = System.currentTimeMillis();
					this.hasPro = 0;
					this.taskAgg = 0.0f; //clear the local aggregator
					this.aggSet.reset();
					context = 
						new GraphContext<V, W, M, I>(this.parId, this.job, this.iteNum, 
								this.centroids, this.aggSet.getAggregators(), this.sigmas, this.weights);
					this.bsp.superstepSetup(context);
				}
			}//全部迭代步结束
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
				new GraphContext<V, W, M, I>(this.parId, job, -1, null, null, null, null);
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
