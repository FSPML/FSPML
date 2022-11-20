/**
 * copyright 2012-2010
 */
package org.apache.hama.bsp;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

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
import org.apache.hama.myhama.api.Dataset;
import org.apache.hama.myhama.api.GraphRecord;
import org.apache.hama.myhama.api.Dataset.Record;
import org.apache.hama.myhama.comm.CommunicationServer;
import org.apache.hama.myhama.comm.SuperStepCommand;
import org.apache.hama.myhama.comm.SuperStepReport;
import org.apache.hama.myhama.graph.GraphDataServer;
import org.apache.hama.myhama.graph.GraphDataServerMem;
import org.apache.hama.myhama.util.AggregatorSetOfKmeans;
import org.apache.hama.myhama.util.GradientOfSGD;
import org.apache.hama.myhama.util.GraphContext;
import org.apache.hama.myhama.util.TaskReportTimer;
import org.apache.hama.myhama.util.Util;
import org.apache.hama.myhama.util.ConcurenceRunner.TaskManager;
import org.apache.hama.myhama.util.Util.Operator;
import org.apache.hama.myhama.util.TaskReportContainer;
import org.apache.hama.Constants.SyncModel;
import org.apache.hama.cnn.Layer;
import org.apache.hama.cnn.Layer.Size;

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
	
	private ExecutorService simulationService;//Enable multiple computing intensive threads to simulate the heavy computing tasks of the TASK and realize load tilt
	
	private int iteNum = 0;
	private boolean conExe;
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
//	private GraphRecord<V, W, M, I>[] points; 
	private Dataset dataset;

	private int syncModel = SyncModel.Concurrent;
	//simulate the slowest. only works for task-0.
	private int idleTime = -1;
	private int idleThreshold = 1000;
	
	//just for testing
	private long taskRunTime = 0L;
	private long computeTime = 0L;
	private long befSyncTime = 0L;
	private long aftSyncTime = 0L;
	private long blkSyncTime = 0L;
	
	//used in Jiangtao's model
	private double sgl = 0;
	private double vhd = 0;
	private int blockVtxCounter = 0;
	
	private int localVertNum;
	
	private boolean isSemiAsynBarrier = false;
	
	/**
	 * CNN
	 */
	private static final long serialVersionUID = 337920299147929932L;
	private static double ALPHA = 0.85;
	protected static final double LAMBDA = 0;
	// 网络的各层
	private List<Layer> layers;
	// 层数
	private int layerNum;

	// 批量更新的大小
	private int batchSize;
	// 除数操作符，对矩阵的每一个元素除以一个值
	private Operator divide_batchSize;

	// 乘数操作符，对矩阵的每一个元素乘以alpha值
	private Operator multiply_alpha;

	// 乘数操作符，对矩阵的每一个元素乘以1-labmda*alpha值
	private Operator multiply_lambda;
	private SemiAsynTimer semiTimer;

	public int intervalOfSyn = 1000;
	public int allHasPro = 0;
	
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
						Thread.sleep(intervalOfSyn );
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
		this.reportServer = (MasterProtocol) RPC.waitForProxy(
				MasterProtocol.class, MasterProtocol.versionID,
				BSPMaster.getAddress(this.job.getConf()), this.job.getConf());
		
		//this.counters = new Counters();
		this.iteNum = 0;
		this.conExe = true;
		this.trt = new TaskReportTimer(this.jobId, this.taskId, this, 3000);
		
		
		this.syncModel = this.job.getSyncModel();
		this.idleTime = this.job.getIdleTime();
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
		this.localVertNum = this.graphDataServer.loadGraphData(taskInfo, this.rawSplit, 
																	this.rawSplitClass);

		
		this.dataset = this.graphDataServer.getDataset();
		this.fulLoad = this.dataset.getlength();
		
		LOG.info("task enter the registerTask() barrier");
		this.reportServer.registerTask(this.jobId, this.taskInfo);
		this.commServer.barrier();
		LOG.info("task leave the registerTask() barrier");
		
		
		LOG.info("syncModel=" + this.syncModel );
	}
	
	/**
	 * Do some preparetion work at the beginning of a new SuperStep.
	 * Do not report or get any information.
	 */
	private void beginIteration() throws Exception {
		long startTime = System.currentTimeMillis();
		LOG.info("====== Begin SuperStep-" + this.iteNum + " ======");
		
		//this.counters.clearValues();
		this.taskAgg = 0.0f; //clear the local aggregator
		this.hasPro = 0; // clear load
		
		//创建一个卷积神经网络
		LayerBuilder builder = new LayerBuilder();
		builder.addLayer(Layer.buildInputLayer(new Size(28, 28)));
		builder.addLayer(Layer.buildConvLayer(6, new Size(5, 5)));
		builder.addLayer(Layer.buildSampLayer(new Size(2, 2)));
		builder.addLayer(Layer.buildConvLayer(12, new Size(5, 5)));
		builder.addLayer(Layer.buildSampLayer(new Size(2, 2)));
		builder.addLayer(Layer.buildOutputLayer(10));
		initCNN(builder, dataset.size()); //此处需要由master创建初始卷积核分发给各task
		//initCNN(builder,500);
		this.blockVtxCounter = 0;
		
		this.befSyncTime += (System.currentTimeMillis()-startTime);
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
		this.graphDataServer.clearAftIte(iteNum);
		SuperStepReport ssr = new SuperStepReport(); //collect local information
		
		ssr.setTaskAgg(this.taskAgg);


		this.reportServer.finishSuperStep(this.jobId, this.parId, ssr);
		this.commServer.barrier();
		//LOG.info("task leave the finishSuperStep() barrier, iteNum=" + iteNum);
		
		// Get the command from JobInProgress.
		this.ssc = this.commServer.getNextSuperStepCommand();

		
		switch (this.ssc.getCommandType()) {
	  	case START:
	  		this.conExe = true;
	  		break;
	  	case CHECKPOINT:
	  		int arcNum = this.graphDataServer.archiveCheckPoint(iteNum, iteNum);
			this.reportServer.syncArchiveData(jobId, parId, arcNum);
			this.commServer.barrier(); //ensure all tasks complete Pull
	  		this.conExe = true;
	  		break;
	  	case RECOVERY:
	  		int loadNum = this.graphDataServer.loadCheckPoint(iteNum);
	  		this.reportServer.syncLoadData(jobId, parId, loadNum);
	  		this.commServer.barrier();
	  		this.conExe = true;
	  		break;
	  	case STOP:
	  		this.conExe = false;
	  		break;
	  	default:
	  		throw new Exception("[Invalid Command Type] " 
	  				+ this.ssc.getCommandType());
		}
		
		
		this.iteNum++;
		this.taskAgg = 0.0;
		this.aftSyncTime += (System.currentTimeMillis()-startTime);
		this.vhd = System.currentTimeMillis()-startTime;
	}
	
	
	/**
	 * Save local results onto distributed file system, such as HDFS.
	 * @throws Exception
	 */
	private void saveResult() throws Exception {
		int num = this.graphDataServer.saveAll(taskId, iteNum);
        LOG.info("task enter the saveResultOver() barrier");
		this.reportServer.saveResultOver(jobId, parId, num);//�������ļ��浽HDFS��
		LOG.info("task leave the saveResultOver() barrier");
	}
	
	/**
	 * Simulate the slowest task. 
	 * Only works for task-0 when iteNum>1 & idleTime>0 & every 1000 data points.
	 */
	private void idle() {
		if (this.parId<4 && this.idleTime>0 && this.iteNum>1  
				&& (this.hasPro%this.idleThreshold)==0) {
			try {
				Thread.sleep(this.idleTime);
			} catch (InterruptedException e) {
				LOG.error("idle()", e);
			}
		}
	}

	/**
	 * 
	 */
	private void idlethread() {
		this.simulationService = Executors.newCachedThreadPool();
		for (int i = 0; i < 5; i++) {
			this.simulationService.execute(new Thread(() -> {
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

			this.iteNum = 1; //#iteration starts from 1, not 0.
			
			/** run the job iteration by iteration */
			long taskStartTime = System.currentTimeMillis();
			beginIteration(); //preprocess before starting one iteration
			//使用多线程计算，用于模拟倾斜
//			if(this.parId < 4) {
//				idlethread();
//			}
			this.semiTimer.start();
			train(dataset); //计算一次全部数据
			this.semiTimer.stop();
			
			this.taskRunTime = System.currentTimeMillis() - taskStartTime;
			
			//使用多线程计算，用于模拟倾斜
//			if(this.parId<4) {
//				this.simulationService.shutdown();
//			}
			
			saveResult(); //save results and prepare to end
			umbilical.clear(this.jobId, this.taskId);
			LOG.info("task is completed successfully!");
			LOG.info("for testing, taskRunTime= " + this.taskRunTime/1000.0 + " seconds.");
			LOG.info("for testing, computeTime= " + this.computeTime/1000.0 + " seconds.");
			LOG.info("for testing, befSyncTime= " + this.befSyncTime/1000.0 + " seconds.");
			LOG.info("for testing, aftSyncTime= " + this.aftSyncTime/1000.0 + " seconds.");
			LOG.info("for testing, blkSyncTime= " + this.blkSyncTime/1000.0 + " seconds.");
		} catch (Exception e) {
			exception = e;
			LOG.error("task is failed!", e);
		} finally {
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
	
	private void updateCounters() throws Exception {
		
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
	
	/**
	 * 初始化网络
	 * 
	 * @param layerBuilder
	 *            网络层
	 * @throws Exception 
	 */
	public void initCNN(LayerBuilder layerBuilder, final int datasize) throws Exception {
		layers = layerBuilder.mLayers;
		layerNum = layers.size();
		setup(dataset.size());
		initPerator();
	}

	/**
	 * 初始化操作符
	 */
	private void initPerator() {
		divide_batchSize = new Operator() {

			private static final long serialVersionUID = 7424011281732651055L;

			@Override
			public double process(double value) {
				return value / allHasPro;
			}

		};
		multiply_alpha = new Operator() {

			private static final long serialVersionUID = 5761368499808006552L;

			@Override
			public double process(double value) {

				return value * ALPHA;
			}

		};
		multiply_lambda = new Operator() {

			private static final long serialVersionUID = 4499087728362870577L;

			@Override
			public double process(double value) {

				return value * (1 - LAMBDA * ALPHA);
			}

		};
	}
	private boolean isBarrier(int count, int batchSize) {
		boolean isbarrier = false;
		if(this.syncModel == SyncModel.Concurrent) {
			isbarrier = count == dataset.size();
		}else if(this.syncModel == SyncModel.Block) {
			isbarrier = count == batchSize;
		}else {
			isbarrier = (this.iteNum > 1 && (this.isSemiAsynBarrier || count == dataset.size())) || (this.iteNum == 1 && count == dataset.size());
		}
		return isbarrier;
	}

	/**
	 * 在训练集上训练网络
	 * 
	 * @param trainset
	 * @param repeat
	 *            迭代的次数
	 * @throws Exception 
	 */
	public void train(Dataset trainset) throws Exception {

		int count = 0;
		
		while(this.conExe) {
			if(this.iteNum == 1) {
				batchSize = dataset.size();
			}else {
				if(this.syncModel == SyncModel.Block) batchSize = this.job.getBlockSize();
			}
						int right = 0;
			for (int index=0; index<dataset.size(); index++) { //全部数据
				
				boolean isRight = train(trainset.getRecord(index)); //训练一条记录，同时返回是否预测正确当前记录

				count++;
				this.hasPro++;
				if (isRight) right++;
				Layer.prepareForNewRecord();
				
				
				if(isBarrier(count, batchSize)) { //处理了一个batch
				
					// 跑完一个batch后更新权重
					updateParas(count); //更新参数
					count = 0;

					finishIteration(); //更新损失函数, 并完成这一轮迭代
					
					Layer.prepareForNewBatch(); //准备下一个batch的训练
					synchronized (job) {
						this.isSemiAsynBarrier = false;
						job.notify();
					}
				}
				idle();
				if(!this.conExe) break;
			}

			double p = 1.0 * right / dataset.size();
			if (iteNum % 10 == 1 && p > 0.96) {//动态调整准学习速率
				ALPHA = 0.001 + ALPHA * 0.9;
			}

		}

	}

	

	/**
	 * 训练一条记录，同时返回是否预测正确当前记录
	 * 
	 * @param record
	 * @return
	 */
	private boolean train(Record record) {
		forward(record);
		boolean result = backPropagation(record);
		return result;
		// System.exit(0);
	}

	/*
	 * 反向传输
	 */
	private boolean backPropagation(Record record) {
		boolean result = setOutLayerErrors(record);
		setHiddenLayerErrors();
		return result;
	}

	/**
	 * 更新参数
	 * @param haspro 这次处理了多少条数据
	 * @throws Exception 
	 * 
	 */
	private void updateParas(int haspro) throws Exception {
		for (int l = 1; l < layerNum; l++) {
			Layer layer = layers.get(l);
			Layer lastLayer = layers.get(l - 1);
			LOG.info(layerNum);
			LOG.info("layer = " + layer.getType() + " lastLayer= " + lastLayer.getType());
			LOG.info("mpaNum = " + layer.getOutMapNum() + " lastMapNum= " + lastLayer.getOutMapNum());
			
			switch (layer.getType()) {
			case "conv":
			case "output":
				updateBiasANDKernels(layer, lastLayer, haspro);
				break;
			default:
				break;
			}
		}
	}

	/**
	 * 更新偏置
	 * 
	 * @param layer
	 * @param lastLayer
	 * @param haspro 这次处理了多少条数据
	 * @throws Exception 
	 */
	private void updateBiasANDKernels(final Layer layer, Layer lastLayer, int haspro) throws Exception {
		final double[][][][] errors = layer.getErrors();
		int mapNum = layer.getOutMapNum();
		final int lastMapNum = lastLayer.getOutMapNum();
		
		SuperStepReport ssr = new SuperStepReport();
				LOG.info("mpaNum = " + mapNum + " lastMapNum= " + lastMapNum);
		ssr.setupdateKernelsSize(lastMapNum, mapNum);
		ssr.setupdateBiasSize(mapNum);
		ssr.setHasPro(haspro);
		//计算偏置
			for (int j = 0; j < mapNum; j++) {
				double[][] error = Util.sum(errors, j);
				// 更新偏置
				double deltaBias = Util.sum(error) / haspro;
				ssr.setUpdataBias(j, deltaBias);
			}

		
		//计算卷积核的残差
			for (int j = 0; j < mapNum; j++) {
				for (int i = 0; i < lastMapNum; i++) {
					// 对batch的每个记录delta求和
					double[][] deltaKernel = null;
					for (int r = 0; r < haspro; r++) {
						double[][] error = layer.getError(r, j);//获取第recordId记录下第mapNo的残差
						if (deltaKernel == null)
							deltaKernel = Util.convnValid(
									lastLayer.getMap(r, i), error);
						else {// 累积求和
							deltaKernel = Util.matrixOp(Util.convnValid(
									lastLayer.getMap(r, i), error),
									deltaKernel, null, null, Util.plus);
						}
					}
					
					//记录每个[i][j]的deltaKernel
					ssr.setUpdataKernel(i, j, deltaKernel);
				}
			}

		
		//将deltaKernels 和deltaBias汇报给master
		LOG.info("enter updateBiasANDKernels ………………………………………………………………");
		this.reportServer.updateBiasANDKernels(this.jobId, this.parId, ssr);
		this.commServer.barrier();

		// 开始更新偏置
		this.ssc = this.commServer.getNextSuperStepCommand();
		double[] deltaBias = this.ssc.getGlobalCNNParameters().getBias();
		for (int j = 0; j < mapNum; j++) {

			double bias = layer.getBias(j) + ALPHA * deltaBias[j];
			layer.setBias(j, bias);
		}

		
		// 开始更新卷积核
		double[][][][] deltaKernels = this.ssc.getGlobalCNNParameters().getKernel();
		
		this.allHasPro = this.ssc.getGlobalCNNParameters().getAllHasPro();
		LOG.info("all haspro : " + allHasPro);
			for (int j = 0; j < mapNum; j++) {
				for (int i = 0; i < lastMapNum; i++) {
					double[][] deltaKernel = null;
					// 除以batchSize
					deltaKernel = Util.matrixOp(deltaKernels[i][j],
							divide_batchSize);
					// 更新卷积核
					double[][] kernel = layer.getKernel(i, j);
					deltaKernel = Util.matrixOp(kernel, deltaKernel,
							multiply_lambda, multiply_alpha, Util.plus);
					layer.setKernel(i, j, deltaKernel);
				}
			}
	}


	/**
	 * 设置中将各层的残差
	 */
	private void setHiddenLayerErrors() {
		for (int l = layerNum - 2; l > 0; l--) {
			Layer layer = layers.get(l);
			Layer nextLayer = layers.get(l + 1);
			switch (layer.getType()) {
			case "samp":
				setSampErrors(layer, nextLayer);
				break;
			case "conv":
				setConvErrors(layer, nextLayer);
				break;
			default:// 只有采样层和卷积层需要处理残差，输入层没有残差，输出层已经处理过
				break;
			}
		}
	}

	/**
	 * 设置采样层的残差
	 * 
	 * @param layer
	 * @param nextLayer
	 */
	private void setSampErrors(final Layer layer, final Layer nextLayer) {
		int mapNum = layer.getOutMapNum();
		final int nextMapNum = nextLayer.getOutMapNum();

		for (int i = 0; i < mapNum; i++) {
			double[][] sum = null;// 对每一个卷积进行求和
			for (int j = 0; j < nextMapNum; j++) {
				double[][] nextError = nextLayer.getError(j);
				double[][] kernel = nextLayer.getKernel(i, j);
				// 对卷积核进行180度旋转，然后进行full模式下得卷积
				if (sum == null)
					sum = Util
							.convnFull(nextError, Util.rot180(kernel));
				else
					sum = Util.matrixOp(
							Util.convnFull(nextError,
									Util.rot180(kernel)), sum, null,
							null, Util.plus);
			}
			layer.setError(i, sum);
		}


	}

	/**
	 * 设置卷积层的残差
	 * 
	 * @param layer
	 * @param nextLayer
	 */
	private void setConvErrors(final Layer layer, final Layer nextLayer) {
		// 卷积层的下一层为采样层，即两层的map个数相同，且一个map只与令一层的一个map连接，
		// 因此只需将下一层的残差kronecker扩展再用点积即可
		int mapNum = layer.getOutMapNum();
		for (int m = 0; m < mapNum; m++) {
			Size scale = nextLayer.getScaleSize();
			double[][] nextError = nextLayer.getError(m);
			double[][] map = layer.getMap(m);
			// 矩阵相乘，但对第二个矩阵的每个元素value进行1-value操作
			double[][] outMatrix = Util.matrixOp(map,
					Util.cloneMatrix(map), null, Util.one_value,
					Util.multiply);
			outMatrix = Util.matrixOp(outMatrix,
					Util.kronecker(nextError, scale), null, null,
					Util.multiply);
			layer.setError(m, outMatrix); //当前这条数据在这一层，第m个的残差
		}


	}

	/**
	 * 设置输出层的残差值,输出层的神经单元个数较少，暂不考虑多线程
	 * 
	 * @param record
	 * @return
	 */
	private boolean setOutLayerErrors(Record record) {

		Layer outputLayer = layers.get(layerNum - 1);
		int mapNum = outputLayer.getOutMapNum();
		// double[] target =
		// record.getDoubleEncodeTarget(mapNum);
		// double[] outmaps = new double[mapNum];
		// for (int m = 0; m < mapNum; m++) {
		// double[][] outmap = outputLayer.getMap(m);
		// double output = outmap[0][0];
		// outmaps[m] = output;
		// double errors = output * (1 - output) *
		// (target[m] - output);
		// outputLayer.setError(m, 0, 0, errors);
		// }
		// // 正确
		// if (isSame(outmaps, target))
		// return true;
		// return false;

		double[] target = new double[mapNum];
		double[] outmaps = new double[mapNum];
		for (int m = 0; m < mapNum; m++) {
			double[][] outmap = outputLayer.getMap(m);
			outmaps[m] = outmap[0][0];

		}
		int lable = record.getLable().intValue();
		target[lable] = 1;
		// Log.i(record.getLable() + "outmaps:" +
		// Util.fomart(outmaps)
		// + Arrays.toString(target));
		double curLoss = 0.0;
		double denominator = 0.0;
		for (int m = 0; m < mapNum; m++) {
			outputLayer.setError(m, 0, 0, outmaps[m] * (1 - outmaps[m])
					* (target[m] - outmaps[m]));
			denominator += Math.pow(Math.E, outmaps[m]);
		}
		
		double p = Math.pow(Math.E, outmaps[lable]) / denominator;
		curLoss = -(Math.log(p));
		if(this.iteNum % 10 == 0){
			LOG.info("Lable : " + lable);
			LOG.info(Arrays.toString(outmaps));
		}

		double lastLoss = record.getLoss();
		if(lastLoss == 0.0) {
			this.taskAgg += curLoss;
		}else {
			this.taskAgg += (curLoss - lastLoss);
		}
		record.setLoss(curLoss);
		
		return lable == Util.getMaxIndex(outmaps);
	}

	/**
	 * 前向计算一条记录
	 * 
	 * @param record
	 */
	private void forward(Record record) {
		// 设置输入层的map
		setInLayerOutput(record);
		for (int l = 1; l < layers.size(); l++) {
			Layer layer = layers.get(l);
			Layer lastLayer = layers.get(l - 1);
			switch (layer.getType()) {
			case "conv":// 计算卷积层的输出
				setConvOutput(layer, lastLayer);
				break;
			case "samp":// 计算采样层的输出
				setSampOutput(layer, lastLayer);
				break;
			case "output":// 计算输出层的输出,输出层是一个特殊的卷积层
				setConvOutput(layer, lastLayer);
				break;
			default:
				break;
			}
		}
	}

	/**
	 * 根据记录值，设置输入层的输出值
	 * 
	 * @param record
	 */
	private void setInLayerOutput(Record record) {
		final Layer inputLayer = layers.get(0);
		final Size mapSize = inputLayer.getMapSize();
		final double[] attr = record.getAttrs();
		if (attr.length != mapSize.x * mapSize.y)
			throw new RuntimeException("数据记录的大小与定义的map大小不一致!");
		for (int i = 0; i < mapSize.x; i++) {
			for (int j = 0; j < mapSize.y; j++) {
				// 将记录属性的一维向量弄成二维矩阵
				inputLayer.setMapValue(0, i, j, attr[mapSize.x * i + j]);
			}
		}
	}

	/*
	 * 计算卷积层输出值,每个线程负责一部分map
	 */
	private void setConvOutput(final Layer layer, final Layer lastLayer) {
		int mapNum = layer.getOutMapNum();
		final int lastMapNum = lastLayer.getOutMapNum();

		for (int j = 0; j < mapNum; j++) {
			double[][] sum = null;// 对每一个输入map的卷积进行求和
			for (int i = 0; i < lastMapNum; i++) {
				double[][] lastMap = lastLayer.getMap(i);
				double[][] kernel = layer.getKernel(i, j);

				if (sum == null)
					sum = Util.convnValid(lastMap, kernel);
				else{
					sum = Util.matrixOp(
							Util.convnValid(lastMap, kernel), sum,
							null, null, Util.plus);
				}
			}
			final double bias = layer.getBias(j);
			sum = Util.matrixOp(sum, new Operator() {
				private static final long serialVersionUID = 2469461972825890810L;

				@Override
				public double process(double value) {
					return Util.sigmod(value + bias);
				}

			});

			layer.setMapValue(j, sum);
		}


	}

	/**
	 * 设置采样层的输出值，采样层是对卷积层的均值处理
	 * 
	 * @param layer
	 * @param lastLayer
	 */
	private void setSampOutput(final Layer layer, final Layer lastLayer) {
		int lastMapNum = lastLayer.getOutMapNum();

				for (int i = 0; i < lastMapNum; i++) {
					double[][] lastMap = lastLayer.getMap(i);
					Size scaleSize = layer.getScaleSize();
					// 按scaleSize区域进行均值处理
					double[][] sampMatrix = Util.scaleMatrix(lastMap, scaleSize);
					layer.setMapValue(i, sampMatrix);
				}


	}

	/**
	 * 设置cnn网络的每一层的参数
	 * 
	 * @param batchSize
	 *            * @param classNum
	 * @param inputMapSize
	 * @throws Exception 
	 */
	public void setup(int batchSize) throws Exception {
		// TODO Auto-generated method stub
		Layer inputLayer = layers.get(0);
		// 每一层都需要初始化输出map
		inputLayer.initOutmaps(batchSize);
		for (int i = 1; i < layers.size(); i++) {
			Layer layer = layers.get(i);
			Layer frontLayer = layers.get(i - 1);
			int frontMapNum = frontLayer.getOutMapNum();
			switch (layer.getType()) {
			case "input":
				break;
			case "conv":
				// 设置map的大小
				layer.setMapSize(frontLayer.getMapSize().subtract(
						layer.getKernelSize(), 1));
				
				//由master初始化卷积核和偏置
				this.reportServer.initConvPara(this.jobId, this.parId, frontMapNum, layer.getOutMapNum(), 5);
				this.commServer.barrier();
				this.ssc = this.commServer.getNextSuperStepCommand();
				
				// 初始化卷积核，共有frontMapNum*outMapNum个卷积核
				layer.initKernel(this.ssc.getGlobalCNNParameters().getKernel());
				// 初始化偏置，共有frontMapNum*outMapNum个偏置
				layer.initBias(this.ssc.getGlobalCNNParameters().getBias());
				// batch的每个记录都要保持一份残差
				layer.initErros(batchSize);
				// 每一层都需要初始化输出map
				layer.initOutmaps(batchSize);
				break;
			case "samp":
				// 采样层的map数量与上一层相同
				layer.setOutMapNum(frontMapNum);
				// 采样层map的大小是上一层map的大小除以scale大小
				layer.setMapSize(frontLayer.getMapSize().divide(
						layer.getScaleSize()));
				// batch的每个记录都要保持一份残差
				layer.initErros(batchSize);
				// 每一层都需要初始化输出map
				layer.initOutmaps(batchSize);
				break;
			case "output":
				this.reportServer.initOutputPara(this.jobId, this.parId, frontMapNum, layer.getOutMapNum(), frontLayer.getMapSize().x);
				this.commServer.barrier();
				this.ssc = this.commServer.getNextSuperStepCommand();
				// 初始化权重（卷积核），输出层的卷积核大小为上一层的map大小
				layer.initOutputKerkel(this.ssc.getGlobalCNNParameters().getKernel());
				// 初始化偏置，共有frontMapNum*outMapNum个偏置
				layer.initBias(this.ssc.getGlobalCNNParameters().getBias());
				// batch的每个记录都要保持一份残差
				layer.initErros(batchSize);
				// 每一层都需要初始化输出map
				layer.initOutmaps(batchSize);
				break;
			}
		}
	}

	/**
	 * 构造者模式构造各层,要求倒数第二层必须为采样层而不能为卷积层
	 * 
	 * @author jiqunpeng
	 * 
	 *         创建时间：2014-7-8 下午4:54:29
	 */
	public static class LayerBuilder {
		public List<Layer> mLayers;

		public LayerBuilder() {
			mLayers = new ArrayList<Layer>();
		}

		public LayerBuilder(Layer layer) {
			this();
			mLayers.add(layer);
		}

		public LayerBuilder addLayer(Layer layer) {
			mLayers.add(layer);
			return this;
		}
	}

}
