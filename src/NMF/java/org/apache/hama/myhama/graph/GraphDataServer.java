package org.apache.hama.myhama.graph;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hama.Constants;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.TaskAttemptID;
import org.apache.hama.monitor.TaskInformation;
import org.apache.hama.myhama.api.BSP;
import org.apache.hama.myhama.api.GraphRecord;
import org.apache.hama.myhama.api.MsgRecord;
import org.apache.hama.myhama.api.UserTool;
import org.apache.hama.myhama.comm.CommRouteTable;
import org.apache.hama.myhama.comm.MsgPack;
import org.apache.hama.myhama.io.InputFormat;
import org.apache.hama.myhama.io.RecordReader;

/**
 * GraphDataServer used to manage graph data. 
 * It is implemented by {@link GraphDataServerMem} for memory 
 * and {@link GraphDataServerDisk} for disk.
 *
 * @param <V> vertex value
 * @param <W> edge weight
 * @param <M> message value
 * @param <I> graph information
 */
public abstract class GraphDataServer<V, W, M, I> {
	private static final Log LOG = LogFactory.getLog(GraphDataServer.class);
	
	protected BSPJob job;
	protected RecordReader<?,?> input; //used to load graph data
	protected UserTool<V, W, M, I> userTool;
	protected BSP<V, W, M, I> bsp;
	protected CommRouteTable<V, W, M, I> commRT;
	protected int taskId;
	
	protected CheckPoint ckHandler;
	
	
	
	@SuppressWarnings("unchecked")
	public GraphDataServer(int _taskId, BSPJob _job) {
		taskId = _taskId;
		job = _job;

		userTool = 
	    	(UserTool<V, W, M, I>) ReflectionUtils.newInstance(job.getConf().getClass(
	    			Constants.USER_JOB_TOOL_CLASS, UserTool.class), job.getConf());

		bsp = 
			(BSP<V, W, M, I>) ReflectionUtils.newInstance(this.job.getConf().getClass(
					"bsp.work.class", BSP.class), this.job.getConf());
	}
	
	/**
	 * Initialize the {@link RecordReader} of reading raw graph data on HDFS.
	 * @param rawSplit
	 * @param rawSplitClass
	 * @return
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	protected void initInputSplit(BytesWritable rawSplit, 
			String rawSplitClass) throws Exception {
		DataInputBuffer splitBuffer = new DataInputBuffer();
	    splitBuffer.reset(rawSplit.getBytes(), 0, rawSplit.getLength());
	    SerializationFactory factory = new SerializationFactory(job.getConf());
		
	    Deserializer<? extends org.apache.hadoop.mapreduce.InputSplit>
	      deserializer = 
	        (Deserializer<? extends org.apache.hadoop.mapreduce.InputSplit>) 
	        factory.getDeserializer(job.getConf().getClassByName(rawSplitClass));
	    deserializer.open(splitBuffer);
	    InputSplit split = deserializer.deserialize(null);
	    
		InputFormat inputformat = 
			(InputFormat) ReflectionUtils.newInstance(this.job.getConf().getClass(
                Constants.USER_JOB_INPUT_FORMAT_CLASS, 
                InputFormat.class), this.job.getConf());
		inputformat.initialize(this.job.getConf());
		this.input = inputformat.createRecordReader(split, this.job);
		this.input.initialize(split, job.getConf());
	}
	
	/**
	 * Initialize some variables the {@link GraphDataServer} object 
	 * after that {@link CommRouteTable} has been initialized in 
	 * {@link BSPTask}.buildRouteTable() 
	 * and the number of Vblocks has been calculated/initialized 
	 * by {@link JobInProgress}/the user-specified parameter. 
	 * 
	 * @param taskInfo
	 * @param _commRT
	 * @throws Exception
	 */
	public void initialize(TaskInformation taskInfo, 
			final CommRouteTable<V, W, M, I> _commRT, 
			TaskAttemptID taskAttId) throws Exception {
		commRT = _commRT;
		
		ckHandler = new CheckPoint(this.job, taskAttId, 
				this.commRT.getCheckPointDirAftBuildRouteTable());
	}
	
	/**
	 * Get the minimum vertex Id by reading 
	 * the first graph record (adjacency list).
	 * 
	 * @return vertexid int
	 * @throws Exception 
	 */
	public int getVerMinId(BytesWritable rawSplit, 
			String rawSplitClass) throws Exception {
		initInputSplit(rawSplit, rawSplitClass);
		int id = 0;
		GraphRecord<V, W, M, I> record = userTool.getGraphRecord();
		if(input.nextKeyValue()) {
			record.parseGraphData(input.getCurrentKey().toString(), 
					input.getCurrentValue().toString());
			id = record.getVerId();
		} else {
			id = -1;
		}
		return id;
	}
	
	/**
	 * Report the progress of loading data 
	 * by invoking getProgress() of RecordReader in HDFS.
	 * 
	 * @return float [0.0, 1.0]
	 * @throws Exception
	 */
	public float getProgress() throws Exception {
		return this.input.getProgress();
	}
	
	/**
	 * Initialize metadata used by in memory/disk server in 
	 * the {@link GraphDataServer} object 
	 * after that {@link CommRouteTable} has been initialized in 
	 * {@link BSPTask}.buildRouteTable() 
	 * and the number of Vblocks has been calculated/initialized 
	 * by {@link JobInProgress}/the user-specified parameter. 
	 * 
	 * @throws Exception
	 */
	public abstract void initMemOrDiskMetaData() throws Exception;
	
	/**
	 * Read data from HDFS, create {@link GraphRecord} and then save data 
	 * into the local task.
	 * The original {@link GraphRecord} will be decomposed by invoking 
	 * user-defined function 
	 * into vertex data and outgoing edge data. 
	 * 
	 * Note: 
	 * 1) for b-pull, vertex/edge data are organized in VBlocks/EBlocks;
	 * 2) for push, vertex data are also managed in VBlocks, 
	 *    but edge data are presented in the adjacency list. 
	 * 3) for hybrid, 
	 *    vertex data ---- VBlocks
	 *    edge data ---- EBlocks and adjacency list.
	 */
	public abstract void loadGraphData(TaskInformation taskInfo, 
			BytesWritable rawSplit, String rawSplitClass) throws Exception;
	
	/**
	 * Get {@link MsgRecord}s based on the outbound edges in local task.
	 * This function should be invoked by RPC to pull messages.
	 * 
	 * @param _tid
	 * @param _bid
	 * @param _iteNum
	 * @return
	 * @throws Exception
	 */
	public abstract MsgPack<V, W, M, I> getMsg(int _tid, int _bid, int _iteNum) 
			throws Exception;
	
	/**
	 * Do preprocessing work before launching a new iteration, 
	 * such as deleting files out of date 
	 * and clearing counters in {@link VerBlockMgr}.
	 * Note that the latter can make sure the correct of 
	 * the function hasNextGraphRecord().
	 * 
	 * @param _iteNum
	 */
	public void clearBefIte(int _iteNum, int _preIteStyle, int _curIteStyle, 
			boolean estimate) throws Exception {		
		
	}
	
	/** 
	 * :)
	 **/
	public abstract void clearBefIteMemOrDisk(int _iteNum);
	
	/**
	 * Do some work after the workload of one iteration on the local task 
	 * is completed. 
	 * 
	 * @param _iteNum
	 */
	public void clearAftIte(int _iteNum) {
		
	}
	
	public abstract GraphRecord<V, W, M, I>[][] getPoints();
	
	/**
	 * Save all final results onto the distributed file system, 
	 * now the default is HDFS.
	 * Note that only vertex id and value of a {@link GraphRecord} is saved.
	 * 
	 * @param taskId
	 * @param _iteNum
	 * @return
	 * @throws Exception
	 */
	public abstract int saveAll(TaskAttemptID taskId, int _iteNum) throws Exception;
	
	/**
	 * Archive checkpoint.
	 * 
	 * @param _version
	 * @param _iteNum
	 * @return Number of archived vertices
	 * @throws Exception
	 */
	public abstract int archiveCheckPoint(int _version, int _iteNum) throws Exception;
	
	/**
	 * Load the most recent available checkpoint.
	 * 
	 * @param _iteNum
	 * @return Number of vertices loaded from checkpoint (0 if no checkpoint is available)
	 * @throws Exception
	 */
	public abstract int loadCheckPoint(int _iteNum) throws Exception;
	
	/**
	 * Only work on the first task, i.e., task-0.
	 * @return
	 */
	public abstract GraphRecord[] getInitCentroids(int numOfCentroids) throws Exception;
	
	public void close() {
		
	}

	/**
	 * Combine the received data with verbuf
	 * @param points
	 */
	public abstract void combinePoints(GraphRecord<V, W, M, I>[] points);

	/**
	 * select points to send
	 * @param numOfSendPoints
	 * @return
	 */
	public abstract GraphRecord<V, W, M, I>[] selectPointsTosend(int numOfSendPoints);
	
	public abstract int getPointerOfSend();
	
	public abstract int getTotalOfRecive();

	public abstract void setTotalOfRecive(int totalOfRecive);

	public abstract void setDemarcationPoint(int totalOfSend);

}
