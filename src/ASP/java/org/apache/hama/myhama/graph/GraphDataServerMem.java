package org.apache.hama.myhama.graph;

import java.io.File;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.TaskAttemptID;
import org.apache.hama.monitor.TaskInformation;
import org.apache.hama.myhama.api.GraphRecord;
import org.apache.hama.myhama.comm.MsgPack;
import org.apache.hama.myhama.io.OutputFormat;
import org.apache.hama.myhama.io.RecordWriter;



/**
 * GraphDataDisk manages graph data on local disks.
 * 
 * @author 
 * @version 0.1
 * 
 * @param <V> vertex value
 * @param <W> edge weight
 * @param <M> message value
 * @param <I> graph information
 */
public class GraphDataServerMem<V, W, M, I> extends GraphDataServer<V, W, M, I> {
	private static final Log LOG = LogFactory.getLog(GraphDataServerMem.class);
	
	/** buffer for loading graph data */
	private GraphRecord<V, W, M, I>[][] verBuf; //graph record

	private int pointerOfSend;
	
	private int pointerOfRecive;

	private int totalOfRecive = 0;
	
	@Override
	public int getPointerOfSend() {
		return pointerOfSend;
	}

	/**
	 * Constructing the GraphDataServer object.
	 * 
	 * @param _parId
	 * @param _job
	 * @param _rootDir
	 */
	public GraphDataServerMem(int _parId, BSPJob _job, String _rootDir) {
		super(_parId, _job);
		StringBuffer sb = new StringBuffer();
		sb.append("\n initialize graph data server in memory(push) version;");
		LOG.info(sb.toString());
	    createDir(_rootDir);
	}
	
	private void createDir(String _rootDir) {
		File rootDir = new File(_rootDir);
		if (!rootDir.exists()) {
			rootDir.mkdirs();
		}
	}
	
	@Override
	public void initMemOrDiskMetaData() throws Exception {
		/*int num = this.verBlkMgr.getBlkNum();
		this.verBuf = (GraphRecord<V, W, M, I>[][])new GraphRecord[num][];
		this.verNums = new int[num];*/
		
	}
	
	@Override
	public void loadGraphData(TaskInformation taskInfo, BytesWritable rawSplit, 
			String rawSplitClass) throws Exception {
		long startTime = System.currentTimeMillis();
		initInputSplit(rawSplit, rawSplitClass);
		
		ArrayList<GraphRecord<V, W, M, I>> buf = 
			new ArrayList<GraphRecord<V, W, M, I>>();
		while (input.nextKeyValue()) {
			GraphRecord<V, W, M, I> graph = this.userTool.getGraphRecord();
			/*if (Integer.parseInt(input.getCurrentKey().toString()) == 10802432) {
				continue;
			}*/
			graph.parseGraphData(input.getCurrentKey().toString(), 
					input.getCurrentValue().toString());
			buf.add(graph);
		}
		
		int idx = 0, len = buf.size();
		this.verBuf = (GraphRecord<V, W, M, I>[][])new GraphRecord[2][len];
		this.pointerOfSend = this.verBuf[0].length-1;
		for (GraphRecord<V, W, M, I> point: buf) {
			this.verBuf[0][idx++] = point;
		}
		buf.clear();
		
		long endTime = System.currentTimeMillis();
		LOG.info("load graph from HDFS, costTime=" 
				+ (endTime-startTime)/1000.0 + " seconds");
	}
	
	@Override
	public MsgPack<V, W, M, I> getMsg(int _tid, int _bid, int _iteNum) 
			throws Exception {
		return null;
	}
	
	@Override
	public void clearBefIteMemOrDisk(int _iteNum) {
		
	}
	
	public GraphRecord<V, W, M, I>[][] getPoints() {
			return this.verBuf;
	}
	
	@Override
	public int saveAll(TaskAttemptID taskId, int _iteNum) throws Exception {
		clearBefIteMemOrDisk(_iteNum);
		
		OutputFormat outputformat = 
        	(OutputFormat) ReflectionUtils.newInstance(job.getOutputFormatClass(), 
        		job.getConf());
        outputformat.initialize(job.getConf());
        RecordWriter output = outputformat.getRecordWriter(job, taskId);
        int saveNum = 0;
        
        for (GraphRecord<V, W, M, I> g: this.verBuf[0]) {
    		output.write(new Text(Integer.toString(g.getVerId())), 
					new Text(g.getFinalValue().toString()));
    		saveNum++;
    	}
        
		output.close(job);
		return saveNum;
	}
	
	@Override
	public int archiveCheckPoint(int _version, int _num) throws Exception {
		return 0;
	}

	@Override
	public int loadCheckPoint(int x) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}
	
	@Override
	public GraphRecord[] getInitCentroids(int numOfCentroids) throws Exception {
		GraphRecord[] result = new GraphRecord[numOfCentroids];
		int counter = 0;
		for (GraphRecord g: this.verBuf[0]) {
			GraphRecord centroid = new GraphRecord();
			centroid.setVerId(g.getVerId());
			double[] dimensions = new double[g.getNumOfDimensions()];
			for(int i=0; i<dimensions.length; i++) {
				dimensions[i] = g.getDimensions()[i];
			}
			centroid.setDimensions(dimensions);
			result[counter] = centroid;
			counter++;
			if (counter == numOfCentroids) {
				return result;
			}
		}
		
		if (counter < numOfCentroids) {
			throw new Exception("fail to find enough initial centroids!");
		}
		
		return null;
	}

	@Override
	public void combinePoints(GraphRecord<V, W, M, I>[] points) {
		for (int i = 0; i < points.length; i++) {
			this.verBuf[1][this.pointerOfRecive+i] = points[i];
		}
		LOG.info(this.verBuf[1][this.pointerOfRecive].getVerId());
		this.pointerOfRecive+=points.length;
		
	}

	@Override
	public GraphRecord<V, W, M, I>[] selectPointsTosend(int numOfSendPoints) {
		
		GraphRecord[] sendBuf = new GraphRecord[numOfSendPoints];
		for (int i = 0; i < numOfSendPoints; i++) {
			sendBuf[i] = this.verBuf[0][this.pointerOfSend];
			this.verBuf[0][this.pointerOfSend].setIsSend(true);
			this.pointerOfSend--;
		}
		return sendBuf;
	}
	
	@Override
	public int getTotalOfRecive() {
		return totalOfRecive;
	}

	@Override
	public void setTotalOfRecive(int totalOfRecive) {
		this.totalOfRecive += totalOfRecive;
	}

	@Override
	public void setDemarcationPoint(int totalOfSend) {
		this.verBuf[0][this.verBuf[0].length - totalOfSend].setIsSend(true);
		
	}
}
