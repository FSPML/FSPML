package org.apache.hama.myhama.graph;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

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


import org.apache.hama.myhama.api.Dataset;
import org.apache.hama.myhama.api.Dataset.Record;;

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
	private Dataset dataset; //graph record
	
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
	public int loadGraphData(TaskInformation taskInfo, BytesWritable rawSplit, 
			String rawSplitClass) throws Exception {
		long startTime = System.currentTimeMillis();
		initInputSplit(rawSplit, rawSplitClass);
		
//		ArrayList<GraphRecord<V, W, M, I>> buf = 
//			new ArrayList<GraphRecord<V, W, M, I>>();
		List<Record> records = new ArrayList<Record>();
		Dataset dataset = new Dataset();
		dataset.lableIndex = 784;
		while (input.nextKeyValue()) {
			int key = Integer.valueOf(input.getCurrentKey().toString());
			String valueString = input.getCurrentValue().toString();
			String[] datas = valueString.split(",");
			double[] data = new double[datas.length];
			for (int i = 0; i < datas.length; i++)
				data[i] = Double.parseDouble(datas[i]);
			Record record = dataset.new Record(data);
			record.setOrder(key);
			dataset.append(record);
		}
		
		this.dataset = dataset;
		
		long endTime = System.currentTimeMillis();
		LOG.info("load graph from HDFS, costTime=" 
				+ (endTime-startTime)/1000.0 + " seconds");
		return records.size();
	}
	
	@Override
	public MsgPack<V, W, M, I> getMsg(int _tid, int _bid, int _iteNum) 
			throws Exception {
		return null;
	}
	
	@Override
	public void clearBefIteMemOrDisk(int _iteNum) {
		
	}
	
	
	@Override
	public Dataset getDataset() {
		// TODO Auto-generated method stub
		return this.dataset;
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
        
//        for (GraphRecord<V, W, M, I> g: this.verBuf) {
//    		output.write(new Text(Integer.toString(g.getVerId())), 
//					new Text(g.getFinalValue().toString()));
//    		saveNum++;
//    	}
        
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


}
