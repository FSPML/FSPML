package org.apache.hama.myhama.graph;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * VerBlockMgr manages metadata information of local VBlocks.
 * @author root
 * 
 * @param <V> vertex value
 * @param <W> edge weight
 * @param <M> message value
 * @param <I> graph information
 */
public class VerBlockMgr {
	private static final Log LOG = LogFactory.getLog(VerBlockMgr.class);
	private int blkNum;
	private int blkLen;
	private int verMinId, verMaxId; //source vids in local task
	private int verNum; //number of source vertices in local task
	private long edgeNum; //number of outgoing edges in local task
	
	private VerBlockBeta[] blocks; //block-id, metadata
	
	/**
	 * VerBlockMgr
	 * @param _verMinId
	 * @param _verMaxId
	 * @param _blkNum
	 * @param _blkLen
	 */
	public VerBlockMgr(int _verMinId, int _verMaxId, int _blkNum, int _blkLen, 
			int _taskNum, int[] _blkNumTask, int bspStyle) {
		verMinId = _verMinId;
		verMaxId = _verMaxId;
		verNum = _verMaxId - _verMinId + 1;
        blkNum = _blkNum;
        blkLen = _blkLen;
        
        int sum = _verMinId;
        blocks = new VerBlockBeta[_blkNum];
        LOG.info("initialize VerBlkMgr with #blocks=" + _blkNum);
        for (int bid = 0; bid < _blkNum; bid++) {
        	int tmpLen = blkLen;
        	if (bid == (blkNum-1)) {
        		tmpLen = _verMaxId - sum + 1;
        	}
        	
        	//distribute mini bucket evenly
        	blocks[bid] = new VerBlockBeta(bid, sum, (sum+tmpLen-1), tmpLen, _taskNum);
        	sum += tmpLen;
        }
	}
	
	/**
	 * Return the total memory usage size of all VBlocks' metadata.
	 * @return
	 */
	public long getMemUsage() {
		long usage = 5 * 4; //fiveint variables
		usage += 8; //one long variable
		for (int bid = 1; bid < this.blkNum; bid++) {
			usage += this.blocks[bid].getMemUsage();
		}
		
		return usage;
	}
	
	/**
	 * Set the number of outgoing edges in this task.
	 * @param _edgeNum
	 */
	public void setEdgeNum(long _edgeNum) {
		this.edgeNum = _edgeNum;
	}
	
	/**
	 * In the loadGraphData() function of {@link GraphDataServer}, 
	 * it should be invoked after finishing loading graph data from HDFS, 
	 * to compute the total #vertices and #edges of this task, 
	 * and set the starting offset for edge data files of each local block.
	 */
	public void loadOver(int bspStyle, int taskNum, int[] blkNumTask) {
		
	}
	
	/**
	 * Get the metadata information of one VBlock.
	 * @param _bid
	 * @return
	 */
	public VerBlockBeta getVerBlkBeta(int _bid) {
		return this.blocks[_bid];
	}
	
	/**
	 * Get the number of VBlocks in this task.
	 * @return
	 */
	public int getBlkNum() {
		return blkNum;
	}
	
	/**
	 * Get the maximum number of source vertices in one local VBlock.
	 * @return
	 */
	public int getBlkLen() {
		return blkLen;
	}
	
	/**
	 * Get the minimum source vertex id in this task.
	 * @return
	 */
	public int getVerMinId() {
		return verMinId;
	}
	
	/**
	 * Get the maximum source vertex id in this task.
	 * @return
	 */
	public int getVerMaxId() {
		return verMaxId;
	}
	
	/**
	 * Get the number of source vertices in this task.
	 * @return
	 */
	public int getVerNum() {
		return verNum;
	}
	
	/**
	 * Get the number of outgoing edges in this task.
	 * @return
	 */
	public long getEdgeNum() {
		return edgeNum;
	}
	
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer("VerBlocks Info.\n");
		for (int i = 0; i < blkNum; i++) {
			sb.append(this.blocks[i].toString()); sb.append("\n");
		}
		return sb.toString();
	}
}
