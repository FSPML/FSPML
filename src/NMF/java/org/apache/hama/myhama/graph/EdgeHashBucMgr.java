package org.apache.hama.myhama.graph;

//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;

public class EdgeHashBucMgr {
	//private static final Log LOG = LogFactory.getLog(EdgeHashBucMgr.class);
	private EdgeHashBucBeta[][] buckets; //Tid, Bid, beta
	
	public EdgeHashBucMgr(int _taskNum, int[] _bucNumTask) {
        buckets = new EdgeHashBucBeta[_taskNum][];
        for (int i = 0; i < _taskNum; i++) {
        	buckets[i] = new EdgeHashBucBeta[_bucNumTask[i]];
        	for (int j = 0; j < _bucNumTask[i]; j++) {
        		buckets[i][j] = new EdgeHashBucBeta(j);
        	}
        }
	}
	
	public long getMemUsage() {
		return 
			(this.buckets.length*this.buckets[0].length) * this.buckets[0][0].getMemUsage();
	}
	
	public void updateBucNum(int _dstTid, int _dstBid, int _verNum, long _edgeNum) {
		buckets[_dstTid][_dstBid].updateNum(_verNum, _edgeNum);
	}
	
	public int getBucVerNum(int _dstTid, int _dstBid) {
		return this.buckets[_dstTid][_dstBid].getVerNum();
	}
	
	public void updateBucEdgeIdBound(int _dstTid, int _dstBid, int eid) {
		buckets[_dstTid][_dstBid].updateEdgeIdBound(eid);
	}
	
	public boolean isInBucEdgeIdBound(int _dstTid, int _dstBid, int eid) {
		return buckets[_dstTid][_dstBid].isInEdgeIdBound(eid);
	}
	
	public int getBucEdgeMinId(int _dstTid, int _dstBid) {
		return this.buckets[_dstTid][_dstBid].getEdgeMinId();
	}
	
	public int getBucEdgeMaxId(int _dstTid, int _dstBid) {
		return this.buckets[_dstTid][_dstBid].getEdgeMaxId();
	}
	
	public long getBucEdgeNum(int _dstTid, int _dstBid) {
		return this.buckets[_dstTid][_dstBid].getEdgeNum();
	}
	
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer("EdgeHashBucMgr Info.\n");
		for (int i = 0; i < this.buckets.length; i++) {
			sb.append("\ndstTid=" + i);
			for (int j = 0; j < this.buckets[i].length; j++) {
				sb.append("\n");
				sb.append(this.buckets[i][j].toString());
			}
		}
		return sb.toString();
	}
}
