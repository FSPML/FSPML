package org.apache.hama.myhama.graph;

public class EdgeHashBucBeta {
	private int bucId;
	
	private int eMinId, eMaxId; //destination vertex id of edges
	private int vNum = 0; //#source vertices
	private long eNum = 0L;
	
	public EdgeHashBucBeta(int _bucId) {
		bucId = _bucId;
		eMinId = Integer.MAX_VALUE;
		eMaxId = 0;
	}
	
	public long getMemUsage() {
		return (4*4 + 1*8);
	}
	
	public void updateNum(int _vNum, long _eNum) {
		vNum += _vNum;
		eNum += _eNum;
	}
	
	public int getBucId() {
		return bucId;
	}
	
	public int getVerNum() {
		return vNum;
	}
	
	public long getEdgeNum() {
		return eNum;
	}
	
	public void updateEdgeIdBound(int eId) {
		eMinId = eMinId<eId? eMinId:eId;
		eMaxId = eMaxId>eId? eMaxId:eId;
	}
	
	public boolean isInEdgeIdBound(int eId) {
		if (eId >= eMinId && eId <= eMaxId) {
			return true;
		} else {
			return false;
		}
	}
	
	public int getEdgeMinId() {
		return this.eMinId;
	}
	
	public int getEdgeMaxId() {
		return this.eMaxId;
	}
	
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("bucId=" + bucId);
		sb.append(" eId=[" + eMinId + " " + eMaxId + "]");
		sb.append(" num=[" + vNum + " " + eNum + "]");
		return sb.toString();
	}
}
