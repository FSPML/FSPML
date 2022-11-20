package org.apache.hama.myhama.graph;


/**
 * Metadata of one VBlock
 * @author root
 */
public class VerBlockBeta {
	private int bid;
	private int granularity = 0; //#tasks * #blocks per task
	
	private int vMinId, vMaxId; //source vertex id in this VBlock
	private int vNum = 0; //number of vertices in this VBlock
	
	public VerBlockBeta(int _bid, int _verMinId, int _verMaxId, int _verNum, 
			int _taskNum) {
		bid = _bid;
		vMinId = _verMinId;
		vMaxId = _verMaxId;
		vNum = _verNum;
	}
	
	public long getMemUsage() {
		long usage = 8 * 4; //ten int variables
		usage += 2; //two boolean variables
		usage += (8*2) * this.granularity; //eFragStart and eFragLen
		usage += 4 * this.granularity; //eFragNum
		usage += 4; //totalFragNum
		
		return usage;
	}
	
	/**
	 * Get the number of source vertices in this VBlock.
	 * @return
	 */
	public int getVerNum() {
		return vNum;
	}
	
	/**
	 * Get the minimum source vertex id in this VBlock.
	 * @return
	 */
	public int getVerMinId() {
		return vMinId;
	}
	
	/**
	 * Get the maximum source vertex id in this VBlock.
	 * @return
	 */
	public int getVerMaxId() {
		return vMaxId;
	}
	
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("bucId=" + bid);
		sb.append(" vId=[" + vMinId + " " + vMaxId + "]");
		sb.append(" num=[" + vNum + "]");
		sb.append("\n");
		return sb.toString();
	}
}
