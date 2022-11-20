package org.apache.hama.myhama.graph;

/**
 * EdgeFragmentEntry used when decomposing a new 
 * {@link GraphRecord} during loading.
 * @author root
 *
 * @param <W>
 */
public class EdgeFragmentEntry<V,W,M,I> extends EdgeFragment<V,W,M,I> {
	private int srcBid=-1, dstTid=-1, dstBid=-1;
	
	public EdgeFragmentEntry(int _vid,  
			int _srcBid, int _dstTid, int _dstBid) {
		super(_vid);
		
		srcBid = _srcBid;
		dstTid = _dstTid;
		dstBid = _dstBid;
	}
	
	public int getSrcBid() {
		return this.srcBid;
	}
	
	public int getDstTid() {
		return this.dstTid;
	}
	
	public int getDstBid() {
		return this.dstBid;
	}
}
