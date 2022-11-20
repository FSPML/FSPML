package org.apache.hama.myhama.graph;

import org.apache.hama.myhama.api.GraphRecord;

/**
 * An edge fragment. 
 * When running {@link Constants}.STYLE.Push, 
 * a fragment maintains all edges of the corresponding source vertices. 
 * When running {@link Constants}.STYLE.Pull, 
 * only edges belonging to one (dstTid,dstBid) grid are hold.
 */
public class EdgeFragment<V, W, M, I> {
	protected int verId;
	protected int edgeNum;
	protected Integer[] edgeIds;
	protected W[] edgeWeights;
	
	/**
	 * Construct an edge fragment using the given source vertex id.
	 * @param _verId
	 */
	public EdgeFragment(int _verId) { verId = _verId; }
	
	public void initialize(Integer[] _edgeIds, W[] _edgeWeights) {
		edgeIds = _edgeIds;
		edgeWeights = _edgeWeights;
		edgeNum = _edgeIds==null? 0:edgeIds.length;
	}
	
	/**
	 * Return the source vertex id.
	 * @return
	 */
	public int getVerId() {
		return this.verId;
	}
	
	public Integer[] getEdgeIds() {
		return this.edgeIds;
	}
	
	public W[] getEdgeWeights() {
		return this.edgeWeights;
	}
	
	/**
	 * Get edges required in {@link BSPInterface}.getMessages(), 
	 * in order to respond pull requests from target vertices. 
	 * Edges in {@link GraphRecord} is read-only.
	 * 
	 * @param record
	 */
	public void getForRespond(GraphRecord<V, W, M, I> record) {
		record.setVerId(verId);
		//record.setEdgeNum(edgeNum);
		//record.setEdges(edgeIds, edgeWeights);
	}
}
