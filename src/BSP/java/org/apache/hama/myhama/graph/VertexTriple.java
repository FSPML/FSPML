package org.apache.hama.myhama.graph;

/**
 * A vertex triple in VBlock. 
 */
public class VertexTriple<V, W, M, I> {
	protected int verId;
	protected V verValue;
	protected I graphInfo;
	protected Integer[] eIds; //adj edges
	protected W[] eWeights; //adj edges
	
	public VertexTriple(int _verId, V _verValue, I _graphInfo) {
		verId = _verId;
		verValue = _verValue;
		graphInfo = _graphInfo;
	}
	
	public void setAdjEdges(Integer[] _eIds, W[] _eWeights) {
		eIds = _eIds;
		eWeights = _eWeights;
	}
	
	public final int getVerId() {
		return verId;
	}
	
	public V getVerValue() {
		return verValue;
	}
	
	public void setVerValue(V _verValue) {
		verValue = _verValue;
	}
	
	public final I getGraphInfo() {
		return graphInfo;
	}
	
	public final int getEdgeNum() {
		return eIds==null? 0:eIds.length;
	}
	
	public final Integer[] getEdgeIds() {
		return eIds;
	}
	
	public final W[] getEdgeWeights() {
		return eWeights;
	}
}
