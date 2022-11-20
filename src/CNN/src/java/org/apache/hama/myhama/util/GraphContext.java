/**
 * Termite System
 * copyright 2012-2010
 */
package org.apache.hama.myhama.util;

import org.apache.hama.bsp.BSPJob;
import org.apache.hama.myhama.api.GraphRecord;

/**
 * GraphContext. 
 * The extended class is used by the core engine of Hybrid.
 * This class contains information about a {@link GraphRecord}, including:
 * 
 * @param <V> vertex value
 * @param <W> edge weight
 * @param <M> message value
 * @param <I> graph information
 */
public class GraphContext<V, W, M, I> extends Context<V, W, M, I> {
	
	public GraphContext(int _taskId, BSPJob _job, int _iteNum, 
			GraphRecord<V, W, M, I>[] _centroids, 
			AggregatorOfKmeans[] _aggregators, 
			GraphRecord<V, W, M, I>[] _sigmas, 
			double[] _weights,
			double[] _theta) {
		super(_taskId, _job, _iteNum);
		this.centroids = _centroids;
		this.aggregators = _aggregators;
		this.sigmas = _sigmas;
		this.weights = _weights;
		this.theta = _theta;
	}
	
	public GraphContext(int _taskId, BSPJob _job, int _iteNum, GradientOfSGD _gradSet, double[] _theta) {
		super(_taskId, _job, _iteNum);
		this.gradients = _gradSet;
		this.theta = _theta;
	}
	
	public void setVBlockId(int id) {
		this.vBlkId = id;
	}
	
	/**
	 * Initialize {@link GraphContext}.
	 * @param _graph
	 * @param _msg
	 * @param _jobAgg
	 * @param _actFlag
	 */
	public void initialize(GraphRecord<V, W, M, I> _graph, double _jobAgg) {
		graph = _graph;
		jobAgg = _jobAgg;
	}
	
	/**
	 * Get the aggregator value from one vertex.
	 * @return
	 */
	public double getVertexAgg() {
		return this.vAgg;
	}
	
	public void reset() {
		this.vAgg = 0.0;
	}
}
