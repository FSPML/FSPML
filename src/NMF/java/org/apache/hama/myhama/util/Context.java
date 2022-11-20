/**
 * Termite System
 * copyright 2012-2010
 */
package org.apache.hama.myhama.util;

import com.aparapi.Kernel;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.myhama.api.GraphRecord;

/**
 * Context defines some functions for users to implement 
 * their own algorithms. 
 * Context includes the following variables:
 * (1) {@link BSPJob};
 * (2) {@link GraphRecord};
 * (3) superstepCounter;
 * (4) message;
 * (5) jobAgg;
 * (6) vAgg;
 * (7) iteStyle;
 * (8) taskId;
 * (9) vBlockId;
 * 
 * @param <V> vertex value
 * @param <W> edge weight
 * @param <M> message value
 * @param <I> graph information
 */
public class Context<V, W, M, I> {
	protected BSPJob job;      //task level
	protected int taskId;      //task level
	protected int iteNum;      //superstep level
	protected int vBlkId;      //VBlock id
	
	/** variables associated with each vertex */
	protected GraphRecord<V, W, M, I> graph;
	protected double jobAgg;
	protected double vAgg;
	
	/** global centroids computed in the previous iteration */
	protected GraphRecord<V, W, M, I>[] centroids;
	/** aggregators */
	protected AggregatorOfKmeans[] aggregators;
	/** global sigma parameter used in GMM */
	protected GraphRecord<V, W, M, I>[] sigmas;
	/** global weights parameter used in GMM */
	protected double[] weights;
	protected double[] theta;
	
	/** gradient used in sgd */
	protected GradientOfSGD gradients;
	
	/** nmf */
	protected double[][] matrixH;
	protected double[] matrixW;
//	protected GraphRecord<V, W, M, I>[] matrixV;
	protected int line; // 记录每个分块数据的总条数
	protected MatrixOfNMF matSet;

	protected int startIdx = -1;
	protected int endIdx = -1;
	protected String hostname;
	protected double[][] gpuPoints;
	protected GraphRecord<V, W, M, I>[] groupPoints;
	protected GraphRecord<V, W, M, I>[] points;
	protected Kernel kernel;
	protected GraphRecord<V, W, M, I>[][] localPoints;

	public Context(int _taskId, BSPJob _job, int _iteNum) {
		this.taskId = _taskId;
		this.job = _job;
		this.iteNum = _iteNum;
		this.vBlkId = -1;
	}
	
	/**
	 * Get a {@link GraphRecord}.
	 * You can modifiy its value
	 * @return
	 */
	public GraphRecord<V, W, M, I> getGraphRecord() {
		return graph;
	}
	
	/**
	 * The centroids are read-only.
	 * @return
	 */
	public final GraphRecord<V, W, M, I>[] getCentroids() {
		return this.centroids;
	}
	
	/**
	 * You can modify the aggregators.
	 * @return
	 */
	public AggregatorOfKmeans[] getAggregators() {
		return this.aggregators;
	}
	
	public GradientOfSGD getGradients() {
		return gradients;
	}

	/**
	 * Only used in GMM.
	 * @return
	 */
	public final GraphRecord<V, W, M, I>[] getSigmas() {
		return this.sigmas;
	}
	
	/**
	 * Only used in GMM.
	 * @return
	 */
	public final double[] getWeights() {
		return this.weights;
	}
	
	/**
	 * Only used in logR.
	 * @return
	 */
	public double[] getTheta() {
		return theta;
	}
	/**
	 * Get the read-only global aggregator value calculated 
	 * at the previous superstep. 
	 * Now, HybridGraph only provides a simple sum-aggregator. 
	 * The returned value only makes sense in 
	 * {@link BSP}.superstepSetup()/Cleanup(), vBlockSetup/Cleanup(), 
	 * and update().
	 * @return
	 */
	public final double getJobAgg() {
		return jobAgg;
	}
	
	/**
	 * Get the read-only superstep counter.
	 * It makes sense in {@link BSP}.superstepSetup()/Cleanup(), 
	 * vBlockSetup()/Cleanup(), update(), and getMessages().
	 * The counter starts from 1.
	 * @return
	 */
	public final int getSuperstepCounter() {
		return iteNum;
	}
	
	/**
	 * Get the read-only {@link BSPJob} object which 
	 * keeps configuration information 
	 * and static global statics of this job.
	 * @return
	 */
	public final BSPJob getBSPJobInfo() {
		return job;
	}
	
	/**
	 * Get the task id. Read-only.
	 * @return
	 */
	public final int getTaskId() {
		return taskId;
	}
	
	/**
	 * Get the read-only VBlock id.
	 * It makes sense in {@link BSP}.vBlockSetup()/Cleanup() and update(). 
	 * @return
	 */
	public final int getVBlockId() {
		return this.vBlkId;
	}
	
	/**
	 * Set the sum-aggregator value based on this vertex.
	 * {@link JobInProgress} will sum up the values of all vertices 
	 * and then send the global aggregator value to all {@link BSPTask} 
	 * to be used at the next superstep by invoking getJobAgg().
	 * This function only makes sense in {@link BSP}.update().
	 * @param agg
	 */
	public void setVertexAgg(double agg) {
		this.vAgg = agg;
	}

	public synchronized void addVertexAgg(double agg) {
		this.vAgg += agg;
	}

	public double[][] getMatrixH() {
		return this.matrixH;
	}
	
	public void setMatrixW(double[] matrixW) {
		this.matrixW = matrixW;
	}
	
	public double[] getMatrixW() {
		return this.matrixW;
	}

	public int getLine() {
		return this.line;
	}
	
//	public GraphRecord<V, W, M, I>[] getMatriV() {
//		return this.matrixV;
//	}
	
	public MatrixOfNMF getMatSet() {
		return this.matSet;
	}
	
	public void setMatSet(MatrixOfNMF matSet) {
		this.matSet = matSet;
	}

	public void setStartIdx(int startIdx) {
		this.startIdx = startIdx;
	}

	public int getStartIdx() {
		return startIdx;
	}

	public void setEndIdx(int endIdx) {
		this.endIdx = endIdx;
	}

	public int getEndIdx() {
		return endIdx;
	}

	public void setHostname(String hostname) {
		this.hostname = hostname;
	}

	public String getHostname() {
		return hostname;
	}

	public void setGpuPoints(double[][] gpuPoints) {
		this.gpuPoints = gpuPoints;
	}

	public double[][] getGpuPoints() {
		return gpuPoints;
	}

	public void setGroupPoints(GraphRecord<V, W, M, I>[] groupPoints) {
		this.groupPoints = groupPoints;
	}

	public GraphRecord<V, W, M, I>[] getGroupPoints() {
		return groupPoints;
	}

	public void setPoints(GraphRecord<V, W, M, I>[] points) {
		this.points = points;
	}

	public GraphRecord<V, W, M, I>[] getPoints() {
		return points;
	}

	public void setKernel(Kernel kernel) {
		this.kernel = kernel;
	}

	public Kernel getKernel() {
		return kernel;
	}

	public void setLocalPoints(GraphRecord<V, W, M, I>[][] localPoints) {
		this.localPoints = localPoints;
	}

	public GraphRecord<V, W, M, I>[][] getLocalPoints() {
		return localPoints;
	}
}