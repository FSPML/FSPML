/**
 * copyright 2011-2016
 */
package hybridgraph.examples.fcm.distributed;

import org.apache.hama.myhama.api.BSP;
import org.apache.hama.myhama.api.GraphRecord;
import org.apache.hama.myhama.util.AggregatorOfKmeans;
import org.apache.hama.myhama.util.Context;

/**
 * FCMBSP.java implements {@link BSP}.
 * 
 * Fuzzy C-Means implementation.
 * 
 * @author 
 * @version 0.1
 */
public class FCMBSP extends BSP<FCMValue, Integer, Integer, Integer> {
	private double fuzzy_m = 1.1;
	private int numOfCentroids, numOfDimensions;
	private GraphRecord<FCMValue, Integer, Integer, Integer>[] centroids;
	
	@Override
	public void taskSetup(
			Context<FCMValue, Integer, Integer, Integer> context) {
		this.numOfCentroids = context.getBSPJobInfo().getNumOfCenters();
		this.numOfDimensions = context.getBSPJobInfo().getNumOfDimensions();
	}
	
	@Override
	public void superstepSetup(
			Context<FCMValue, Integer, Integer, Integer> context) {
		this.centroids = context.getCentroids();
	}
	
	@Override
	public void update(
			Context<FCMValue, Integer, Integer, Integer> context) 
				throws Exception {
		GraphRecord<FCMValue, Integer, Integer, Integer> point = 
			context.getGraphRecord();
		AggregatorOfKmeans[] aggregators = context.getAggregators();
		
		double[] dist = new double[numOfCentroids];
		for (int j = 0; j < numOfCentroids; j++) {
			dist[j] = distance(point.getDimensions(), 
								centroids[j].getDimensions());
		}
		
		double cur_fuzzy_error = 0.0;
		double last_fuzzy_error = point.getVerValue().getError();
		double[] cur_val_miu = new double[numOfCentroids];
		double[] last_val_miu = (context.getSuperstepCounter()==1? 
				new double[numOfCentroids]:point.getVerValue().getMiu());
		for (int j = 0; j < numOfCentroids; j++) {
			/** compute fuzzy_miu */
			double sum = 0.0;
			for (int k = 0; k < numOfCentroids; k++) {
				sum += Math.pow(dist[j]/dist[k], 2.0/(fuzzy_m-1.0));
			}
			cur_val_miu[j] = Math.pow(1.0/sum, fuzzy_m); //fuzzy miu
			
			/** compute error */
			cur_fuzzy_error += cur_val_miu[j]*Math.pow(dist[j], 2.0);
			
			/** update aggregators */
			double diff = cur_val_miu[j] - last_val_miu[j];
			double[] aggOfSum = new double[numOfDimensions];
			double[] dims = point.getDimensions();
			for (int k = 0; k < numOfDimensions; k++) {
				aggOfSum[k] += diff*dims[k];
			}
			aggregators[j].add(aggOfSum, diff);
		}
		
		/** update errors */
		context.setVertexAgg(cur_fuzzy_error-last_fuzzy_error);
		
		/** update vertex values */
		point.getVerValue().set(cur_fuzzy_error, cur_val_miu);
	}
	
	private double distance(double[] point, double[] center) {
		double sum = 0.0, dist = 0.0;
		for (int i = 0; i < numOfDimensions; i++) {
			dist = Math.abs(point[i]-center[i]);
			sum += dist * dist;
		}
		
		return Math.sqrt(sum);
	}
}