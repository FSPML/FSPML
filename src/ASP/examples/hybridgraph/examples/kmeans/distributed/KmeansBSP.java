/**
 * copyright 2011-2016
 */
package hybridgraph.examples.kmeans.distributed;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hama.bsp.BSPTask;
import org.apache.hama.myhama.api.BSP;
import org.apache.hama.myhama.api.GraphRecord;
import org.apache.hama.myhama.util.AggregatorOfKmeans;
import org.apache.hama.myhama.util.Context;

/**
 * KmeansBSP.java implements {@link BSP}.
 * 
 * Traditional Kmeans implementation.
 * 
 * @author 
 * @version 0.1
 */
public class KmeansBSP extends BSP<KmeansValue, Integer, Integer, Integer> {
	private static final Log LOG = LogFactory.getLog(KmeansBSP.class);
	private int numOfCentroids, numOfDimensions;
	private GraphRecord<KmeansValue, Integer, Integer, Integer>[] centroids;
	
	@Override
	public void taskSetup(
			Context<KmeansValue, Integer, Integer, Integer> context) {
		this.numOfCentroids = context.getBSPJobInfo().getNumOfCenters();
		this.numOfDimensions = context.getBSPJobInfo().getNumOfDimensions();
	}
	
	@Override
	public void superstepSetup(
			Context<KmeansValue, Integer, Integer, Integer> context) {
		this.centroids = context.getCentroids();
	}
	
	@Override
	public void update(
			Context<KmeansValue, Integer, Integer, Integer> context) 
				throws Exception {
		GraphRecord<KmeansValue, Integer, Integer, Integer> point = 
			context.getGraphRecord();
		AggregatorOfKmeans[] aggregators = context.getAggregators();
		int startRepeatIte = context.getStartRepeatIte();
		int iteNum = context.getSuperstepCounter();
		
		KmeansValue lastValue = point.getVerValue();
		KmeansValue curValue = new KmeansValue();
		int tag = -1;
		double minDist = Double.MAX_VALUE;
		for (int i = 0; i < this.numOfCentroids; i++) {
			double dist = computeDistance(this.numOfDimensions, 
					point.getDimensions(), this.centroids[i].getDimensions());
			//dist = 0.0f;
			if (dist < minDist) {
				minDist = dist;
				tag = i;
			}
		}
		curValue.set(tag, minDist);
		if(iteNum <= startRepeatIte+1) {
			point.setBaseVerValue(lastValue);
		}else if (iteNum == 3) {
			context.setVertexAgg(curValue.getDistance());
		}else if(iteNum == startRepeatIte+2){
			lastValue = point.getBaseVerValue();
			context.setVertexAgg(curValue.getDistance() - lastValue.getDistance());
		}
		
		if(point.isFirstCompute()) {//第一遍计算
			point.setFirstCompute(false);
			point.setVerValue(curValue);
			aggregators[curValue.getTag()].add(point.getDimensions(), 1);
			context.setVertexAgg(curValue.getDistance());
			
		}else { //非第一遍计算
			if (!curValue.equals(lastValue) && iteNum != startRepeatIte+2) {
				point.setVerValue(curValue);
				aggregators[curValue.getTag()].add(point.getDimensions(), 1);
				aggregators[lastValue.getTag()].decrease(point.getDimensions(), 1); //减去作用在JobInProgress里体现
				context.setVertexAgg((curValue.getDistance()-lastValue.getDistance()));
			}
			
		}
		
		
		
//		if (!point.isFirstCompute()) {
//			if(context.getSuperstepCounter() == 3) {//ite3 只做对比， 不计入
//				context.setVertexAgg(curValue.getDistance());
//			}
//			if (!curValue.equals(lastValue) && context.getSuperstepCounter() != 3) {
//					point.setVerValue(curValue);
//					aggregators[curValue.getTag()].add(point.getDimensions(), 1);
//					aggregators[lastValue.getTag()].decrease(point.getDimensions(), 1); //减去作用在JobInProgress里体现
//					context.setVertexAgg((curValue.getDistance()-lastValue.getDistance()));
//			}
//
//		} else {
//			point.setFirstCompute(false);
//			point.setVerValue(curValue);
//	
//			aggregators[curValue.getTag()].add(point.getDimensions(), 1);
//			context.setVertexAgg(curValue.getDistance());
//		}
	}
	
	private double computeDistance(int dim, double[] point, double[] center) {
		double sum = 0.0, dist = 0.0;
		for (int i = 0; i < dim; i++) {
			dist = Math.abs(point[i]-center[i]);
			sum += dist * dist;
		}
		
		return Math.sqrt(sum);
	}
}