/**
 * copyright 2011-2016
 */
package hybridgraph.examples.gmm.distributed;

//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
import hybridgraph.examples.fcm.distributed.FCMValue;
import org.apache.hama.myhama.api.BSP;
import org.apache.hama.myhama.api.GraphRecord;
import org.apache.hama.myhama.util.AggregatorOfKmeans;
import org.apache.hama.myhama.util.Context;

import blog.distrib.MultivarGaussian;
import Jama.Matrix;

/**
 * GMMBSP.java implements {@link BSP}.
 * 
 * GMM implementation.
 * 
 * @author 
 * @version 0.1
 */
public class GMMBSP extends BSP<GMMValue, Integer, Integer, Integer> {
	//private static final Log LOG = LogFactory.getLog(GMMBSP.class);

	private int k, dim;
	
	private Matrix[] p_centroids;
	private Matrix[] p_sigmas;
	private double[] p_weights;
	
	private MultivarGaussian[] mgs;

	@Override
	public void getLocalData(GraphRecord<GMMValue, Integer, Integer, Integer>[][] localPoints) {
		// do nothing
	}

	@Override
	public void taskSetup(Context<GMMValue, Integer, Integer, Integer> context) {
		k = context.getBSPJobInfo().getNumOfCenters();
		dim = context.getBSPJobInfo().getNumOfDimensions();
		
		p_centroids = new Matrix[k];
		p_sigmas = new Matrix[k];
		p_weights = new double[k];
		mgs = new MultivarGaussian[k];
	}
	
	@Override
	public void superstepSetup(Context<GMMValue, Integer, Integer, Integer> context) {
		GraphRecord<GMMValue, Integer, Integer, Integer>[] org_centriods = context.getCentroids();
		GraphRecord<GMMValue, Integer, Integer, Integer>[] org_sigmas = context.getSigmas();

		for (int i = 0; i < k; i++) {
			p_centroids[i] = GMMUtil.parseRowMatrix(org_centriods[i]);
			p_sigmas[i] = GMMUtil.parseDiagMatrix(org_sigmas[i]);
			
			mgs[i] = new MultivarGaussian(p_centroids[i], p_sigmas[i]);;
		}
		p_weights = context.getWeights();
	}
	
	@Override
	public void update(
			Context<GMMValue, Integer, Integer, Integer> context) 
				throws Exception {
		GraphRecord<GMMValue, Integer, Integer, Integer> point = 
			context.getGraphRecord();
		AggregatorOfKmeans[] aggregators = context.getAggregators();
		
		double[] lastErrVals = point.getVerValue().getErrors();
		double[] curErrVals = new double[k];
		double lastErrSum = 0.0, curErrSum = 0.0;
		for(int j=0; j<k; j++) {
			curErrVals[j] = 
				p_weights[j] * mgs[j].getProb(GMMUtil.parseRowMatrix(point));
			//curErrVals[j] = Double.isNaN(curErrVals[j])? 0:curErrVals[j];
			
			curErrSum += curErrVals[j];
			
			lastErrSum += lastErrVals[j];
		}
		
		if (curErrSum == 0.0) {
			//LOG.info(point.getVerId() + ", cur-sum=" + curErrSum + ", log-sum=" + Math.log(curErrSum));
			return;
		}

		if (context.getSuperstepCounter()==1 || lastErrSum==0.0) {
			context.setVertexAgg(Math.log(curErrSum));
		} else {
			context.setVertexAgg((Math.log(curErrSum)-Math.log(lastErrSum)));
		}
		
		double[] lastRelations = point.getVerValue().getRelations();
		double[] curRelations = new double[k];
		for(int j=0; j<k; j++) {
			double r = curErrVals[j]/curErrSum;
			double[] aggX = new double[dim];
			double[] aggS = new double[dim];
			if (context.getSuperstepCounter() == 1) {
				for(int d=0; d<dim; d++) {
					double value = point.getDimensions()[d];
					aggX[d] = r*value;
					aggS[d] = r*value*value;
				}
				aggregators[j].add(aggX, r);
				aggregators[j].addAggS(aggS);
			} else {
				for(int d=0; d<dim; d++) {
					double value = point.getDimensions()[d];
					aggX[d] = (r-lastRelations[j])*value;
					aggS[d] = (r-lastRelations[j])*value*value;
				}
				aggregators[j].add(aggX, (r-lastRelations[j]));
				aggregators[j].addAggS(aggS);
			}
			curRelations[j] = r;
		}
		
		point.getVerValue().set(curErrVals, curRelations);
	}

	@Override
	public void updateOnGpu(Context<GMMValue, Integer, Integer, Integer> context) throws Exception {

	}
}