package org.apache.hama.myhama.util;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;

/**
 * Including a m-dimension array to keep the sum regarding each dimension, 
 * and the number of points which fall into this center.
 * @author root
 *
 */
public class AggregatorOfKmeans implements Writable {
	private static final Log LOG = LogFactory.getLog(AggregatorOfKmeans.class);
	
	private double numOfPoints;
	private double[] sumOfDists;
	
	private double[] agg_S;
	
	public AggregatorOfKmeans() {
		this.numOfPoints = 0.0;
		this.sumOfDists = null;
		
		this.agg_S = null;
	}
	
	public AggregatorOfKmeans(int _numOfDimensions, boolean useAggS) {
		this.numOfPoints = 0.0;
		
		this.sumOfDists = new double[_numOfDimensions];
		if (useAggS) {
			this.agg_S = new double[_numOfDimensions];
		} else {
			this.agg_S = null;
		}
	}
	
	/**
	 * Accumulate dimension values and point number. 
	 * The accumulated values may be negative.
	 * @param _sums Also agg_X in GMM
	 * @param _num Also agg_R in GMM
	 */
	public void add(double[] _sums, double _num) {
		for (int i = 0; i < this.sumOfDists.length; i++) {
			this.sumOfDists[i] += _sums[i];
		}
		this.numOfPoints += _num;
	}
	
	/**
	 * Only used in GMM for accumulating values in agg_S.
	 * @param _sums
	 */
	public void addAggS(double[] _sums) {
		for (int i = 0; i < this.agg_S.length; i++) {
			this.agg_S[i] += _sums[i];
		}
	}
	
	/**
	 * Remove a point from a given center.
	 * The dimension values and _num is positive. 
	 * This is only used by user's {@link BSPInterface}.update() 
	 * when performing incremental update for aggregators.
	 * 
	 * @param _sums Also agg_X in GMM
	 * @param _num Also agg_R in GMM
	 */
	public void decrease(double[] _sums, double _num) {
		for (int i = 0; i < this.sumOfDists.length; i++) {
			this.sumOfDists[i] -= _sums[i];
		}
		this.numOfPoints -= _num;
	}
	
	public void decreaseAggS(double[] _sums) {
		for (int i = 0; i < this.agg_S.length; i++) {
			this.agg_S[i] -= _sums[i];
		}
	}
	
	/**
	 * Also agg_X in GMM
	 * @return
	 */
	public double[] getSumOfDists() {
		return this.sumOfDists;
	}
	
	/**
	 * Also agg_R in GMM
	 * @return
	 */
	public double getNumOfPoints() {
		return this.numOfPoints;
	}
	
	public double[] getAggS() {
		return this.agg_S;
	}
	
	public void reset() {
		this.numOfPoints = 0.0;
		
		if (this.sumOfDists != null) {
			Arrays.fill(this.sumOfDists, 0.0);
		}
		if (this.agg_S != null) {
			Arrays.fill(this.agg_S, 0.0);
		}
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		int numOfDimensions = in.readInt();
		this.sumOfDists = new double[numOfDimensions];
		for (int i = 0; i < numOfDimensions; i++) {
			this.sumOfDists[i] = in.readDouble();
		}
		
		int numOfaggS = in.readInt();
		//LOG.info("wzg-numOfaggS=" + numOfaggS);
		if (numOfaggS == 0) {
			this.agg_S = null;
		} else {
			this.agg_S = new double[numOfaggS];
			for (int i = 0; i < numOfaggS; i++) {
				this.agg_S[i] = in.readDouble();
			}
		}
		
		this.numOfPoints = in.readDouble();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.sumOfDists.length);
		for (int i = 0; i < this.sumOfDists.length; i++) {
			out.writeDouble(this.sumOfDists[i]);
		}
		
		if (this.agg_S == null) {
			out.writeInt(0);
		} else {
			out.writeInt(this.agg_S.length);
			for (int i = 0; i < this.agg_S.length; i++) {
				out.writeDouble(this.agg_S[i]);
			}
		}
		
		out.writeDouble(this.numOfPoints);
	}
	
	public void readBytes(DataInputStream in) throws IOException {
		int numOfDimensions = in.readInt();
		this.sumOfDists = new double[numOfDimensions];
		for (int i = 0; i < numOfDimensions; i++) {
			this.sumOfDists[i] = in.readDouble();
		}
		
		int numOfaggS = in.readInt();
		if (numOfaggS == 0) {
			this.agg_S = null;
		} else {
			this.agg_S = new double[numOfaggS];
			for (int i = 0; i < numOfaggS; i++) {
				this.agg_S[i] = in.readDouble();
			}
		}
		
		this.numOfPoints = in.readDouble();
	}
	
	public void writeBytes(DataOutputStream out) throws IOException {
		out.writeInt(this.sumOfDists.length);
		for (int i = 0; i < this.sumOfDists.length; i++) {
			out.writeDouble(this.sumOfDists[i]);
		}
		
		if (this.agg_S == null) {
			out.writeInt(0);
		} else {
			out.write(this.agg_S.length);
			for (int i = 0; i < this.agg_S.length; i++) {
				out.writeDouble(this.agg_S[i]);
			}
		}
		
		out.writeDouble(this.numOfPoints);
	}
}
