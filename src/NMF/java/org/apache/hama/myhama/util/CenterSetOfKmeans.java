package org.apache.hama.myhama.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hama.myhama.api.GraphRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;

public class CenterSetOfKmeans implements Writable {
	private GraphRecord[] centroids;
	
	private GraphRecord[] sigmas; //used in GMM
	private double[] weights; //used in GMM
	private double[] theta; //used in sgd
	private double[][] matrixH; // used in NMF
	
	private Log LOG = LogFactory.getLog(CenterSetOfKmeans.class);
	
	public CenterSetOfKmeans() {
		
	}
	
	public void set(GraphRecord[] _centroids, GraphRecord[] _sigmas, double[] _weights) {
		this.centroids = _centroids;
		this.sigmas = _sigmas;
		this.weights = _weights;
	}
	public void set(double[] _theta) {
		this.theta = _theta;
	}
	
	public GraphRecord[] getCentroids() {
		return this.centroids;
	}
	
	public GraphRecord[] getSigmas() {
		return this.sigmas;
	}
	
	public double[] getWeights() {
		return this.weights;
	}
	
	public double[] getTheta() {
		return this.theta;
	}
	
	/**
	 * @param matrixH
	 */
	public void setMatrixH(double[][] matrixH) {
		this.matrixH = matrixH;
	}
	
	/**
	 * @return
	 */
	public double[][] getMatrixH() {
		return matrixH;
	}
	
	public void update(int centerIdx, double[] centroid, double[] sigma, double weight) {
		 this.centroids[centerIdx].setDimensions(centroid);
		 if (this.sigmas != null) {
			 this.sigmas[centerIdx].setDimensions(sigma);
		 }
		 if (this.weights != null) {
			 this.weights[centerIdx] = weight;
		 }
	}

	public void update(int centerIdx, double gradient, float rate, int allComputePoint) {

		 if (this.theta != null) {
			 this.theta[centerIdx] = this.theta[centerIdx] - rate*(gradient/allComputePoint);
		 }
	}
	
	/**
	 * 更新矩阵 H
	 */
	public void updateMatrixH(double[][] sumOfUpdateH) {
		if (this.matrixH != null) {
			int hr = this.matrixH.length;
			int hc = this.matrixH[0].length;
			
			for (int i = 0; i < hr; i++) {
				for (int j = 0; j < hc; j++) {
					this.matrixH[i][j] += sumOfUpdateH[i][j];
				}
			}
		}
	}
	
	public int getNumOfCentroids() {
		return (this.centroids==null? 0:this.centroids.length);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		int count = in.readInt();
		this.centroids = new GraphRecord[count];
		for (int i = 0; i < count; i++) {
			this.centroids[i] = new GraphRecord();
			this.centroids[i].readFields(in);
		}
		
		count = in.readInt();
		if (count == 0) {
			this.sigmas = null;
		} else {
			this.sigmas = new GraphRecord[count];
			for (int i = 0; i < count; i++) {
				this.sigmas[i] = new GraphRecord();
				this.sigmas[i].readFields(in);
			}
		}
		
		count = in.readInt();
		if (count == 0) {
			this.weights = null;
		} else {
			this.weights = new double[count];
			for (int i = 0; i < count; i++) {
				this.weights[i] = in.readDouble();
			}
		}
		
		count = in.readInt();
		if (count == 0) {
			this.theta = null;
		} else {
			this.theta = new double[count];
			for (int i = 0; i < count; i++) {
				this.theta[i] = in.readDouble();
			}
		}
		
		count = in.readInt();
		if (count == 0) {
			this.matrixH = null;
		} else {
			this.matrixH = new double[6][count];
			for (int i = 0; i < 6; i++) {
				for (int j = 0; j < count; j++) {
					this.matrixH[i][j] = in.readDouble();
				}
			}
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		int count = getNumOfCentroids();
		out.writeInt(count);
		for (int i = 0; i < count; i++) {
			this.centroids[i].write(out);
		}
		
		if (this.sigmas == null) {
			out.writeInt(0);
		} else {
			out.writeInt(this.sigmas.length);
			for (int i = 0; i < this.sigmas.length; i++) {
				this.sigmas[i].write(out);
			}
		}
		
		if (this.weights == null) {
			out.writeInt(0);
		} else {
			out.writeInt(this.weights.length);
			for (int i = 0; i < this.weights.length; i++) {
				out.writeDouble(this.weights[i]);
			}
		}
		
		if (this.theta == null) {
			out.writeInt(0);
		} else {
			out.writeInt(this.theta.length);
			for (int i = 0; i < this.theta.length; i++) {
				out.writeDouble(this.theta[i]);
			}
		}
		
		if (this.matrixH == null) {
			out.writeInt(0);
		} else {
			out.writeInt(this.matrixH[0].length);
			for (int i = 0; i < 6; i++) {
				for (int j = 0; j < this.matrixH[0].length; j++) {
					out.writeDouble(this.matrixH[i][j]);
				}
			}
		}
	}
	
	public String printCentroids() {
		StringBuffer sb = new StringBuffer("centroids:");
		if (this.centroids != null) {
			for (GraphRecord g: this.centroids) {
				sb.append("\n");
				sb.append(g);
			}
		} else {
			sb.append("null");
		}
		
		return sb.toString();
	}
}
