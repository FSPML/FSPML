package org.apache.hama.myhama.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hama.myhama.api.GraphRecord;

import org.apache.hadoop.io.Writable;

public class CenterSetOfKmeans implements Writable {
	private GraphRecord[] centroids;
	
	private GraphRecord[] sigmas; //used in GMM
	private double[] weights; //used in GMM
	
	public CenterSetOfKmeans() {
		
	}
	
	public void set(GraphRecord[] _centroids, GraphRecord[] _sigmas, double[] _weights) {
		this.centroids = _centroids;
		this.sigmas = _sigmas;
		this.weights = _weights;
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
	
	public void update(int centerIdx, double[] centroid, double[] sigma, double weight) {
		 this.centroids[centerIdx].setDimensions(centroid);
		 if (this.sigmas != null) {
			 this.sigmas[centerIdx].setDimensions(sigma);
		 }
		 if (this.weights != null) {
			 this.weights[centerIdx] = weight;
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
