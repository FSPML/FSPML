package org.apache.hama.myhama.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class DeltaParaOfCNN implements Writable{
	private double[][][][] deltaKernels = null;
	private double[] deltaBias = null;

	public double[][][][] getDeltaKernels() {
		return deltaKernels;
	}

	public void setDeltaKernels(double[][][][] deltaKernels) {
		this.deltaKernels = deltaKernels;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		int count = in.readInt();
		if (count == 0) {
			this.deltaKernels = null;
		} else {
			int i = in.readInt();
			int j = in.readInt();
			int m = in.readInt();
			int n = in.readInt();
			this.deltaKernels = new double[i][j][m][n];
			for(int x=0; x<i; x++) {
				for(int y=0; y<j; y++) {
					for(int z=0; z<m; z++) {
						for(int w=0; w<n; w++) {
							deltaKernels[x][y][z][w] = in.readDouble();
						}
					}
				}
			}
		}
		count = in.readInt();
		if (count == 0) {
			this.deltaBias = null;
		} else {
			this.deltaBias = new double[count];
			for (int i = 0; i < count; i++) {
				this.deltaBias[i] = in.readDouble();
			}
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		
		if (this.deltaKernels == null) {
			out.writeInt(0);
		} else {
			out.writeInt(1);
			int i = deltaKernels.length;
			int j = deltaKernels[0].length;
			int m = deltaKernels[0][0].length;
			int n = deltaKernels[0][0][0].length;
			out.writeInt(i);
			out.writeInt(j);
			out.writeInt(m);
			out.writeInt(n);
			for(int x=0; x<i; x++) {
				for(int y=0; y<j; y++) {
					for(int z=0; z<m; z++) {
						for(int w=0; w<n; w++) {
							out.writeDouble(deltaKernels[x][y][z][w]);
						}
					}
				}
			}
		}
		if (this.deltaBias == null) {
			out.writeInt(0);
		} else {
			int i = deltaBias.length;
			out.writeInt(i);
			for(int j=0; j<i; j++) {
				out.writeDouble(deltaBias[j]);
			}
		}
		
	}

	public void setUpdataKernel(int i, int j, double[][] deltaKernel) {
		// TODO Auto-generated method stub
		this.deltaKernels[i][j] = deltaKernel;
	}

	public void setDeltaBias(double[] ds) {
		// TODO Auto-generated method stub
		this.deltaBias = ds;
	}

	public void setUpdataBia(int j, double deltaBias2) {
		// TODO Auto-generated method stub
		this.deltaBias[j] = deltaBias2;
	}

	public double[] getDeltaBias() {
		return deltaBias;
	}


}
