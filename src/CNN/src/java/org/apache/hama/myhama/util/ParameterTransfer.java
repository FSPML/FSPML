package org.apache.hama.myhama.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;

public class ParameterTransfer implements Writable{
	private static final Log LOG = LogFactory.getLog(ParameterTransfer.class);
	private double[][][][] kernel = null;// 卷积核，只有卷积层和输出层有
	private double[] bias = null;// 每个map对应一个偏置，只有卷积层和输出层有
	// 保存各个batch的输出map，outmaps[0][0]表示第一条记录训练下第0个输出map
	private double[][][][] outmaps;
	private int allHasPro;

	@Override
	public void readFields(DataInput in) throws IOException {
		int count;
		count = in.readInt();
		if (count == 0) {
			this.kernel = null;
		} else {
			int i = in.readInt();
			int j = in.readInt();
			int m = in.readInt();
			int n = in.readInt();
			LOG.info("i = " + i + " j= " + j + " m= " + m + " n= " + n + "=================");
			this.kernel = new double[i][j][m][n];
			for(int x=0; x<i; x++) {
				for(int y=0; y<j; y++) {
					for(int z=0; z<m; z++) {
						for(int w=0; w<n; w++) {
							kernel[x][y][z][w] = in.readDouble();
						}
					}
				}
			}
		}
		
		count = in.readInt();
		if (count == 0) {
			this.bias = null;
		} else {
			this.bias = new double[count];
			for (int i = 0; i < count; i++) {
				this.bias[i] = in.readDouble();
			}
		}
		this.allHasPro = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		if (this.kernel == null) {
			out.writeInt(0);
		} else {
			out.writeInt(1);
			int i = kernel.length;
			int j = kernel[0].length;
			int m = kernel[0][0].length;
			int n = kernel[0][0][0].length;
			out.writeInt(i);
			out.writeInt(j);
			out.writeInt(m);
			out.writeInt(n);
			LOG.info("i = " + i + " j= " + j + " m= " + m + " n= " + n + "=================");
			for(int x=0; x<i; x++) {
				for(int y=0; y<j; y++) {
					for(int z=0; z<m; z++) {
						for(int w=0; w<n; w++) {
							out.writeDouble(kernel[x][y][z][w]);
						}
					}
				}
			}
		}
		
		if (this.bias == null) {
			out.writeInt(0);
		} else {
			int i = bias.length;
			out.writeInt(i);
			for(int j=0; j<i; j++) {
				out.writeDouble(bias[j]);
			}
		}
		out.writeInt(allHasPro);
	}

	public double[][][][] getKernel() {
		return kernel;
	}

	public void setKernel(double[][][][] kernel) {
		this.kernel = kernel;
	}

	public double[] getBias() {
		return bias;
	}

	public void setBias(double[] bias) {
		this.bias = bias;
	}

	public double[][][][] getOutmaps() {
		return outmaps;
	}

	public void setOutmaps(double[][][][] outmaps) {
		this.outmaps = outmaps;
	}

	public void setPara(double[][][][] kernel, double[] bias) {
		// TODO Auto-generated method stub
		this.kernel = kernel;
		this.bias = bias;
	}

	public void setAllHasPro(int haspro) {
		// TODO Auto-generated method stub
		this.allHasPro = haspro;
	}

	public int getAllHasPro() {
		return allHasPro;
	}

}
