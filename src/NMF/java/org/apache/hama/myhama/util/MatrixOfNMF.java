package org.apache.hama.myhama.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;
import org.apache.hama.bsp.BSPTask;

public class MatrixOfNMF implements Writable {
	
	private int column; // 列数
	private double matrixUpdateH[][];
	private int part;
	private int idx;
	private int len;
	
	private static final Log LOG = LogFactory.getLog(MatrixOfNMF.class);
	
	public MatrixOfNMF() {}
	
	public MatrixOfNMF(int column) {
		this.column = column;
		this.matrixUpdateH = new double[6][column];
		this.part = 0;
		this.idx = 0;
		this.len = 0;
	}
	
	public double[][] getMatrixUpdateH() {
		return this.matrixUpdateH;
	}
	
	public void setMatrixUpdateH(double[][] matrixUpdateH) {
		for (int i = 0; i < this.matrixUpdateH.length; i++) {
			for (int j = 0; j < this.matrixUpdateH[0].length; j++) {
				this.matrixUpdateH[i][j] += matrixUpdateH[i][j];
			}
		}
	}
	
	public void setParam(int part, int idx, int len) {
		this.part = part;
		this.idx = idx;
		this.len = len;
	}
	
	public int getPart() {
		return part;
	}

	public int getIdx() {
		return idx;
	}

	public int getLen() {
		return len;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		int count = in.readInt();
		this.matrixUpdateH = new double[6][count];
		
		for (int i = 0; i < 6; i++) {
			for (int j = 0; j < count; j++) {
				this.matrixUpdateH[i][j] = in.readDouble();
			}
		}
		
		this.part = in.readInt();
		this.idx = in.readInt();
		this.len = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.column);
		
		for (int i = 0; i < 6; i++) {
			for (int j = 0; j < this.matrixUpdateH[0].length; j++) {
				out.writeDouble(this.matrixUpdateH[i][j]);
			}
		}
		
		out.writeInt(this.part);
		out.writeInt(this.idx);
		out.writeInt(this.len);
	}
	
	public void reset() {
		if (this.matrixUpdateH != null) {
			for (int i = 0; i < this.matrixUpdateH.length; i++) {
				for (int j = 0; j < this.matrixUpdateH[0].length; j++) {
					this.matrixUpdateH[i][j] = 0.0;
				}
			}
		}
		this.part = 0;
		this.idx = 0;
		this.len = 0;
	}
	
}
