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

public class GradientOfSGD implements Writable{
	private static final Log LOG = LogFactory.getLog(GradientOfSGD.class);

	private double[] gradient;//just use sgd
	private int oneItePointNum;
	private int gradientNum;

	public GradientOfSGD() {
		
	}

	
	public GradientOfSGD(int gradientNum) {
		this.gradientNum = gradientNum;  //dim -1
		this.gradient = new double[this.gradientNum];
		this.oneItePointNum = 0;
	}


	public double[] getGradient() {
		return gradient;
	}
	public void setGradient(double[] gradient) {
		for (int i = 0; i < gradient.length; i++) {
			this.gradient[i] += gradient[i];
		}
		this.oneItePointNum += 1;
	}
	
	public int getOneItePointNum() {
		int giveNum = this.oneItePointNum;
		this.oneItePointNum = 0;
		return giveNum;
	}


	@Override
	public void readFields(DataInput in) throws IOException {
		int numOfgradient = in.readInt();

		this.gradient = new double[numOfgradient];
		for (int i = 0; i < numOfgradient; i++) {
			this.gradient[i] = in.readDouble();
		}
		this.oneItePointNum = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.gradientNum);

		for (int i = 0; i < this.gradient.length; i++) {
			out.writeDouble(this.gradient[i]);
		}
		out.writeInt(this.oneItePointNum);
	}
	
	public void readBytes(DataInputStream in) throws IOException {
		int numOfgradient = in.readInt();
		this.gradient = new double[numOfgradient];
		for (int i = 0; i < numOfgradient; i++) {
			this.gradient[i] = in.readDouble();
		}

	}
	
	public void writeBytes(DataOutputStream out) throws IOException {
		out.writeInt(this.gradientNum);
		for (int i = 0; i < this.gradient.length; i++) {
			out.writeDouble(this.gradient[i]);
		}
	}


	public void reset() {
		if (this.gradient != null) {
			Arrays.fill(this.gradient, 0.0);
		}
		this.oneItePointNum = 0;
	}



}
