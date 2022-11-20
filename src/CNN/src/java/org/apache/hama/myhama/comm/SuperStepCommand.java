package org.apache.hama.myhama.comm;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hama.Constants.CommandType;
import org.apache.hama.myhama.util.AggregatorSetOfKmeans;
import org.apache.hama.myhama.util.CenterSetOfKmeans;
import org.apache.hama.myhama.util.ParameterTransfer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class SuperStepCommand implements Writable {
	private CommandType commandType;
	private double jobAgg;
	private int blockNum;
	
	private CenterSetOfKmeans csk;
	private ParameterTransfer parameterTransfer;
	
	public SuperStepCommand() {
		
	}
	
	public void setGlobalParameters(CenterSetOfKmeans _centroids) {
		this.csk = _centroids;
	}
	
	public CenterSetOfKmeans getGlobalParameters() {
		return this.csk;
	}
	
	public void setBlockNum(int _size) {
		this.blockNum = _size;
	}
	
	public int getBlockNum() {
		return this.blockNum;
	}
	
	public CommandType getCommandType() {
		return this.commandType;
	}
	
	public void setCommandType(CommandType commandType) {
		this.commandType = commandType;
	}

	public double getJobAgg() {
		return this.jobAgg;
	}
	
	public void setJobAgg(double jobAgg) {
		this.jobAgg = jobAgg;
	}
	
	public int getNumOfCentroids() {
		return this.csk.getNumOfCentroids();
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		this.commandType = WritableUtils.readEnum(in, CommandType.class);
		
		if (in.readInt() == 0) {
			this.parameterTransfer = null;
		}else {
			this.parameterTransfer = new ParameterTransfer();
			this.parameterTransfer.readFields(in);
		}

	}

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeEnum(out, this.commandType);
		
		if (this.parameterTransfer == null) {
			out.writeInt(0);
		}else {
			out.writeInt(1);
			this.parameterTransfer.write(out);
		}
	}
	
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("command="); sb.append(this.commandType);
		sb.append("\tsum-agg="); sb.append(this.jobAgg);
		sb.append("\tblock=" + this.blockNum);
		return sb.toString();
	}

	public void setGlobalCNNParameters(ParameterTransfer parameterTransfer) {
		this.parameterTransfer = parameterTransfer;
	}
	public ParameterTransfer getGlobalCNNParameters() {
		return this.parameterTransfer;
	}
	
}
