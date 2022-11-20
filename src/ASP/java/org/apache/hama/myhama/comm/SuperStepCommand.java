package org.apache.hama.myhama.comm;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hama.Constants.CommandType;
import org.apache.hama.myhama.util.CenterSetOfKmeans;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class SuperStepCommand implements Writable {
	private CommandType commandType;
	private double jobAgg;
	
	private CenterSetOfKmeans csk;
	private int intervalOfSyn;
	private int secStartIte = 0;
	private int[] stragglerInfo;
	


	public SuperStepCommand() {
		
	}
	
	public void setGlobalParameters(CenterSetOfKmeans _centroids) {
		this.csk = _centroids;
	}
	
	public CenterSetOfKmeans getGlobalParameters() {
		return this.csk;
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
		//this.jobAgg = in.readDouble();
		
		this.csk = new CenterSetOfKmeans();
		this.csk.readFields(in);
		this.intervalOfSyn = in.readInt();
		this.secStartIte = in.readInt();
		
		int count = in.readInt();
		if (count == 0) {
			this.stragglerInfo = null;
		}else {
			this.stragglerInfo = new int[count];
			for (int i = 0; i < stragglerInfo.length; i++) {
				stragglerInfo[i] = in.readInt();
			}
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeEnum(out, this.commandType);
		//out.writeDouble(this.jobAgg);
		
		this.csk.write(out);
		out.writeInt(intervalOfSyn);
		out.writeInt(this.secStartIte);
		
		if (stragglerInfo == null) {
			out.writeInt(0);
		}else {
			out.writeInt(stragglerInfo.length);
			for (int i = 0; i < stragglerInfo.length; i++) {
				out.writeInt(stragglerInfo[i]);
			}
		}
	}
	
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("command="); sb.append(this.commandType);
		sb.append("\tsum-agg="); sb.append(this.jobAgg);
		return sb.toString();
	}
	public void setNextIntervalOfSync(int intervalOfSync) {
		this.intervalOfSyn = intervalOfSync;
	}
	public int getIntervalOfSyn() {
		return intervalOfSyn;
	}

	public void setSceAdjustIntIte(int secStartIte) {
		this.secStartIte  = secStartIte;
		
	}
	
	public int getSecStartIte() {
		return secStartIte;
	}
	
	public void setStragglersInfo(int[] whoStragglers) {
		this.stragglerInfo = whoStragglers;
	}

	public int[] getStragglerInfo() {
		return stragglerInfo;
	}
}
