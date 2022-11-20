package org.apache.hama.myhama.comm;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hama.myhama.util.AggregatorSetOfKmeans;
import org.apache.hama.myhama.util.Counters;
import org.apache.hama.myhama.util.GradientOfSGD;

public class SuperStepReport implements Writable {
	private Counters counters;
	private double taskAgg;
	
	private AggregatorSetOfKmeans aggregators;
	private GradientOfSGD gradAgg;
	private int hasPro = 0;
	private int dataProCounters = 0;
	
	
	
	public GradientOfSGD getGradientAgg() {
		return gradAgg;
	}
	public void setGradientAgg(GradientOfSGD gradSet) {
		this.gradAgg = gradSet;
	}
	
	
	public SuperStepReport() {
		this.counters = new Counters();
	}
	
	public void setCounters(Counters counters) {
		this.counters = counters;
	}
	
	public Counters getCounters() {
		return this.counters;
	}
	
	public double getTaskAgg() {
		return this.taskAgg;
	}
	
	public void setTaskAgg(double taskAgg) {
		this.taskAgg = taskAgg;
	}
	
	public void setAggregatorSet(AggregatorSetOfKmeans _aggregators) {
		this.aggregators = _aggregators;
	}
	
	public AggregatorSetOfKmeans getAggregatorSet() {
		return this.aggregators;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		//this.counters.readFields(in);
		this.taskAgg = in.readDouble();
		if (in.readInt() == 0) {
			this.aggregators = null;
		}else {
			this.aggregators = new AggregatorSetOfKmeans();
			this.aggregators.readFields(in);
		}
		if(in.readInt() ==0) {
			this.gradAgg = null;
		}else {
			this.gradAgg = new GradientOfSGD();
			this.gradAgg.readFields(in);
		}
		this.hasPro = in.readInt();
		
		this.dataProCounters = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		//this.counters.write(out);
		out.writeDouble(this.taskAgg);
		
		if (this.aggregators == null) {
			out.writeInt(0);
		}else {
			out.writeInt(1);
			this.aggregators.write(out);
		}
		if (this.gradAgg == null) {
			out.writeInt(0);
		}else {
			out.writeInt(1);
			this.gradAgg.write(out);
		}
		out.writeInt(hasPro);
		out.writeInt(this.dataProCounters);
	}
	
	@Override
	public String toString() {
		return "[TaskAgg] = " + this.taskAgg 
			+ "\n" + this.counters.toString();
	}
	public void setHasPro(int hasPro) {
		this.hasPro = hasPro;
		
	}
	public int getHasPro() {
		return this.hasPro;
	}
	public void setdataProCounters(int dataProCounters) {
		this.dataProCounters = dataProCounters;
	}
	public int getDataProCounters() {
		return dataProCounters;
	}
	
	//use report second superstep haspro
	public void setSecHaspro(int hasPro) {
		this.hasPro = hasPro;
	}
	
	//use get second superstep haspro
	public int getSecHaspro() {
		int num = this.hasPro;
		this.hasPro = 0; 
		return num;
	}


}
