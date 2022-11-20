package org.apache.hama.myhama.comm;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hama.myhama.util.AggregatorSetOfKmeans;
import org.apache.hama.myhama.util.Counters;

public class SuperStepReport implements Writable {
	private Counters counters;
	private double taskAgg;
	
	private AggregatorSetOfKmeans aggregators;
	
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
		
		this.aggregators = new AggregatorSetOfKmeans();
		this.aggregators.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		//this.counters.write(out);
		out.writeDouble(this.taskAgg);
		
		this.aggregators.write(out);
	}
	
	@Override
	public String toString() {
		return "[TaskAgg] = " + this.taskAgg 
			+ "\n" + this.counters.toString();
	}
}
