package org.apache.hama.myhama.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hama.myhama.util.AggregatorOfKmeans;

public class AggregatorSetOfKmeans implements Writable {
	/** local & remote variable recording the aggregated value */
	private AggregatorOfKmeans[] aggregators;
	
	public AggregatorSetOfKmeans() {
		
	}
	
	public AggregatorSetOfKmeans(int numOfCentroids, int numOfDimensions, boolean isGMM) {
		this.aggregators = new AggregatorOfKmeans[numOfCentroids];
		for (int i = 0; i < numOfCentroids; i++) {
			this.aggregators[i] = new AggregatorOfKmeans(numOfDimensions, isGMM);
		}
	}
	
	public AggregatorOfKmeans[] getAggregators() {
		return this.aggregators;
	}
	
	public void reset() {
		for (AggregatorOfKmeans aggregator: this.aggregators) {
			aggregator.reset();
		}
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		int numOfAggregators = in.readInt();
		this.aggregators = new AggregatorOfKmeans[numOfAggregators];
		for (int i = 0; i < numOfAggregators; i++) {
			this.aggregators[i] = new AggregatorOfKmeans();
			this.aggregators[i].readFields(in);
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		int numOfAggregators = this.aggregators==null? 0: this.aggregators.length;
		
		out.writeInt(numOfAggregators);
		for (int i = 0; i < this.aggregators.length; i++) {
			this.aggregators[i].write(out);
		}
	}
}
