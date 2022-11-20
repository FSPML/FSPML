/**
 * Termite System
 * copyright 2012-2010
 */
package org.apache.hama.myhama.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 * Counters.
 * This class contains relative {@link COUNTER} counters. 
 * 
 * @author 
 * @version 0.1
 */
public class Counters implements Writable {
	
	public static enum COUNTER {
		/** counters of vertices */
		Vert_Read, //number of vertices loaded from local disk/memory
		
		/** counters of runtime */
		Time_Ite,  //runtime of one whole iteration
	}
	
	public static int SIZE = COUNTER.values().length;
	
	private long[] counters;
	
	public Counters() {
		this.counters = new long[SIZE];
		this.clearValues();
	}
	
	/**
	 * Add the value of one counter and return the new summary.
	 * If the counter does not exist, throw an Exception.
	 * 
	 * @param name Enum<?> {@link COUNTER}
	 * @param value long
	 */
	public void addCounter(Enum<?> name, long value) {
		this.counters[name.ordinal()] += value;
	}
	
	/**
	 * Add all values of a given {@link Counters} other to the current one.
	 * @param other
	 */
	public void addCounters(Counters other) {
		for (Enum<?> name: COUNTER.values()) {
			this.counters[name.ordinal()] += other.getCounter(name);
		}
	}
	
	/**
	 * Return the value of a counter.
	 * 
	 * @param name Enum<?> {@link COUNTER}
	 * @return value long
	 */
	public long getCounter(Enum<?> name) {
		return this.counters[name.ordinal()];
	}

	/**
	 * Clear values of all {@link COUNTER}s.
	 * That means the value is set to zero.
	 */
	public void clearValues() {
		Arrays.fill(this.counters, 0L);
	}
	
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer("Counters:");
		for (Enum<?> name: COUNTER.values()) {
			sb.append("\n");
			sb.append(name); sb.append("=");
			sb.append(this.counters[name.ordinal()]);
		}
		
		return sb.toString();
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		for (int i = 0; i < SIZE; i++) {
			COUNTER name = WritableUtils.readEnum(in, COUNTER.class);
			this.counters[name.ordinal()] = in.readLong();
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		for (Enum<?> name: COUNTER.values()) {
			WritableUtils.writeEnum(out, name);
			out.writeLong(this.counters[name.ordinal()]);
		}
	}
}
