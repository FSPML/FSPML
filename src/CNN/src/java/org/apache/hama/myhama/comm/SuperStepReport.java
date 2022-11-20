package org.apache.hama.myhama.comm;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hama.myhama.util.AggregatorSetOfKmeans;
import org.apache.hama.myhama.util.Counters;
import org.apache.hama.myhama.util.DeltaParaOfCNN;
import org.apache.hama.myhama.util.GradientOfSGD;

public class SuperStepReport implements Writable {
	private Counters counters;
	private double taskAgg;
	
	private AggregatorSetOfKmeans aggregators;


	private GradientOfSGD gradAgg;
	
	//cnn
	private DeltaParaOfCNN deltaPara;
	private int haspro;
	
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
		
		if(in.readInt() ==0) {
			this.deltaPara = null;
		}else {
			this.deltaPara = new DeltaParaOfCNN();
			this.deltaPara.readFields(in);
		}
		this.haspro = in.readInt();
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
		
		if (this.deltaPara == null) {
			out.writeInt(0);
		} else {
			out.writeInt(1);
			this.deltaPara.write(out);
		}
		out.writeInt(haspro);
	}
	
	@Override
	public String toString() {
		return "[TaskAgg] = " + this.taskAgg 
			+ "\n" + this.counters.toString();
	}
	public void setupdateKernelsSize(int lastMapNum, int mapNum) {
		this.deltaPara = new DeltaParaOfCNN();
		deltaPara.setDeltaKernels(new double[lastMapNum][mapNum][][]);
	}
	public void setUpdataKernel(int i, int j, double[][] deltaKernel) {
		this.deltaPara.setUpdataKernel(i, j, deltaKernel);
		
	}
	public double[][][][] getDeltaKernels() {
		return this.deltaPara.getDeltaKernels();
	}
	
	public DeltaParaOfCNN getDeltaPara() {
		return deltaPara;
	}
	public void setDeltaPara(DeltaParaOfCNN deltaPara) {
		this.deltaPara = deltaPara;
	}
	public void setupdateBiasSize(int mapNum) {
		deltaPara.setDeltaBias(new double[mapNum]);
	}
	public void setUpdataBias(int j, double deltaBias) {
		// TODO Auto-generated method stub
		this.deltaPara.setUpdataBia(j, deltaBias);
	}
	public void setHasPro(int haspro) {
		// TODO Auto-generated method stub
		this.haspro = haspro;
	}
	public int getHaspro() {
		return haspro;
	}
}
