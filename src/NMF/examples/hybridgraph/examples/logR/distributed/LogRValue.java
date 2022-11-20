package hybridgraph.examples.logR.distributed;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;


public class LogRValue implements Writable{
	private double lost;
	
	public LogRValue() {
		this.lost = 0.0;
	}
	
	

	public double getLost() {
		return lost;
	}



	public void setLost(double lost) {
		this.lost = lost;
	}



	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.lost = in.readDouble();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeDouble(lost);
	}

}
