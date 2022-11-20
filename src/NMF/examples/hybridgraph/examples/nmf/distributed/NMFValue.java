package hybridgraph.examples.nmf.distributed;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * 
 * @author panchao dai
 */
public class NMFValue implements Writable {
	
	// 一条数据的损失值
	private double lost;
	
	public NMFValue() {
		this.lost = 0.0;
	}

	public double getLost() {
		return lost;
	}

	public void setLost(double lost) {
		this.lost = lost;
	}

	@Override
	public void readFields(DataInput input) throws IOException {
		// TODO Auto-generated method stub
		this.lost = input.readDouble();
	}

	@Override
	public void write(DataOutput output) throws IOException {
		// TODO Auto-generated method stub
		output.writeDouble(lost);
	}

}
