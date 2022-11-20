package hybridgraph.examples.kmeans.distributed;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class KmeansValue implements Writable{
	private int tag;
	private double distance;
	
	public KmeansValue() {
		this.tag = -1;
		this.distance = -1.0;
	}
	
	public void set(int _tag, double _distance) {
		this.tag = _tag;
		this.distance = _distance;
	}
	
	public int getTag() {
		return this.tag;
	}
	
	public double getDistance() {
		return this.distance;
	}
	
	@Override
	public boolean equals(Object o) {
		if (!(o instanceof KmeansValue)) {
			return false;
		}
		
		KmeansValue other = (KmeansValue) o;
		if (this.tag==other.tag 
				&& this.distance==other.distance) {
			return true;
		} else {
			return false;
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.tag = in.readInt();
		this.distance = in.readDouble();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.tag);
		out.writeDouble(this.distance);
	}
}
