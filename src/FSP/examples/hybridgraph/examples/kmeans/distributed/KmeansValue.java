package hybridgraph.examples.kmeans.distributed;

public class KmeansValue {
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
}
