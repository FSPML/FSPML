package hybridgraph.examples.fcm.distributed;

public class FCMValue {
	private double error;
	private double[] miu; //1*k
	
	public FCMValue(int k) {
		this.error = 0.0;
		this.miu = new double[k];
	}
	
	public void set(double _error, double[] _miu) {
		this.error = _error;
		this.miu = _miu;
	}
	
	public double getError() {
		return this.error;
	}
	
	public double[] getMiu() {
		return this.miu;
	}
}
