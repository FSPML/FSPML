package hybridgraph.examples.gmm.distributed;

/**
 * Store error values and probabilities, used to 
 * calculate the change of objective function and 
 * aggregators.
 * @author root
 *
 */
public class GMMValue {
	/** 1*k, error value (this, the kth center) */ 
	private double[] errors;
	/** 1*k, probability (this, the kth Gaussian component) */
	private double[] relations;
	
	/**
	 * GMMValue
	 * @param k #centers
	 */
	public GMMValue(int k) {
		this.errors = new double[k];
		this.relations = new double[k];
	}
	
	public void set(double[] _errors, double[] _relations) {
		this.errors = _errors;
		this.relations = _relations;
	}
	
	/**
	 * Return all error values (this, the kth center), 1*k. 
	 * @return
	 */
	public double[] getErrors() {
		return this.errors;
	}
	
	/**
	 * Return all probabilities (this, the kth Gaussian component), 1*k.
	 * @return
	 */
	public double[] getRelations() {
		return this.relations;
	}
}
