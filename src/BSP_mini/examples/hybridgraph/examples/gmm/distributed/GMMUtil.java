package hybridgraph.examples.gmm.distributed;

import org.apache.hama.myhama.api.GraphRecord;

import Jama.Matrix;

public class GMMUtil {
	
	/**
	 * Parse dimensions (1*k) of a given point into a matrix (k*1).
	 * @param point
	 * @return
	 */
	public static Matrix parseRowMatrix(
			GraphRecord<GMMValue, Integer, Integer, Integer> point) {
		Matrix res = new Matrix(point.getNumOfDimensions(), 1);
		int j = 0;
		for (double value: point.getDimensions()) {
			res.set(j, 0, value);
			j++;
		}
		return res;
	}
	
	/**
	 * Parse dimensions (1*k) of a given point into a diag matrix (k*k).
	 * @param point
	 * @return
	 */
	public static Matrix parseDiagMatrix(
			GraphRecord<GMMValue, Integer, Integer, Integer> point) {
		Matrix res = new Matrix(point.getNumOfDimensions(), point.getNumOfDimensions());
		int j = 0;
		for (double value: point.getDimensions()) {
			res.set(j, j, value);
			j++;
		}
		return res;
	}
}
