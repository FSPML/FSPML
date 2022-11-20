package hybridgraph.examples.nmf.distributed;

import hybridgraph.examples.fcm.distributed.FCMValue;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hama.myhama.api.BSP;
import org.apache.hama.myhama.api.GraphRecord;
import org.apache.hama.myhama.util.Context;

/**
 * 
 * @author panchao dai
 */
public class NMFBSP extends BSP<NMFValue, Integer, Integer, Integer> {
	
	private int column;
	private double[] matrixW;
	private double[][] matrixH;
	private int mini = Integer.MAX_VALUE; // 该分块的最小 id
	private Log LOG = LogFactory.getLog(NMFBSP.class);

	@Override
	public void getLocalData(GraphRecord<NMFValue, Integer, Integer, Integer>[][] localPoints) {
		// do nothing
	}

	@Override
	public void taskSetup(Context<NMFValue, Integer, Integer, Integer> context) {
		this.column = context.getBSPJobInfo().getNumOfDimensions();
//		this.matrixW = new double[context.getLine()][6];
//		this.matrixW = context.getMatrixW();
		this.matrixH = new double[6][this.column];
		this.matrixH = context.getMatrixH();
	}
	
	@Override
	public void superstepSetup(Context<NMFValue, Integer, Integer, Integer> context) {
//		this.matrixW = context.getMatrixW();
		this.matrixH = context.getMatrixH();
	}

	@Override
	public void update(Context<NMFValue, Integer, Integer, Integer> context) throws Exception {
		
		GraphRecord<NMFValue, Integer, Integer, Integer> point = context.getGraphRecord();
		double[] V = new double[this.column];
		double[] V1 = new double[this.column];
		V = point.getDimensions();
//		int id = point.getVerId();
//		this.mini = Math.min(this.mini, id);
		this.matrixW = context.getMatrixW();
		V1 = multiply(this.matrixW, this.matrixH);
		
		NMFValue lastValue = point.getVerValue();
		NMFValue curValue = new NMFValue();
		curValue.setLost(costFun(V, V1));
		
		if (context.getSuperstepCounter() > 1) {
			if (!curValue.equals(lastValue)) {
				point.setVerValue(curValue);
				context.setVertexAgg((curValue.getLost() - lastValue.getLost()));
			}
		} else {
			point.setVerValue(curValue);
			context.setVertexAgg(curValue.getLost());
		}
		
		double[][] V2 = new double[1][this.column];
		V2[0] = V;
		updateMatrixW(context.getMatSet().getPart(), context.getMatSet().getIdx(), context.getMatSet().getLen(), V2);
	}

	@Override
	public void updateOnGpu(Context<NMFValue, Integer, Integer, Integer> context) throws Exception {

	}

	/**
	 * 更新矩阵 W
	 */
	public void updateMatrixW(int i, int idx, int len, double[][] V) {
		double[][] W = new double[1][6];
		W[0] = this.matrixW;
		W = sub(5E-8, multiply(V, transpose(this.matrixH)), multiply(multiply(W, this.matrixH), transpose(this.matrixH)));
		
		for (int k = 0; k < 6; k++) {
			this.matrixW[k] += W[0][k];
		}
	}
	
	/**
     * 一维矩阵与二维矩阵相乘
     */
    public double[] multiply(double[] a, double[][] b) {
        int n = a.length;
        int m = b[0].length;
        double[] ans = new double[m];

        for (int i = 0; i < m; i++) { // b有多少列
        	ans[i] = 0;
            for (int j = 0; j < n; j++) { // a有多少列
                ans[i] += a[j] * b[j][i];
            }
        }
        
        return ans;
    }
    
    /**
	 * 矩阵相减
	 */
	private double[][] sub(double base, double[][] a, double[][] b) {
		int n = a.length;
		int m = a[0].length;
		double[][] ans = new double[n][m];
		
		for (int i = 0; i < n; i++) {
			for (int j = 0; j < m; j++) {
				ans[i][j] = base * (a[i][j] - b[i][j]);
			}
		}
		
		return ans;
	}
	
	/**
	 * 矩阵转置
	 */
	private double[][] transpose(double[][] a) {
        int n = a.length;
        int m = a[0].length;
        double[][] ans = new double[m][n];

        for (int i = 0; i < m; i++) {
            for (int j = 0; j < n; j++) {
                ans[i][j] = a[j][i];
            }
        }
        return ans;
    }
	
	/**
	 * 普通矩阵相乘
	 */
	private double[][] multiply(double[][] a, double[][] b) {
        int n = a.length;
        int m = b[0].length;
        int l = a[0].length;
        double[][] ans = new double[n][m];

        for (int i = 0; i < n; i++) {
            for (int j = 0; j < m; j++) {
                ans[i][j] = 0;
                for (int k = 0; k < l; k++) {
                    ans[i][j] += a[i][k] * b[k][j];
                }
            }
        }
        return ans;
    }
    
    /**
     * 得到lost
     */
    public double costFun(double[] a, double[] b) {
		double lost = 0;
		
		for (int i = 0; i < a.length; i++) {
			lost = lost + (a[i] - b[i]) * (a[i] - b[i]);
		}
		
		return lost;
	}

}
