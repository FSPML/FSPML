package hybridgraph.examples.nmf.single;

import java.io.IOException;
import java.util.Random;

/**
 * NMF 算法单机版实现
 * 
 * @author panchao dai
 */
public class NMFTest {

    // 矩阵大小
    private static int N = 100, M = 100, R = 6;
    private static double[][] V = new double[N][M];
    private static double[][] W = new double[N][R];
    private static double[][] H = new double[R][M];
    private static double[][] V1 = new double[N][M];

    private static double threshold = 15000.0;
    private static int k = 10;
    private static double lost = 0;

    public static void main(String[] args) throws IOException {
        init();
        double sum = 0;

        while (k-- > 0) {
        	System.out.println("W");
        	print(W);
        	System.out.println("H");
            print(H);
        	
            V1 = multiply(W, H);
            sum = 0;
            
            for (int i = 0; i < N; i++) {
                for (int j = 0; j < M; j++) {
                    sum += Math.pow(V[i][j] - V1[i][j], 2);
                }
            }
            
            W = divide(multiplyOther(W, multiply(V, transpose(H))), multiply(W, multiply(H, transpose(H))));
        	H = divide(multiplyOther(H, multiply(transpose(W), V)), multiply(transpose(W), multiply(W, H)));
            lost = sum;
            System.out.println(k + ": " + lost);
            if (lost < threshold) break;
        }
        
        System.out.println("V1 与  V 的差距:" + lost);
    }

    /**
     * 初始化矩阵（矩阵 V 应是真实数据，矩阵 W 和 矩阵 H 内的元素随机初始化）
     */
    public static void init() {
        Random random = new Random();

        for (int i = 0; i < N; i++) {
            for (int j = 0; j < M; j += 10) {
                V[i][j] = random.nextDouble() * 100.0 + random.nextDouble() * 10.0 + random.nextDouble();
            }
        }

        for (int i = 0; i < N; i++) {
            for (int j = 0; j < R; j++) {
                W[i][j] = random.nextDouble() * 100.0 + random.nextDouble() * 10.0 + random.nextDouble();
            }
        }

        for (int i = 0; i < R; i++) {
            for (int j = 0; j < M; j++) {
                H[i][j] = random.nextDouble() * 100.0 + random.nextDouble() * 10.0 + random.nextDouble();
            }
        }
    }

    /**
     * 矩阵转置
     * @param a
     * @return
     */
    public static double[][] transpose(double[][] a) {
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
     * @param a
     * @param b
     */
    public static double[][] multiply(double[][] a, double[][] b) {
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
     * 两个行数和列数一样的矩阵“相乘”，相同位置元素相乘
     * @param a
     * @param b
     * @return
     */
    public static double[][] multiplyOther(double[][] a, double[][] b) {
		int n = a.length;
		int m = a[0].length;
		double[][] ans = new double[n][m];
		
		for (int i = 0; i < n; i++) {
			for (int j = 0; j < m; j++) {
				ans[i][j] = a[i][j] * b[i][j];
			}
		}
		
		return ans;
	}
    
    /**
     * 矩阵相除
     * @param a
     * @param b
     * @return
     */
    public static double[][] divide(double[][] a, double[][] b) {
		int n = a.length;
		int m = a[0].length;
		double[][] ans = new double[n][m];
		
		for (int i = 0; i < n; i++) {
			for (int j = 0; j < m; j++) {
				ans[i][j] = a[i][j] / b[i][j];
			}
		}
		
		return ans;
	}

    /**
     * 打印矩阵
     * @param a
     */
    public static void print(double[][] a) {
        int n = a.length;
        int m = a[0].length;

        for (int i = 0; i < n; i++) {
        	System.out.print(i + "    ");
            for (int j = 0; j < m; j++) {
                System.out.print(a[i][j] + " ");
            }
            System.out.println();
        }
    }

}
