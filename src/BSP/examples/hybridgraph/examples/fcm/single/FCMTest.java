package hybridgraph.examples.fcm.single;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;

/**
 * A single-machine version of Fuzzy C-Means for testing the correctness 
 * of our distributed implmentation. Only compute and output the centers.
 * @author root
 *
 */
public class FCMTest {
	private static String input = null, output = null;
	private static int numOfPoints = 1;
	private static int numOfCenters = 1;
	private static int numOfDimensions = 1;
	private static int numOfIterations = 10;
	private static int batch = -1;
	
	private static ArrayList<ArrayList<Double>> points;
	private static double[][] val_miu; //n*k
	private static double[] val_error; //n
	
	private static ArrayList<Double>[] centers;
	private static double aggOfError;
	
	private static double[][] aggOfSum;
	private static double[] aggOfCounter;
	
	private static double fuzzy_m; //a larger value means FCM converges rapidly.
	
	public static void main(String[] args) throws Exception {
    	args = new String[]{"/usr/lily/HybridGraph/kmeans-rd-point10000-dim20", 
    			"6", "50", "3000"};
    	
    	if (args.length != 4) {
    		System.out.println("Usage: " +
    				"[input] [#centers] [#iterations] [#batch]");
    		System.exit(-1);
    	} else {
    		input = args[0];
    		numOfCenters = Integer.parseInt(args[1]);
    		numOfIterations = Integer.parseInt(args[2]);
    		batch = Integer.parseInt(args[3]);
    		output = args[0] + "-" + args[1] + "-" 
    				+ args[2] + "-" + args[3];
    	}
    	
    	points = load();
    	numOfPoints = points.size();
    	numOfDimensions = points.get(0).size();
    	val_miu = new double[numOfPoints][numOfCenters];
    	val_error = new double[numOfPoints];

    	centers = initCenters();
    	aggOfError = 0.0;
    	
		aggOfSum = new double[numOfCenters][numOfDimensions];
		aggOfCounter = new double[numOfCenters];
		
		fuzzy_m = 1.1; //numOfPoints/(double)(numOfPoints-2);
    	
    	int counterOfIterations = 1;
    	int counterOfBatch = 1;
    	BufferedWriter bw = new BufferedWriter(new FileWriter(output));
    	
    	System.out.println("##Fuzzy C-Means, fuzzy=" + fuzzy_m);
    	long start = System.currentTimeMillis();
    	while (counterOfIterations <= numOfIterations) {
    		//bw.write(stringOfCenters(counterOfIterations, centers));
    		//bw.newLine();
    		
    		long iteStart = System.currentTimeMillis();
    		counterOfBatch = 1;
    		for (int i = 0; i < numOfPoints; i++) {
    			ArrayList<Double> point = points.get(i);
    			
    			double[] dist = new double[numOfCenters];
    			for (int j = 0; j < numOfCenters; j++) {
    				dist[j] = distance(point, centers[j]);
    			}
    			
    			double fuzzy_error = 0.0;
    			for (int j = 0; j < numOfCenters; j++) {
    				/** compute fuzzy_miu */
    				double sum = 0.0;
    				for (int k = 0; k < numOfCenters; k++) {
    					sum += Math.pow(dist[j]/dist[k], 2.0/(fuzzy_m-1.0));
    				}
    				double fuzzy_miu = Math.pow(1.0/sum, fuzzy_m);
    				
    				/** compute error */
    				fuzzy_error += fuzzy_miu*Math.pow(dist[j], 2.0);
    				
    				/** update aggregators */
    				double diff = fuzzy_miu - val_miu[i][j];
    				aggOfCounter[j] += diff;
    				for (int k = 0; k < numOfDimensions; k++) {
    					aggOfSum[j][k] += diff*point.get(k);
    				}
    				
    				/** update vertex values */
    				val_miu[i][j] = fuzzy_miu; 
    			}
    			
    			aggOfError += (fuzzy_error-val_error[i]);
				val_error[i] = fuzzy_error;
				//System.out.println("\t" + i + "  " + fuzzy_error);
				
				if (counterOfIterations>1 && batch>0 
    					&& (counterOfBatch%batch)==0) {
    				centers = updateCenters();
    			}
				counterOfBatch++;
    		}

    		//bw.write(Arrays.toString(aggOfSum[0]));
    		//bw.newLine();
    		//bw.write(Arrays.toString(aggOfCounter));
    		//bw.newLine();
    		//errors.add(error);
    		centers = updateCenters();
    		
    		double time = (System.currentTimeMillis()-iteStart)/1000.0;
    		System.out.println(counterOfIterations + "\t" + aggOfError + "\t" + time + "sec");
    		bw.write(counterOfIterations + "\t" + aggOfError + "\t" + time + "sec");
    		bw.newLine();
    		
    		counterOfIterations++;
    	} //while
    	
    	/*StringBuffer sb = new StringBuffer("errors:");
    	for (Float error: errors) {
    		sb.append("\n");
    		sb.append(error.toString());
    	}
    	bw.write(sb.toString());*/
    	long end = System.currentTimeMillis();
    	System.out.println("run successfully, runtime = " + (end-start)/1000.0 + " seconds");
    	bw.write("computation time = " + Double.toString((end-start)/1000.0));
    	bw.newLine();
    	
    	bw.close();
    }
	
	/**
	 * Load data points.
	 * @throws Exception 
	 */
	private static ArrayList<ArrayList<Double>> load() throws Exception {
		ArrayList<ArrayList<Double>> points = new ArrayList<ArrayList<Double>>();
		
		BufferedReader br = new BufferedReader(new FileReader(input));
		String context = null;
		while((context=br.readLine()) != null) {
			String[] dims = context.split("\t")[1].split(",");
			ArrayList<Double> point = new ArrayList<Double>();
			for (String dim: dims) {
				point.add(Double.parseDouble(dim));
			}
			points.add(point);
		}
		br.close();
		System.out.println("load " + points.size() + " points successfully!");
		
		return points;
	}
	
	/**
	 * Initialize centers before computation.
	 * Centers cannot be chosen from input data points. 
	 * Otherwise, zero-error will happen when computing ||xi-cj||/||xi-ck||
	 * @return
	 */
	private static ArrayList<Double>[] initCenters() {
		ArrayList<Double>[] centers = new ArrayList[numOfCenters];
		for (int i = 0; i < numOfCenters; i++) {
			ArrayList<Double> center = new ArrayList<Double>();
			for (int j = 0; j < numOfDimensions; j++) {
				center.add(i+1.0);
			}
			centers[i] = center;
		}
		return centers;
	}
	
	/**
	 * Compute the distance of two points.
	 * @param point
	 * @param center
	 * @return
	 */
	private static double distance(ArrayList<Double> point, ArrayList<Double> center) {
		double sum = 0.0f;
		for (int i = 0; i < numOfDimensions; i++) {
			sum = sum + (point.get(i)-center.get(i))*(point.get(i)-center.get(i));
		}
		return Math.sqrt(sum);
	}
	
	private static ArrayList<Double>[] updateCenters() {
		ArrayList<Double>[] centers = new ArrayList[numOfCenters];
		for (int i = 0; i < numOfCenters; i++) {
			ArrayList<Double> center = new ArrayList<Double>();
			for (int j = 0; j < numOfDimensions; j++) {
				center.add(aggOfSum[i][j]/aggOfCounter[i]);
			}
			centers[i] = center;
		}
		return centers;
	}
}
