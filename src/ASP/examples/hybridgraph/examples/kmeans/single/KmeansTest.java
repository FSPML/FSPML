package hybridgraph.examples.kmeans.single;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;
//import java.util.Arrays;

/**
 * A single-machine version of K-means for testing the correctness 
 * of our distributed implmentation. Only compute and output the centers.
 * @author root
 *
 */
public class KmeansTest {
	
	public static void main(String[] args) throws Exception {
    	String input = null, output = null;
    	int numOfCenters = 1;
    	int numOfIterations = 10;
    	int batch = -1;
    	//args = new String[]{"input", "6", "500", "-1"};
    	
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
    	
    	ArrayList<ArrayList<Double>> points = load(input);
    	int[] tags = new int[points.size()];
    	Arrays.fill(tags, -1);
    	ArrayList<Double>[] centers = initCenters(points, numOfCenters);
    	//ArrayList<Float> errors = new ArrayList<Float>();
    	int counterOfIterations = 0;
    	int counterOfBatch = 0;
    	int numOfDimensions = points.get(0).size();
    	
    	File outputF = new File(output);
    	FileWriter fw = new FileWriter(outputF);
    	BufferedWriter bw = new BufferedWriter(fw);
    	
    	long start = System.currentTimeMillis();
		double[][] aggOfSum = new double[numOfCenters][numOfDimensions];
		int[] aggOfCounter = new int[numOfCenters];
    	while (counterOfIterations < numOfIterations) {
    		//bw.write(stringOfCenters(counterOfIterations, centers));
    		//bw.newLine();
    		
    		double dist, minDist, error = 0.0f;
    		int tag;
    		counterOfBatch = 0;
    		for (ArrayList<Double> point: points) {
    			minDist = Double.MAX_VALUE;
    			tag = -1;
    			for (int i = 0; i < numOfCenters; i++) {
    				dist = distance(point, centers[i]);
    				if (dist < minDist) {
    					minDist = dist;
    					tag = i;
    				}
    			}
    			error += minDist;
    			if (tags[counterOfBatch] != tag) {
    				aggregate(aggOfSum, aggOfCounter, point, 
    						tag, tags[counterOfBatch], counterOfIterations);
        			tags[counterOfBatch] = tag;
    			}
    			counterOfBatch++;
    			if (counterOfIterations>0 && batch>0 
    					&& (counterOfBatch%batch)==0) {
    				centers = updateCenters(aggOfSum, aggOfCounter);
    			}
    		}
    		
    		//bw.write(Arrays.toString(aggOfSum[0]));
    		//bw.newLine();
    		//bw.write(Arrays.toString(aggOfCounter));
    		//bw.newLine();
    		//errors.add(error);
    		centers = updateCenters(aggOfSum, aggOfCounter);
    		
    		counterOfIterations++;
    		double time = (System.currentTimeMillis()-start)/1000.0;
    		bw.write("ite=" + counterOfIterations + ", error=" + error + ", time=" + time);
    		bw.newLine();
    		if (counterOfIterations%10 == 0) {
    			System.out.println("#iterations=" + counterOfIterations 
    					+ ", #error=" + error);
    		}
    	} //while
    	
    	/*StringBuffer sb = new StringBuffer("errors:");
    	for (Float error: errors) {
    		sb.append("\n");
    		sb.append(error.toString());
    	}
    	bw.write(sb.toString());*/
    	long end = System.currentTimeMillis();
    	bw.write("computation time = " + Double.toString((end-start)/1000.0));
    	bw.newLine();
    	
    	bw.close();
    	fw.close();
    }
	
	/**
	 * Load data points.
	 * @param points
	 * @param input
	 * @throws Exception 
	 */
	private static ArrayList<ArrayList<Double>> load(String input) 
			throws Exception {
		ArrayList<ArrayList<Double>> points = new ArrayList<ArrayList<Double>>();
		File inputF = new File(input);
		FileReader fr = new FileReader(inputF);
		BufferedReader br = new BufferedReader(fr); 
		
		String context = null;
		while((context=br.readLine()) != null) {
			String[] dims = context.split("\t")[1].split(",");
			ArrayList<Double> point = new ArrayList<Double>();
			for (String dim: dims) {
				point.add(Double.parseDouble(dim));
			}
			points.add(point);
		}
		System.out.println("load " + points.size() + " points successfully!");
		
		return points;
	}
	
	/**
	 * Initialize centers before computation.
	 * @param points
	 * @param numOfCenters
	 * @return
	 */
	private static ArrayList<Double>[] initCenters(
			ArrayList<ArrayList<Double>> points, int numOfCenters) {
		ArrayList<Double>[] centers = new ArrayList[numOfCenters];
		for (int i = 0; i < numOfCenters; i++) {
			ArrayList<Double> center = new ArrayList<Double>();
			for (double dim: points.get(i)) {
				center.add(dim);
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
		int numOfDim = point.size();
		for (int i = 0; i < numOfDim; i++) {
			sum = sum + (point.get(i)-center.get(i))*(point.get(i)-center.get(i));
		}
		return Math.sqrt(sum);
	}
	
	private static void aggregate(double[][] aggOfSum, int[] aggOfCounter, 
			ArrayList<Double> point, int tag, int oldTag, int counterOfIterations) {
		int i = 0;
		for (Double dim: point) {
			aggOfSum[tag][i] += dim;
			if (counterOfIterations > 0) {
				aggOfSum[oldTag][i] -= dim;
			}
			i++;
		}
		aggOfCounter[tag]++;
		if (counterOfIterations > 0) {
			aggOfCounter[oldTag]--;
		}
	}
	
	private static ArrayList<Double>[] updateCenters(double[][] aggOfSum, 
			int[] aggOfCounter) {
		int numOfCenters = aggOfSum.length;
		int numOfDim = aggOfSum[0].length;
		ArrayList<Double>[] centers = new ArrayList[numOfCenters];
		for (int i = 0; i < numOfCenters; i++) {
			ArrayList<Double> center = new ArrayList<Double>();
			for (int j = 0; j < numOfDim; j++) {
				center.add(aggOfSum[i][j]/aggOfCounter[i]);
			}
			centers[i] = center;
		}
		return centers;
	}
	
	private static String stringOfCenters(int counterOfIterations, 
			ArrayList<Double>[] centers) {
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < centers.length; i++) {
			sb.append("{");
			sb.append(Integer.toString(i));
			sb.append(", [");
			for (double dim: centers[i]) {
				sb.append(Double.toString(dim));
				sb.append(",");
			}
			sb.append("]}");
			sb.append("\n");
		}
		return sb.toString();
	}
}
