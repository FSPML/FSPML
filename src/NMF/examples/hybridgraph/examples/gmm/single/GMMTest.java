package hybridgraph.examples.gmm.single;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.Arrays;

import blog.distrib.MultivarGaussian;
import Jama.Matrix;

/**
 * Jiangtao's Concurrent GMM implementation (single machine)
 * @author root
 *
 */
public class GMMTest {

	private static int dim = 55;
	private static int k = 40;
	private static int nodenum = 1000;
	
	private static Matrix[] nodes;
	private static double[][] val_E;
	private static double[][] val_relations; // node values
	
	private static Matrix[] p_centroids;
	private static Matrix[] p_sigma;
	private static double[] p_weight;
	
	private static double[] agg_R; // sum of r_{ij}, i.e., R
	private static double[][] agg_X; // sum of r_{ij}*x_i, i.e., X
	private static double[][] agg_S; // sum of r_{ij}*(x_i)^2, i.e., S
	private static double agg_E;
	
	private static double error;
	private static int max_iterations;
	private static int batch;
	
	public static void loadNodeToMem(String tableFile) throws Exception {
		System.out.println("begin to load data points...");
		BufferedReader br = new BufferedReader(new FileReader(tableFile));
	    
		int count = 0;
		while(br.ready()){
			String line = br.readLine();

			int index = line.indexOf("\t");
			if(index != -1){
				String nodestr = line.substring(0, index);
				int nodeid = Integer.parseInt(nodestr);
				
				String information = line.substring(index+1);
				String items[] = information.split(",");
				Matrix node = new Matrix(dim, 1);
				
				for(int i=0; i<items.length && i<dim; i++)
				{
					double value = Double.parseDouble(items[i]);
					node.set(i, 0, value); //row, col, value
				}
				nodes[nodeid] = node;
				count++;
			}
		}
		br.close();
		System.out.println("load " + count + " points successfully!");
	}
	
	public static void initcentroids()
	{
		for(int i=0; i<k; i++)
		{
			Matrix node = new Matrix(dim, 1);
			for (int j=0; j<dim; j++) {
				node.set(j, 0, nodes[i].get(j, 0));
			}
			p_centroids[i] = node;
		}
	}
	
	public static void initweight()
	{
		for(int i=0; i<k; i++)
		{
			p_weight[i] = 1.0/k;
		}
	}
	
	public static void initsigma()
	{
		for(int i=0; i<k; i++)
		{
			p_sigma[i] = Matrix.identity(dim, dim); //1
		}
	}
	

	public static void compute(int iteCounter)
	{
		int batchCounter = 0;
		/** 
		 * E-step, compute probability.
		 * Input:  one node, p_centroids, p_sigma, p_weight
		 * Output: 
		 *  */
		MultivarGaussian[] mgs = new MultivarGaussian[k];
		for (int j=0; j<k; j++) {
			mgs[j] = new MultivarGaussian(p_centroids[j], p_sigma[j]);
			
			/*StringBuffer sbc = new StringBuffer();
			StringBuffer sbs = new StringBuffer();
			StringBuffer sbn = new StringBuffer();
			if (j==0) {
				for (int m = 0; m < 6; m++) {
					sbc.append(p_centroids[j].get(m, 0) + " ");
					sbs.append(p_sigma[j].get(m, 0) + " ");
					sbn.append(nodes[0].get(m, 0) + " ");
				}
				
				System.out.println("special-c0:\n" + sbc.toString());
				System.out.println("special-s0:\n" + sbs.toString());
				System.out.println("special-n0:\n" + sbn.toString());
			}*/
		}
		
		
		for(int i=0; i<nodenum; i++)
		{
			Matrix node = nodes[i];
			double belongs[] = new double[k];
			double totalvalue = 0.0, lastvalue = 0.0;
			
			for(int j=0; j<k; j++)
			{
				belongs[j] = p_weight[j] * mgs[j].getProb(node);
				totalvalue += belongs[j];
				
				lastvalue += val_E[i][j];
				val_E[i][j] = belongs[j];
				
				/*if (i == 0) {
					double[] ww = new double[]{0.5338920546310262,0.12888913421899517,0.279285046602186,0.1457300901498464,0.33881725812758323,0.8296714759585713};
					double[] cc = new double[]{0.4234374898072672, 0.41522334857866866, 0.47618309269938386, 0.3675906424315059, 0.5613085978659965, 0.5561323342976876};
					double[] ss = new double[]{0.04881443992288631, 0.059899386195012866, 0.06981063756203354, 0.07024232790724483, 0.06481932717616662, 0.09960598102751772};
					Matrix w = new Matrix(6, 1);
					Matrix c = new Matrix(6, 1);
					Matrix s = new Matrix(6, 6);
					for (int ii = 0; ii < 6; ii++) {
						w.set(ii, 0, ww[ii]);
						c.set(ii, 0, cc[ii]);
						s.set(ii, ii, ss[ii]);
					}
					System.out.print(new MultivarGaussian(c, s).getProb(node) + " ");
					//System.out.print(new MultivarGaussian(p_centroids[j], p_sigma[j]).getProb(node) + " ");
				}*/
			}
			/*if (i == 0) {
				System.out.println("\nnode-" + i + " belongs=" + Arrays.toString(belongs));
			}*/
			if (iteCounter == 0) {
				agg_E += Math.log(totalvalue);
			} else {
				agg_E += (Math.log(totalvalue)-Math.log(lastvalue));
			}
			
			for(int j=0; j<k; j++)
			{
				//System.out.println(belongs[j] + " " + totalvalue);
				double r = belongs[j]/totalvalue;
				
				if (iteCounter == 0) {
					agg_R[j] += r;
					for(int d=0; d<dim; d++)
					{
						double value = nodes[i].get(d, 0);
						agg_X[j][d] += r*value;
						agg_S[j][d] += r*value*value;
					}
				} else {
					agg_R[j] += (r-val_relations[i][j]);
					for(int d=0; d<dim; d++)
					{
						double value = nodes[i].get(d, 0);
						agg_X[j][d] += (r-val_relations[i][j])*value;
						agg_S[j][d] += (r-val_relations[i][j])*value*value;
					}
				}
				
				val_relations[i][j] = r;
			}
			
			batchCounter++;
			if (iteCounter>0 && batch>0 && (batchCounter%batch)==0) {
				updateParameters();
				for (int j=0; j<k; j++) {
					mgs[j] = new MultivarGaussian(p_centroids[j], p_sigma[j]);
				}
				System.out.println("  batch once...");
			}
		}
		
		/** 
		 * M-step, 
		 * 1) update parameters: weight, centroids, and sigma, 
		 *    using intermediate variables, i.e., aggregators: agg_R, agg_X, and agg_S;
		 * 2) compute objective function value: error
		 *    using aggregator agg_E
		 *  */
		updateParameters();	
	}
	
	private static void updateParameters() {
		error = agg_E/nodenum;
		for(int j=0; j<k; j++)
		{
			p_weight[j] = agg_R[j]/nodenum;
			
			for(int d=0; d<dim; d++)
			{
				double value = agg_X[j][d]/agg_R[j];
				p_centroids[j].set(d, 0, value);
				
				double sigmavalue = 
					agg_S[j][d]/agg_R[j] - (agg_X[j][d]*agg_X[j][d])/(agg_R[j]*agg_R[j]);
				p_sigma[j].set(d, d, sigmavalue);
			}
		}
	}
	
	public static void main(String[] args) throws Exception
	{
		args = new String[]{
				"/usr/lily/HybridGraph/kmeans-rd-point10000-dim20", 
				"10000", "20", "6", "50", "-1"
		};
		
		String datafile = null, output = null;
		if (args.length != 6) {
    		System.out.println("Usage: " +
    				"[input] [#nodes] [#dimensions] [#centers] [#iterations] [#batch:-1=>concurrent,>0=>block]");
    		System.exit(-1);
    	} else {
    		datafile = args[0];
    		nodenum = Integer.parseInt(args[1]);
    		dim = Integer.parseInt(args[2]);
    		k = Integer.parseInt(args[3]);
    		max_iterations = Integer.parseInt(args[4]);
    		batch = Integer.parseInt(args[5]);
    		output = args[0] + "-" + args[1] + "-" 
    				+ args[2] + "-" + args[3] + "-" 
    				+ args[4] + "-" + args[5];
    	}
		BufferedWriter bw = new BufferedWriter(new FileWriter(output));
		bw.write("intput=" + args[0]); bw.newLine();
		bw.write("#nodes=" + args[1]); bw.newLine();
		bw.write("#dimen=" + args[2]); bw.newLine();
		bw.write("#centers=" + args[3]); bw.newLine();
		bw.write("#iterations=" + args[4]); bw.newLine();
		bw.write("#batch=" + args[5]); bw.newLine();
		
		nodes = new Matrix[nodenum];
		val_E = new double[nodenum][k];
		val_relations = new double[nodenum][k];
		
		p_sigma = new Matrix[k];
		p_centroids = new Matrix[k];
		p_weight = new double[k];
		
		agg_R = new double[k];
		agg_X = new double[k][dim];
		agg_S = new double[k][dim];
		agg_E = 0.0;
		
		loadNodeToMem(datafile);
		initcentroids();
		initweight();
		initsigma();
		
		boolean flag = true;
		int iteCounter = 0;
		error = 0.0;
		/** cleanup aggregators: agg_R, agg_X, agg_S, agg_E */
		agg_E = 0.0;
		for(int j=0; j<k; j++)
		{
			agg_R[j] = 0;
			for(int d=0; d<dim; d++)
			{
				agg_X[j][d] = 0;
				agg_S[j][d] = 0;
			}
		}
		
		/*Matrix m = new Matrix(5, 5); //Matrix.identity(5, 5);
		System.out.print("\n");
		for (int i = 0; i < 5; i++) {
			for (int j = 0; j < 5; j++) {
				System.out.print(m.get(i, j) + " ");
			}
			System.out.print("\n");
		}*/
		
		//printcentroids();
		System.out.println("#GMM concurrent");
		long start = System.currentTimeMillis();
		long ite_start = System.currentTimeMillis();
		while(flag)
		{
			compute(iteCounter);
			//printIntermediate();
			//printcentroids();
			
			long ite_end = System.currentTimeMillis();
			System.out.println(iteCounter + "\t" + error + "\t" + (ite_end - ite_start)/1000.0 + "sec");
			bw.write(iteCounter + "\t" + error + "\t" + (ite_end - ite_start)/1000.0 + "sec");
			bw.newLine();
			ite_start = ite_end;
			
			iteCounter++;
			if(iteCounter>max_iterations)
				flag = false;
		}
		long end = System.currentTimeMillis();
		System.out.println("run successfully, runtime=" + (end-start)/1000.0 + " seconds");
		bw.write("run successfully, runtime=" + (end-start)/1000.0 + " seconds");
		bw.newLine();
		bw.close();
		System.out.println("output path = " + output);
	}
	
	public static void printcentroids()
	{
		for(int j=0; j<k; j++)
		{
			//centroids[j].print(0, 0);
			//centroids[j].print(1, 0);
			System.out.print("centriod " + j + ": ");
			for(int c=0; c<dim; c++)
				System.out.print(p_centroids[j].get(c, 0) + ";");
			System.out.println("");
		}
		
		
		for(int t=0; t<k; t++)
		{
			System.out.print("sigma " + t + ":");
			for(int j=0; j<dim; j++)
			{
				System.out.print(p_sigma[t].get(j, j) + ",");
			}
			System.out.println();
		}
		
		System.out.println("weights:" + Arrays.toString(p_weight));
	}
	
	
	
	
	
	public static void printIntermediate()
	{
		System.out.println("agg_R: ");
		for(int j=0; j<k; j++)
		{
			//centroids[j].print(0, 0);
			//centroids[j].print(1, 0);
			
			System.out.print(agg_R[j] + ";");
		}
		System.out.println();
		System.out.println("agg_X: ");
		for(int t=0; t<k; t++)
		{
			for(int i=0; i<dim; i++)
			{
				System.out.print(agg_X[t][i] + " ");
			}
			System.out.println();
		}
		
		System.out.println("agg_S: ");
		for(int t=0; t<k; t++)
		{
			for(int i=0; i<dim; i++)
			{
				System.out.print(agg_S[t][i] + " ");
			}
			System.out.println();
		}
		
	}
}
