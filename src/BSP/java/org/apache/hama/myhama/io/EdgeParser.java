package org.apache.hama.myhama.io;

import java.util.ArrayList;

/**
 * Parse the string argument as edges (edge ids [and weights]) 
 * around the matches of the given characters.
 * @author root
 *
 */
public class EdgeParser {
	
	public class IntDoubleEdgeSet {
		Integer[] ids;
		Double[] weights;
		
		public IntDoubleEdgeSet(Integer[] _ids, Double[] _weights) {
			this.ids = _ids;
			this.weights = _weights;
		}
		
		public Integer[] getEdgeIds() {
			return this.ids;
		}
		
		public Double[] getEdgeWeights() {
			return this.weights;
		}
	}
	
	/**
	 * Parse the string argument as target vertex ids of edges 
	 * around the matches of the given character.
	 * @param eData
	 * @param c
	 * @return
	 */
	public Integer[] parseEdgeIdArray(String eData, char c) {
        ArrayList<Integer> tmpEdgeId = new ArrayList<Integer>();
    	char edges[] = eData.toCharArray();
        int begin = 0, end = 0;
        for(end = 0; end < edges.length; end++) {
            if(edges[end] != c) {
                continue;
            }
            tmpEdgeId.add(Integer.valueOf(
            		new String(edges, begin, end-begin)));
            begin = ++end;
        }
        tmpEdgeId.add(Integer.valueOf(
        		new String(edges, begin, end-begin)));
        
        Integer[] edgeIds = new Integer[tmpEdgeId.size()];
        tmpEdgeId.toArray(edgeIds);
        
        return edgeIds;
	}
	
	/**
	 * Parse the string argument as a group of target vertex id && 
	 * edge weight around the matches of the given character.
	 * @param eData
	 * @return
	 */
	public IntDoubleEdgeSet parseEdgeIdWeightArray(String eData, char c) {
        ArrayList<Integer> tmpEdgeId = new ArrayList<Integer>();
        ArrayList<Double> tmpEdgeWeight = new ArrayList<Double>();
        boolean isId = true;
    	char edges[] = eData.toCharArray();
        int begin = 0, end = 0;
        for(end = 0; end < edges.length; end++) {
            if(edges[end] != c) {
                continue;
            }
            if (isId) {
                tmpEdgeId.add(Integer.valueOf(
                		new String(edges, begin, end-begin)));
            } else {
            	tmpEdgeWeight.add(Double.valueOf(
                		new String(edges, begin, end-begin)));
            }
            isId = !isId;

            begin = ++end;
        }
        tmpEdgeWeight.add(Double.valueOf(
        		new String(edges, begin, end-begin)));
        
        Integer[] edgeIds = new Integer[tmpEdgeId.size()];
        tmpEdgeId.toArray(edgeIds);
        Double[] edgeWeights = new Double[tmpEdgeWeight.size()];
        tmpEdgeWeight.toArray(edgeWeights);
        
        return new IntDoubleEdgeSet(edgeIds, edgeWeights);
	}
	
	/**
	 * Parse the string argument as dimensional values (float) 
	 * around the matches of the given character.
	 * @param dimData
	 * @param c
	 * @return
	 */
	public double[] parseDimensionArray(String dimData, char c) {
        ArrayList<Double> tmpDimVals = new ArrayList<Double>();
    	char dims[] = dimData.toCharArray();
        int begin = 0, end = 0;
        for(end = 0; end < dims.length; end++) {
            if(dims[end] != c) {
                continue;
            }
            tmpDimVals.add(Double.valueOf(
            		new String(dims, begin, end-begin)));
            begin = ++end;
        }
        tmpDimVals.add(Double.valueOf(
        		new String(dims, begin, end-begin)));
        
        double[] dimVals = new double[tmpDimVals.size()];
        int idx = 0;
        for (double val: tmpDimVals) {
        	dimVals[idx++] = val;
        }
        
        return dimVals;
	}
}
