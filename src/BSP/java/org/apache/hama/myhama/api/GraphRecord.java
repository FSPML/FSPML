package org.apache.hama.myhama.api;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * GraphRecord is a data structure used by HybridGraph. 
 * Intuitively, it represents one adjacency list.
 *  
 * Users should define their own representation by 
 * extending {@link GraphRecord}.
 * 
 * @author zhigang wang
 *
 * @param <V> vertex value
 * @param <W> edge weight
 * @param <M> message value
 * @param <I> graph information
 */
public class GraphRecord<V, W, M, I> implements Writable {
	
	public GraphRecord() { };
	
	//===================================================================
	// Variables and operations in Triple (i.e., elements of a VBlock)
	//===================================================================
	protected int verId;
	protected V verValue;
	
	public void setVerId(int _verId) {
		verId = _verId;
	}
	
	public final int getVerId() {
		return verId;
	}
	
	/**
	 * Set the vertex value.
	 * The physical meaning of vertex value in HybridGraph 
	 * may be different from that in Giraph for some algorithms. 
	 * Actually, in HybridGraph, vertex value is used to generate 
	 * correct messages in {@link BSP}.getMessages() without 
	 * help of statistical information of edges. 
	 * This is because edges may be divided and then stored in 
	 * several fragments, and in getMessages(), only partial edges 
	 * in one fragment are provided. Obviously, the global statistical 
	 * information, such as out-degree, is not available.
	 * Take PageRank as an example. 
	 * it is PageRank_Score/out-degree, 
	 * because "out-degree" is not available in getMessages(). 
	 * However, for Giraph, it is PageRank_Score, and the corrent 
	 * message value can be calculated based on out-degree, because 
	 * edges are not divided. 
	 * 
	 * @param _verValue
	 */
	public void setVerValue(V _verValue) {
		verValue = _verValue;
	}
	
	/**
	 * Get a read-only vertex value.
	 * The physical meaning of vertex value in HybridGraph 
	 * may be different from that in Giraph for some algorithms. 
	 * Actually, in HybridGraph, vertex value is used to generate 
	 * correct messages in {@link BSP}.getMessages() without 
	 * help of statistical information of edges. 
	 * This is because edges may be divided and then stored in 
	 * several fragments, and in getMessages(), only partial edges 
	 * in one fragment are provided. Obviously, the global statistical 
	 * information, such as out-degree, is not available.
	 * Take PageRank as an example. 
	 * it is PageRank_Score/out-degree, 
	 * because "out-degree" is not available in getMessages(). 
	 * However, for Giraph, it is PageRank_Score, and the corrent 
	 * message value can be calculated based on out-degree, because 
	 * edges are not divided. 
	 * @return
	 */
	public final V getVerValue() {
		return verValue;
	}
    
    /**
     * Get the correct vertex value and save it onto HDFS. 
     * Return this.verValue if {@link BSPJob}useGraphInfoInUpdate(false). 
     * Otherwise, it should be defined based on the logics in 
     * {@link BSP}.update(). 
     * Take PageRank as an example, this.verValue actually represents 
     * PageRank_Score/out-degree calculated in {@link BSP}.update(). 
     * Thus, users should recovery the correct value PageRank_Score 
     * by this.verValue*this.graphInfo.
     * @return
     */
    public V getFinalValue() {
    	return verValue;
    }
    
    protected double[] dimensions;
    
    public void setDimensions(double[] _dim) {
    	this.dimensions = _dim;
    }
    
    public double[] getDimensions() {
    	return this.dimensions;
    }
    
    public int getNumOfDimensions() {
    	return (this.dimensions==null? 0:this.dimensions.length);
    }
    
    @Override
	public void write(DataOutput out) throws IOException {
    	out.writeInt(this.verId);
    	int count = getNumOfDimensions();
    	out.writeInt(count);
    	for (int i = 0; i < count; i++) {
    		out.writeDouble(this.dimensions[i]);
    	}
    }
    
    @Override
	public void readFields(DataInput in) throws IOException {
    	this.verId = in.readInt();
    	int count = in.readInt();
    	this.dimensions = new double[count];
    	for (int i = 0; i < count; i++) {
    		this.dimensions[i] = in.readDouble();
    	}
    }
    
	public void readBytes(DataInputStream in) throws IOException {
		this.verId = in.readInt();
		int count = in.readInt();
    	this.dimensions = new double[count];
    	for (int i = 0; i < count; i++) {
    		this.dimensions[i] = in.readDouble();
    	}
	}
	
	public void writeBytes(DataOutputStream out) throws IOException {
		out.writeInt(this.verId);
    	int count = getNumOfDimensions();
    	out.writeInt(count);
    	for (int i = 0; i < count; i++) {
    		out.writeDouble(this.dimensions[i]);
    	}
	}
		
   //==========================================================
   // The following functions must be implemented by users.
   //==========================================================
   /**
     * Parse the graph data and then initialize variables 
     * in {@link GraphRecord}.
     * This function is only be invoked in 
     * {@link GraphDataServer}.loadGraph().
     * vData is read from HDFS as the <code>key</code> 
     * and eData is read from HDFS as the <code>value</code>.
     * 
     * @param vData String
     * @param eData String
     */
    public void parseGraphData(String vData, String eData) {
    	
    }
    
    /**
     * Only used for testing checkpoint
     * @param valData
     */
    public void parseVerValue(String valData) {
    	
    }
    
    @Override
    public String toString() {
    	StringBuffer sb = new StringBuffer();
    	sb.append("{");
    	sb.append(Integer.toString(this.verId));
    	sb.append(", [");
    	
    	if (this.dimensions != null) {
    		sb.append(Double.toString(this.dimensions[0]));
    		for (int i = 1; i < this.dimensions.length; i++) {
    			sb.append(",");
    			sb.append(Double.toString(this.dimensions[i]));
    		}
    	}
    	
    	sb.append("]}");
    	return sb.toString();
    }
}