/**
 * Termite System
 * copyright 2011-2016
 */
package org.apache.hama.myhama.api;

import org.apache.hama.myhama.util.Context;

/**
 * This class provides an abstract implementation of {@link BSPInterface}. 
 * Users can define their own computing logics by implementing 
 * or extending functions in {@link BSP}.
 * 
 * Typically, an iterative computing algorithm is executed by multiple 
 * tasks in parallel. The computation consists of a series of supersteps 
 * separated by global synchronous barriers. 
 * In one superstep, each task loops its local VBlocks to process 
 * every vertex if necessary, based on user-defined functions 
 * update() and getMessages().
 * 
 * @author zhigang wang
 * @version 0.1
 */
public abstract class BSP<V, W, M, I> implements BSPInterface<V, W, M, I> {
	
	@Override
	public void taskSetup(Context<V, W, M, I> context) {
		//do nothing as default.
	}
	
	
	@Override
	public void superstepSetup(Context<V, W, M, I> context) {
		//do nothing as default.
	}
	
	/**
	 * Setup before processing vertices in one VBlock.
	 * 
	 * Currently, users can use this function to decide 
	 * whether vertices in this VBlock should be processed or not.
	 * Specifically, users may set the update rule by 
	 * {@link Context}.setVBlockUpdateRule({@link Constants}.VBlockUpdateRule rule). 
	 * By default, the rule is {@link Constants}.VBlockUpdateRule.MSG_DEPEND, 
	 * indicating that vertices will be updated if and only if they 
	 * have received messages.
	 * 
	 * @param context
	 * @return
	 */
	@Override
	public void vBlockSetup(Context<V, W, M, I> context) {
		
	}
	
	@Override
	public void vBlockCleanup(Context<V, W, M, I> context) {
		//do nothing as default.
	}
	
	@Override
	public void superstepCleanup(Context<V, W, M, I> context) {
		//do nothing as default.
	}
	
	@Override
	public void taskCleanup(Context<V, W, M, I> context) {
		//do nothing as default.
	}
}

