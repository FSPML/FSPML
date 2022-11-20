package org.apache.hama.myhama.api;

/**
 * UserTool, implemented by users.
 * @author root
 *
 * @param <V> vertex value
 * @param <W> edge weight
 * @param <M> message value
 * @param <I> graph information
 */
public abstract class UserTool<V, W, M, I> {
	/**
	 * Get new user-defined {@link GraphRecord<V, W, M, I>}.
	 * @return
	 */
	public abstract GraphRecord<V, W, M, I> getGraphRecord();
}
