/**
 * copyright 2012-2010
 */
package org.apache.hama.ipc;

import java.io.Closeable;

import org.apache.hama.Constants;
import org.apache.hama.bsp.BSPRPCProtocolVersion;
import org.apache.hama.monitor.JobInformation;
import org.apache.hama.myhama.api.MsgRecord;
import org.apache.hama.myhama.comm.MsgPack;
import org.apache.hama.myhama.comm.SuperStepCommand;
import org.apache.hama.myhama.util.CenterSetOfKmeans;

/**
 * CommunicationRPCProtocol.
 * 
 * @author 
 * @version 0.1
 */
public interface CommunicationServerProtocol<V, W, M, I> 
		extends BSPRPCProtocolVersion,
		Closeable, Constants {
	public int parId = 0;
	
	/**
	 * Build RouteTable.
	 */
	public void buildRouteTable(JobInformation global);
	
	/**
	 * Set the preparation information.
	 */
	public void setPreparation(JobInformation local, SuperStepCommand ssc);
	
	/**
	 * Receive messages from source vertices.
	 * Used in style.Push.
	 * 
	 * @param srcParId
	 * @param iteNum
	 * @param pack
	 * @return #messagesOnDisk for push
	 * @throws Exception
	 */
	public long recMsgData(int srcParId, int iteNum, 
			MsgPack<V, W, M, I> pack) throws Exception;
	
	/**
	 * Obtain {@link MsgRecord} from tasks which contain edges.
	 * These messages will be put into {@link MsgDataServer}.
	 * @param _srcParId
	 * @param _bid
	 * @param _iteNum
	 * @return
	 */
	public MsgPack<V, W, M, I> obtainMsgData(int _srcParId, int _bid, int _iteNum) 
			throws Exception;
	
	/**
	 * Set the command for the next SuperStep.
	 * @param SuperStepCommand ssc
	 */
	public void setNextSuperStepCommand(SuperStepCommand ssc);

	/**
	 * If all tasks have completed the preparation work, 
	 * then tell all tasks start the next SuperStep.
	 */
	public void startNextSuperStep();
	
	/**
	 * Exit the synchronize signaled by sync().
	 */
	public void quitSync();
	
	//for semi-asynchronous
	public void initiateBarrier();
	
	public void setCentroids(CenterSetOfKmeans _csk, int blockSize);
}
