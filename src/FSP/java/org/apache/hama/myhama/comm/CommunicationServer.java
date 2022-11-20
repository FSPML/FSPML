/**
 * Termite System
 * 
 * copyright 2012-2010
 */
package org.apache.hama.myhama.comm;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;

import org.apache.hama.Constants;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.BSPJobID;
import org.apache.hama.bsp.TaskAttemptID;
import org.apache.hama.ipc.CommunicationServerProtocol;
import org.apache.hama.monitor.JobInformation;
import org.apache.hama.myhama.util.CenterSetOfKmeans;

/**
 * CommunicationServer. 
 * 
 * @author 
 * @version 0.1
 */
public class CommunicationServer<V, W, M, I> 
		implements CommunicationServerProtocol<V, W, M, I> {
	private static final Log LOG = LogFactory.getLog(CommunicationServer.class);
	private int taskNum;
	private HamaConfiguration conf;
	private BSPJobID jobId;
	private int parId;
	
	private String bindAddr;
	private InetSocketAddress peerAddr;
	private int serverPort;
	private Server server;
	private CommRouteTable<V, W, M, I> commRT;
	
	private volatile Integer mutex = 0;
	private boolean hasNotify = false;
	private SuperStepCommand ssc;
	
	private volatile Boolean semiAsynBarrier = false;
	
	public CommunicationServer (BSPJob job, int parId, TaskAttemptID taskId) 
			throws Exception {
		this.conf = new HamaConfiguration();
		this.jobId = job.getJobID();
		this.parId = parId;
		taskNum = job.getNumBspTask();
		LOG.info("start msg handle threads: " + taskNum);
		
		this.commRT = new CommRouteTable<V, W, M, I>(job, this.parId);
		this.bindAddr = job.get("host");
		this.serverPort = conf.getInt(Constants.PEER_PORT, 
				Constants.DEFAULT_PEER_PORT) 
				+ Integer.parseInt(jobId.toString().substring(17)) 
				+ Integer.parseInt(taskId.toString().substring(26,32));
		this.peerAddr = new InetSocketAddress(this.bindAddr, this.serverPort);
		
		this.server = RPC.getServer(this, 
				this.peerAddr.getHostName(), this.peerAddr.getPort(), this.conf);
		this.server.start();
		LOG.info("CommunicationServer address:" + this.peerAddr.getHostName() 
				+ " port:" + this.peerAddr.getPort());
	}
	
	public String getAddress() {
		return bindAddr;
	}
	
	public Boolean getSemiAsynBarrier() {
		return semiAsynBarrier;
	}
	
	public int getPort() {
		return this.serverPort;
	}
	
	public void clearBefIte(int _iteNum, int _iteStyle) {
		
	}
	
	public void barrier() throws Exception {
		synchronized(mutex) {
			if (!this.hasNotify) {
				mutex.wait();
			}
			
			this.hasNotify = false;
		}
	}
	
	public boolean isSemiAsynBarrier() {
		synchronized(this.semiAsynBarrier) {
			return this.semiAsynBarrier;
		}
	}
	
	@Override
	public long recMsgData(int srcParId, int iteNum, MsgPack<V, W, M, I> pack) 
			throws Exception {
		return 0L;
	}
	
	@Override
	public MsgPack<V, W, M, I> obtainMsgData(int _srcParId, int _bid, int _iteNum) 
			throws Exception {
		return null;
	}
	
	@Override
	public void buildRouteTable(JobInformation global) {
		this.commRT.initialilze(global);
		
		synchronized(mutex) {
			this.hasNotify = true;
			mutex.notify();
		}
	}
	
	@Override
	public void setPreparation(JobInformation _jobInfo, SuperStepCommand _ssc) {
		this.commRT.resetJobInformation(_jobInfo);
		this.ssc = _ssc;
		
		synchronized(mutex) {
			this.hasNotify = true;
			mutex.notify();
		}
	}
	
	@Override
	public void setNextSuperStepCommand(SuperStepCommand _ssc) {
		this.ssc = _ssc;
		
		synchronized(semiAsynBarrier) {
			this.semiAsynBarrier = false;
		}
		
		synchronized(mutex) {
			this.hasNotify = true;
			mutex.notify();
		}
	}
	
	@Override
	public void startNextSuperStep() {
		synchronized(mutex) {
			this.hasNotify = true;
			mutex.notify();
		}
	}
	
	@Override
	public void initiateBarrier() {
		synchronized(semiAsynBarrier) {
			this.semiAsynBarrier = true;
		}
	}
	
	@Override
	public void setCentroids(CenterSetOfKmeans _csk) {
		this.ssc.setGlobalParameters(_csk);
		
		synchronized(semiAsynBarrier) {
			this.semiAsynBarrier = false;
		}
	}
	
	@Override
	public void quitSync() {
		synchronized(mutex) {
			this.hasNotify = true;
			mutex.notify();
		}
	}
	
	@Override
	public long getProtocolVersion(String arg0, long arg1) throws IOException {
		return CommunicationServerProtocol.versionID;	
	}
	
	@Override
	public void close() {
		this.server.stop();
	}
	
	public final CommRouteTable<V, W, M, I> getCommRouteTable() {
		return this.commRT;
	}
	
	public SuperStepCommand getNextSuperStepCommand() {
		return this.ssc;
	}

	
	public final JobInformation getJobInformation() {
		return this.commRT.getJobInformation();
	}
}
