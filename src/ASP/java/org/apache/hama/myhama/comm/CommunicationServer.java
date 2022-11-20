/**
 * Termite System
 * 
 * copyright 2012-2010
 */
package org.apache.hama.myhama.comm;


import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

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
import org.apache.hama.myhama.api.GraphRecord;
import org.apache.hama.myhama.graph.GraphDataServer;
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
	private ExecutorService msgHandlePool;
	private GraphDataServer<V, W, M, I> graphDataServer;
	
	private volatile Integer mutex = 0;
	private boolean hasNotify = false;
	private SuperStepCommand ssc;
	
	private volatile Boolean semiAsynBarrier = false;
	
	public class PushMsgDataThread implements Callable<Boolean> {
		private GraphRecord<V, W, M, I>[] points;
		private CommunicationServerProtocol<V, W, M, I> comm;
		
		public PushMsgDataThread(CommunicationServerProtocol<V, W, M, I> comm, 
				GraphRecord<V, W, M, I>[] points
				) {
			this.comm = comm;
			this.points = points;
		}
		
		public Boolean call() {
			boolean done = false;
			try {
					Long startTime = System.currentTimeMillis();
					comm.recPoints(this.points, parId);
					done = true;
					LOG.info("send points time is    "+ (System.currentTimeMillis()-startTime));
			} catch (Exception e) {
				LOG.error("fatal unknown error", e);
			}
			
			this.points = null;
			
			return done;
		}
	}
	
	public CommunicationServer (BSPJob job, int parId, TaskAttemptID taskId) 
			throws Exception {
		this.conf = new HamaConfiguration();
		this.jobId = job.getJobID();
		this.parId = parId;
		taskNum = job.getNumBspTask();
		this.msgHandlePool = Executors.newFixedThreadPool(taskNum);
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
	
	public void bindGraphData(GraphDataServer<V, W, M, I> _graphDataServer) {
		this.graphDataServer = _graphDataServer;
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
		this.msgHandlePool.shutdown();
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


	public void sendPoints(int totalOfSend, int startIdx, int migrationMode) {
		this.graphDataServer.setDemarcationPoint(totalOfSend);
		ArrayList<Integer> tableOfSend = new ArrayList<>(Arrays.asList(this.parId+4, this.parId+8, this.parId+12));

		
		InetSocketAddress[] toAddresses = new InetSocketAddress[tableOfSend.size()];
		CommunicationServerProtocol[] comms = new CommunicationServerProtocol[tableOfSend.size()];
		for(int i=0; i<tableOfSend.size(); i++) {
			toAddresses[i] = commRT.getInetSocketAddress(tableOfSend.get(i));
			comms[i] = commRT.getCommServer(toAddresses[i]);
			comms[i].setTotalOfRec(totalOfSend/3);
		}
		int unitOfOneSend = 15000;
		while(totalOfSend > 0) {
			if (totalOfSend<=unitOfOneSend) {
				unitOfOneSend=totalOfSend;
			}
			for (int i = 0; i < tableOfSend.size(); i++) {
				GraphRecord<V, W, M, I>[] points = this.graphDataServer.selectPointsTosend(unitOfOneSend/tableOfSend.size());
				startPushGraphDataThread(comms[i], points); //使用线程池
				LOG.info("destination is --" + tableOfSend.get(i) + "; points num is --" + points.length);
				points = null;
			}
			totalOfSend = totalOfSend-unitOfOneSend;
		}
		//控制阻塞式负载均衡
		if(migrationMode==2) {
			this.msgHandlePool.shutdown();
			try {
				boolean loop = true;
				while(loop) {
					loop = !this.msgHandlePool.awaitTermination(2, TimeUnit.SECONDS);
				}
				
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}


	private void startPushGraphDataThread(CommunicationServerProtocol comm, GraphRecord<V, W, M, I>[] points) {

		Future<Boolean> future = this.msgHandlePool.submit(
				new PushMsgDataThread(comm, points));
		points = null;
	}

	

	@Override
	public void recPoints(GraphRecord<V, W, M, I>[] points, int parId) {
		this.graphDataServer.combinePoints(points);
		points = null;
	}
	
	@Override
	public void setTotalOfRec(int num) {
		this.graphDataServer.setTotalOfRecive(num);
	}
}
