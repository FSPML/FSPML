/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hama.bsp;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hama.Constants;
import org.apache.hama.myhama.util.TaskReportContainer;

/**
 * This class represents a BSP peer. 
 */
public class BSPPeer implements BSPPeerInterface {
	public static final Log LOG = LogFactory.getLog(BSPPeer.class);
  private InetSocketAddress peerAddress;
  private TaskStatus currentTaskStatus;

  /**
   * Constructor
   */
  public BSPPeer(Configuration conf) throws IOException {

    String bindAddress = conf.get(Constants.PEER_HOST,
        Constants.DEFAULT_PEER_HOST);
    
    int bindPort = conf
        .getInt(Constants.PEER_PORT, Constants.DEFAULT_PEER_PORT);
    peerAddress = new InetSocketAddress(bindAddress, bindPort);
  }

  public boolean increaseSuperStep(BSPJobID jobId, TaskAttemptID taskId){
	  return true;
  }


  public void clear(BSPJobID jobId, TaskAttemptID taskId) {

  }

  @Override
  public void close() throws IOException {

  }
  
  @Override
  public long getProtocolVersion(String arg0, long arg1) throws IOException {
    return BSPPeerInterface.versionID;
  }

  /**
   * @return the string as host:port of this Peer
   */
  public String getPeerName(BSPJobID jobId, TaskAttemptID taskId) {
    return peerAddress.getHostName() + ":" + peerAddress.getPort();
  }

  /**
   * Sets the current status
   * 
   * @param currentTaskStatus
   */
  public void setCurrentTaskStatus(TaskStatus currentTaskStatus) {
    this.currentTaskStatus = currentTaskStatus;
  }

  /**
   * @return the count of current super-step
   */
  public long getSuperstepCount(BSPJobID jobId, TaskAttemptID taskId) {
    return currentTaskStatus.getSuperStepCounter();
  }

  /**
   * Sets the job configuration
   * 
   * @param jobConf
   */
  public void setJobConf(BSPJob jobConf) {
	  
  }

@Override
public int getLocalTaskNumber(BSPJobID jobId) {
	// TODO Auto-generated method stub
	return 0;
}

@Override
public void reportProgress(BSPJobID jobId, TaskAttemptID taskId,
		TaskReportContainer taskReportContainer) {
	// TODO Auto-generated method stub
	
}

@Override
public void reportException(BSPJobID jobId, TaskAttemptID taskId, Exception e) {
	LOG.error("BSPPeer");
}

}
