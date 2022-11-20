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
package org.apache.hama.ipc;

import java.io.IOException;

import org.apache.hama.bsp.BSPJobID;
import org.apache.hama.bsp.GroomServerStatus;
import org.apache.hama.bsp.Directive;
import org.apache.hama.monitor.TaskInformation;
import org.apache.hama.myhama.comm.SuperStepReport;
import org.apache.hama.myhama.util.AggregatorSetOfKmeans;
import org.apache.hama.myhama.util.GradientOfSGD;

/**
 * A new protocol for GroomServers communicate with BSPMaster. This
 * protocol paired with WorkerProtocl, let GroomServers enrol with 
 * BSPMaster, so that BSPMaster can dispatch tasks to GroomServers.
 * һ���µ�����GroomServers��BSPMasterͨ�ŵ�Э�顣��Э����WorkerProtocl��ԣ�
 * ��GroomServers��BSPMasterע�ᣬ����BSPMaster�Ϳ�����GroomServers��������
 */
public interface MasterProtocol extends HamaRPCProtocolVersion {

  /**
   * A GroomServer register with its status to BSPMaster, which will update
   * GroomServers cache.
   *
   * @param status to be updated in cache.
   * @return true if successfully register with BSPMaster; false if fail.
   */
  boolean register(GroomServerStatus status) throws IOException;

  /**
   * A GroomServer (periodically) reports task statuses back to the BSPMaster.
   * @param directive 
   */
  boolean report(Directive directive) throws IOException;

  public String getSystemDir();
  
  /**
   * Build the route table
   * @param jobId
   * @param local
   * @return
   */
  public void buildRouteTable(BSPJobID jobId, TaskInformation local);
  
  /**
   * Once the task has run successfully, it will register to the JobInProgress
   * and report some information.
   * 
   * Once the function is invoked, that means the task has run successfully and
   * finished loading data.
   * 
   * @author 
   */
  public void registerTask(BSPJobID jobId, TaskInformation statis);
  
  /**
   * Make sure than all tasks have completed the preparation work 
   * before beginning a new superstep.
   * 
   * @param jobId
   * @param parId
   * @return
   */
  public void beginSuperStep(BSPJobID jobId, int parId);
  
  /**
   * Report local information after finishing the current superstep, 
   * and then get the {@link SuperStepCommand} for the next superstep.
   * 
   * @author 
   * @param jobId
   * @param parId
   * @param SuperStepReport ssr
   */
  public void finishSuperStep(BSPJobID jobId, int parId, SuperStepReport ssr, 
		  double sgl, double vhd);
  
  /**
   * Have saved the local results onto the distributed file system, such as HDFS.
   * @param jobId
   * @param parId
   * @param saveRecordNum
   */
  public void saveResultOver(BSPJobID jobId, int parId, int saveRecordNum);
  
  /**
   * Just synchronize, do nothing.
   * @param jobId
   * @param parId
   */
  public void sync(BSPJobID jobId, int parId);
  
  /**
   * Synchronize the archiving operation used in checkpoint.
   * @param jobId
   * @param parId
   * @param arcNum number of archived vertices
   */
  public void syncArchiveData(BSPJobID jobId, int parId, int arcNum);
  
  /**
   * Synchronize the loading operation used in checkpoint
   * @param jobId
   * @param parId
   * @param loadNum number of loaded vertices
   */
  public void syncLoadData(BSPJobID jobId, int parId, int loadNum);
  
  
  public void reportAggregators(BSPJobID jobId, int parId, 
		  AggregatorSetOfKmeans _ask, double taskAgg, double sgl, double vhd);

  public void reportSGDAggregators(BSPJobID jobId, int parId, GradientOfSGD gradSet, double taskAgg, double sgl, double vhd);
}
