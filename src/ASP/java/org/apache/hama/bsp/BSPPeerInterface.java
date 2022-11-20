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

import java.io.Closeable;

import org.apache.hama.myhama.util.TaskReportContainer;

/**
 * BSP communication interface.
 */
public interface BSPPeerInterface extends BSPRPCProtocolVersion, Closeable {

  /**
   * @return the count of current super-step
   */
  public long getSuperstepCount(BSPJobID jobId, TaskAttemptID taskId);
 

  /**
   * Clears all queues entries.
   */
  public void clear(BSPJobID jobId, TaskAttemptID taskId);
  
  
  public void reportProgress(BSPJobID jobId, TaskAttemptID taskId, TaskReportContainer taskReportContainer);
  
  public void reportException(BSPJobID jobId, TaskAttemptID taskId, Exception e);
  
  /**
   * in version-0.2.7
   * @return true if the increasement of superstep is success(+1, if the two message queues are empty)
   */
  public boolean increaseSuperStep(BSPJobID jobId, TaskAttemptID taskId);
  
  /**
   * Get the number of tasks that run on the same machine.
   * 
   * @return
   */
  public int getLocalTaskNumber(BSPJobID jobId);
}
