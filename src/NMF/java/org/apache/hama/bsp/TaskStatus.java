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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hama.Constants.TaskState;
import org.apache.hama.Constants.TaskPhase;

/**
 * Describes the current status of a task. This is not intended to be a
 * comprehensive piece of data.
 */
public class TaskStatus implements Writable, Cloneable {
  static final Log LOG = LogFactory.getLog(TaskStatus.class);

  private BSPJobID jobId;
  private TaskAttemptID taskId;
  
  private double progress;
  private double usedMem;
  private double totalMem;
  
  private volatile TaskState runState;
  private String stateString;
  private String groomServer;
  private int superstepCounter;

  private long startTime;
  private long finishTime;

  private volatile TaskPhase phase = TaskPhase.STARTING;

  /**
   * 
   */
  public TaskStatus() {
    jobId = new BSPJobID();
    taskId = new TaskAttemptID();
    this.superstepCounter = 0;
  }

  public TaskStatus(BSPJobID jobId, TaskAttemptID taskId, float progress,
      TaskState runState, String stateString, String groomServer, TaskPhase phase) {
    this.jobId = jobId;
    this.taskId = taskId;
    this.progress = progress;
    this.runState = runState;
    this.stateString = stateString;
    this.groomServer = groomServer;
    this.phase = phase;
    this.superstepCounter = 0;
  }

  // //////////////////////////////////////////////////
  // Accessors and Modifiers
  // //////////////////////////////////////////////////

  public BSPJobID getJobId() {
    return jobId;
  }

  public TaskAttemptID getTaskId() {
    return taskId;
  }

  public double getProgress() {
    return progress;
  }

  public void setProgress(double progress) {
    this.progress = progress;
  }
  
  	public double getUsedMemory() {
		return usedMem;
	}

	public void setUsedMemory(double usedMemory) {
		this.usedMem = usedMemory;
	}

	public double getTotalMemory() {
		return totalMem;
	}

	public void setTotalMemory(double totalMemory) {
		this.totalMem = totalMemory;
	}

  public void setSuperStepCounter(int _superStepCounter) {
	  this.superstepCounter = _superStepCounter;
  }
  
  public int getSuperStepCounter() {
	  return this.superstepCounter;
  }
  
  public TaskState getRunState() {
    return runState;
  }

  public synchronized void setRunState(TaskState state) {
    this.runState = state;
  }

  public String getStateString() {
    return stateString;
  }

  public void setStateString(String stateString) {
    this.stateString = stateString;
  }

  public String getGroomServer() {
    return groomServer;
  }

  public void setGroomServer(String groomServer) {
    this.groomServer = groomServer;
  }

  public long getFinishTime() {
    return finishTime;
  }

  void setFinishTime(long finishTime) {
    this.finishTime = finishTime;
  }

  /**
   * Get start time of the task.
   * 
   * @return 0 is start time is not set, else returns start time.
   */
  public long getStartTime() {
    return startTime;
  }

  /**
   * Set startTime of the task.
   * 
   * @param startTime start time
   */
  void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  /**
   * Get current phase of this task.
   * 
   * @return .
   */
  public TaskPhase getPhase() {
    return this.phase;
  }

  /**
   * Set current phase of this task.
   * 
   * @param phase phase of this task
   */
  void setPhase(TaskPhase phase) {
    this.phase = phase;
  }

  /**
   * Update the status of the task.
   * 
   * This update is done by ping thread before sending the status.
   * 
   * @param progress
   * @param state
   * @param counters
   */
  synchronized void statusUpdate(float progress, String state) {
    setProgress(progress);
    setStateString(state);
  }

  /**
   * Update the status of the task.
   * 
   * @param status updated status
   */
  synchronized void statusUpdate(TaskStatus status) {
    this.progress = status.getProgress();
    this.usedMem = status.getUsedMemory();
    this.totalMem = status.getTotalMemory();
    this.runState = status.getRunState();
    this.stateString = status.getStateString();

    if (status.getStartTime() != 0) {
      this.startTime = status.getStartTime();
    }
    if (status.getFinishTime() != 0) {
      this.finishTime = status.getFinishTime();
    }

    this.phase = status.getPhase();
  }

  /**
   * Update specific fields of task status
   * 
   * This update is done in BSPMaster when a cleanup attempt of task reports its
   * status. Then update only specific fields, not all.
   * 
   * @param runState
   * @param progress
   * @param state
   * @param phase
   * @param finishTime
   */
  synchronized void statusUpdate(TaskState runState, float progress, String state,
      TaskPhase phase, long finishTime) {
    setRunState(runState);
    setProgress(progress);
    setStateString(state);
    setPhase(phase);
    if (finishTime != 0) {
      this.finishTime = finishTime;
    }
  }

  /**
   * Increments the number of BSP super steps executed by the task.
   */
  public void incrementSuperstepCount() {
    superstepCounter += 1;
  }

  @Override
  public Object clone() {
    try {
      return super.clone();
    } catch (CloneNotSupportedException cnse) {
      // Shouldn't happen since we do implement Clonable
      throw new InternalError(cnse.toString());
    }
  }

  // ////////////////////////////////////////////
  // Writable
  // ////////////////////////////////////////////

  @Override
  public void readFields(DataInput in) throws IOException {
    this.jobId.readFields(in);
    this.taskId.readFields(in);
    
    this.progress = in.readDouble();
    this.usedMem = in.readDouble();
    this.totalMem = in.readDouble();
    
    this.runState = WritableUtils.readEnum(in, TaskState.class);
    this.stateString = Text.readString(in);
    this.groomServer = Text.readString(in);
    this.phase = WritableUtils.readEnum(in, TaskPhase.class);
    this.startTime = in.readLong();
    this.finishTime = in.readLong();
    this.superstepCounter = in.readInt();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    jobId.write(out);
    taskId.write(out);
    
    out.writeDouble(progress);
    out.writeDouble(this.usedMem);
    out.writeDouble(this.totalMem);
    
    WritableUtils.writeEnum(out, runState);
    Text.writeString(out, stateString);
    Text.writeString(out, this.groomServer);
    WritableUtils.writeEnum(out, phase);
    out.writeLong(startTime);
    out.writeLong(finishTime);
    out.writeInt(superstepCounter);
  }
}
