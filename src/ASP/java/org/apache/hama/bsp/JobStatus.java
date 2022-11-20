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
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;

/**
 * Describes the current status of a job.
 */
public class JobStatus implements Writable, Cloneable {
  public static final Log LOG = LogFactory.getLog(JobStatus.class);
  
  static {
    WritableFactories.setFactory(JobStatus.class, new WritableFactory() {
      public Writable newInstance() {
        return new JobStatus();
      }
    });
  }

  public static enum State{
	PREP(1),
	LOAD(2),
    RUNNING(3),
    SAVE(4),
    SUCCEEDED(5),
    FAILED(6),
    KILLED(7);
    int s;
    State(int s){
      this.s = s;
    }
    public int value(){
      return this.s;
    }
  }

  public static final int PREP = 1;
  public static final int LOAD = 2;
  public static final int RUNNING = 3;
  public static final int SAVE = 4;
  public static final int SUCCEEDED = 5;
  public static final int FAILED = 6;
  public static final int KILLED = 7;

  private BSPJobID jobid;
  private float[] progress;  // min & max
  private int[] progressTaskIds; //min & max
  private int superstepCounter;
  private int totalSuperStep;
  private int runState;
  private String schedulingInfo = "NA";
  private String user;
  private int taskNum;

  private long submitTime;
  private long startTime;
  private long currentTime;
  private long finishTime;
  
  private HashMap<TaskAttemptID, TaskStatus> taskStatuses;
  
  public JobStatus() {
	  this.progress = new float[2];
	  this.progressTaskIds = new int[2];
	  this.taskStatuses = new HashMap<TaskAttemptID, TaskStatus>();
  }

  /**
   * Create an object to maintaince the information of one job.
   * @param jobid
   * @param user
   * @param progress
   * @param superStepCounter
   * @param runState
   */
  public JobStatus(BSPJobID jobid, String user, float[] progress, int[] progressTaskIds,
      int superStepCounter, int runState) {
    this.jobid = jobid;
    this.progress = progress;
    this.progressTaskIds = progressTaskIds;
    this.superstepCounter = superStepCounter;
    this.runState = runState;
    this.user = user;
    this.taskStatuses = new HashMap<TaskAttemptID, TaskStatus>();
  }
  
  public void updateTaskStatus(TaskAttemptID taskId, TaskStatus taskStatus) {
	  this.taskStatuses.put(taskId, taskStatus);
  }
  
  public HashMap<TaskAttemptID, TaskStatus> getTaskStatuses() {
	  return this.taskStatuses;
  }
  
  public BSPJobID getJobID() {
    return jobid;
  }

  public float[] getProgress() {
    return this.progress;
  }
  
  public int[] getProgressTaskIds() {
	  return this.progressTaskIds;
  }

  public void setProgress(float[] p, int[] ids) {
    this.progress = p;
    this.progressTaskIds = ids;
  }

  public synchronized int getRunState() {
    return runState;
  }

  public synchronized void setRunState(int state) {
    this.runState = state;
  }

  public synchronized void setSuperStepCounter(int _superStepCounter) {
	  this.superstepCounter = _superStepCounter;
  }
  
  public synchronized int getSuperstepCounter() {
    return superstepCounter;
  }
  
  public void setTotalSuperStep(int totalSuperStep) {
	  this.totalSuperStep = totalSuperStep;
  }
  
  public int getTotalSuperStep() {
	  return this.totalSuperStep;
  }
  
  public synchronized void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  public synchronized long getStartTime() {
    return startTime;
  }
  
  public long getSubmitTime() {
	  return this.submitTime;
  }
  
  public void setSubmitTime(long submitTime) {
	  this.submitTime = submitTime;
  }
  
  public void setCurrentTime(long currentTime) {
	  this.currentTime = currentTime;
  }
  
  public long getCurrentTime() {
	  return this.currentTime;
  }
  
  public void setFinishTime(long finishTime) {
    this.finishTime = finishTime;
  }
  
  /**
   * Get the finish time of the job.
   */
  public long getFinishTime() { 
    return finishTime;
  }
  
  public float getRunCostTime() {
	  if (this.finishTime == 0L) {
		  this.finishTime = System.currentTimeMillis();
	  }
	  
	  return (this.finishTime - this.startTime) / 1000.0f;
  }
  
  /**
   * @param user The username of the job
   */
  public void setUsername(String user) {
    this.user = user;
  }

  /**
   * @return the username of the job
   */
  public synchronized String getUsername() {
    return user;
  }

  @Override
  public Object clone() {
    try {
      return super.clone();
    } catch (CloneNotSupportedException cnse) {
      throw new InternalError(cnse.toString());
    }
  }

  public synchronized String getSchedulingInfo() {
    return schedulingInfo;
  }

  public synchronized void setSchedulingInfo(String schedulingInfo) {
    this.schedulingInfo = schedulingInfo;
  }

  public synchronized boolean isJobComplete() {
    return (runState == JobStatus.SUCCEEDED || runState == JobStatus.FAILED || runState == JobStatus.KILLED);
  }

  public synchronized void write(DataOutput out) throws IOException {
    jobid.write(out);
    out.writeFloat(progress[0]);
    out.writeFloat(progress[1]);
    out.writeInt(this.progressTaskIds[0]);
    out.writeInt(this.progressTaskIds[1]);
    out.writeInt(runState);
    out.writeLong(submitTime);
    out.writeLong(startTime);
    out.writeLong(currentTime);
    out.writeLong(finishTime);
    Text.writeString(out, user);
    Text.writeString(out, schedulingInfo);
    out.writeInt(superstepCounter);
    out.writeInt(this.totalSuperStep);
    out.writeInt(this.taskNum);
    
    int num = this.taskStatuses.size();
    out.writeInt(num);
    for (Entry<TaskAttemptID, TaskStatus> entry: this.taskStatuses.entrySet()) {
    	entry.getKey().write(out);
    	entry.getValue().write(out);
    }
  }

  public synchronized void readFields(DataInput in) throws IOException {  
	this.jobid = new BSPJobID();
    jobid.readFields(in);
    this.progress[0] = in.readFloat();
    this.progress[1] = in.readFloat();
    this.progressTaskIds[0] = in.readInt();
    this.progressTaskIds[1] = in.readInt();
    this.runState = in.readInt();
    this.submitTime = in.readLong();
    this.startTime = in.readLong();
    this.currentTime = in.readLong();
    this.finishTime = in.readLong();
    this.user = Text.readString(in);
    this.schedulingInfo = Text.readString(in);
    this.superstepCounter = in.readInt();
    this.totalSuperStep = in.readInt();
    this.taskNum = in.readInt();
    
    int num = in.readInt();
    for (int i = 0; i < num; i++) {
    	TaskAttemptID taskId = new TaskAttemptID();
    	TaskStatus taskStatus = new TaskStatus();
    	taskId.readFields(in);
    	taskStatus.readFields(in);
    	this.taskStatuses.put(taskId, taskStatus);
    }
  }

public int getTaskNum() {
	return taskNum;
}

public void setTaskNum(int taskNum) {
	this.taskNum = taskNum;
}
}
