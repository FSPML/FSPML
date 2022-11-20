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
import java.text.NumberFormat;

/**
 * TaskID represents the immutable and unique identifier for a BSP Task.
 */
public class TaskID extends ID {
  protected static final String TASK = "task";
  protected static final NumberFormat idFormat = NumberFormat.getInstance();
  static {
    idFormat.setGroupingUsed(false);
    idFormat.setMinimumIntegerDigits(6);
  }

  private BSPJobID jobId;

  public TaskID(BSPJobID jobId, int id) {
    super(id);
    if (jobId == null) {
      throw new IllegalArgumentException("jobId cannot be null");
    }
    this.jobId = jobId;
  }

  public TaskID(String jtIdentifier, int jobId, int id) {
    this(new BSPJobID(jtIdentifier, jobId), id);
  }

  public TaskID() {
    jobId = new BSPJobID();
  }

  /** Returns the {@link BSPJobID} object that this tip belongs to */
  public BSPJobID getJobID() {
    return jobId;
  }

  @Override
  public boolean equals(Object o) {
    if (!super.equals(o))
      return false;

    TaskID that = (TaskID) o;
    return this.jobId.equals(that.jobId);
  }

  @Override
  public int compareTo(ID o) {
    TaskID that = (TaskID) o;
    int jobComp = this.jobId.compareTo(that.jobId);
    if (jobComp == 0) {
      return this.id - that.id;
    } else {
      return jobComp;
    }
  }

  @Override
  public String toString() {
    return appendTo(new StringBuilder(TASK)).toString();
  }

  protected StringBuilder appendTo(StringBuilder builder) {
    return jobId.appendTo(builder).append(SEPARATOR)
        .append(idFormat.format(id));
  }

  @Override
  public int hashCode() {
    return jobId.hashCode() * 524287 + id;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    jobId.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    jobId.write(out);
  }

  public static TaskID forName(String str) throws IllegalArgumentException {
    if (str == null)
      return null;
    try {
      String[] parts = str.split("_");
      if (parts.length == 5) {
        if (parts[0].equals(TASK)) {
          return new TaskID(parts[1], Integer.parseInt(parts[2]), Integer
              .parseInt(parts[4]));
        }
      }
    } catch (Exception ex) {
    }
    throw new IllegalArgumentException("TaskId string : " + str
        + " is not properly formed");
  }
}
