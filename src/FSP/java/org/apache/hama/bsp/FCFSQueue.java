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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hama.bsp.JobInProgress;

class FCFSQueue implements Queue<JobInProgress> {

  public static final Log LOG = LogFactory.getLog(FCFSQueue.class);
  private final String name;
  private BlockingQueue<JobInProgress> queue = new LinkedBlockingQueue<JobInProgress>();
  //change in version-0.2.4 new variable
  private List<JobInProgress> resort_tmp=new ArrayList<JobInProgress>();

  public FCFSQueue(String name) {
    this.name = name;
  }

  @Override
  public String getName() {
    return this.name;
  }

  @Override
  public void addJob(JobInProgress job) {
    try {
      queue.put(job);
    } catch (InterruptedException ie) {
      LOG.error("Fail to add a job to the " + this.name + " queue.", ie);
    }
  }
  
  @Override
  //change in version-0.2.4 new function: resort the queue
  public void resortQueue(){
	  Comparator<JobInProgress> comp = new Comparator<JobInProgress>() {
	      public int compare(JobInProgress o1, JobInProgress o2) {
	        int res = o1.getPriority().compareTo(o2.getPriority());
	        if(res == 0) {
	          if(o1.getStartTime() < o2.getStartTime())
	            res = -1;
	          else
	            res = (o1.getStartTime()==o2.getStartTime() ? 0 : 1);
	        }
	          
	        return res;
	     }
	  };
	    
	  synchronized (queue) {
		  try{
			  resort_tmp.clear();
			  int wait_count=queue.size();
			  int i=0;
			  for(i=0;i<wait_count;i++)
				  resort_tmp.add(queue.take());
			  
			  Collections.sort(resort_tmp, comp);
			  for(i=0;i<wait_count;i++)
				  queue.put(resort_tmp.get(i));
		  }catch(Exception e){
			  LOG.error("resort error: "+e.toString());
		  }
	  }
  }
  

  @Override
  public void removeJob(JobInProgress job) {
    queue.remove(job);
  }

  @Override
  public JobInProgress removeJob() throws Exception {
    try {
      return queue.take();
    } catch (InterruptedException ie) {
      LOG.error("Fail to remove a job from the " + this.name + " queue.", ie);
    }
    return null;
  }

  @Override
  public Collection<JobInProgress> getJobs() {
    return queue;
  }

}
