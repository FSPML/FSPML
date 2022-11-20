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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.ipc.WorkerProtocol;

import org.apache.hama.bsp.JobInProgress;

/**
 * A simple task scheduler.
 */
class SimpleTaskScheduler extends TaskScheduler {

	private static final Log LOG = LogFactory.getLog(SimpleTaskScheduler.class);

	public static final String WAIT_QUEUE = "waitQueue";
	public static final String PROCESSING_QUEUE = "processingQueue";
	public static final String FINISHED_QUEUE = "finishedQueue";

	private QueueManager queueManager;
	private volatile boolean initialized;
	private JobListener jobListener;
	private JobProcessor jobProcessor;

	private class JobListener extends JobInProgressListener {
		@Override
		public void jobAdded(JobInProgress job) throws IOException {
			queueManager.initJob(job); // init task

			// lock to WAIT_QUEUE, control(JobProcessor.run()--find(WAIT_QUEUE))
			synchronized (WAIT_QUEUE) {
				queueManager.addJob(WAIT_QUEUE, job);
				queueManager.resortWaitQueue(WAIT_QUEUE);
				LOG.info(job.getJobID() + " is added to the Wait-Queue");
			}
		}

		@Override
		public void jobRemoved(JobInProgress job) throws IOException {
			queueManager.moveJob(PROCESSING_QUEUE, FINISHED_QUEUE, job);
		}
	}

	private class JobProcessor extends Thread implements Schedulable {
		JobProcessor() {
			super("JobProcess");
		}

		/**
		 * Main logic scheduling task to GroomServer(s). Also, it will move
		 * JobInProgress from Wait Queue to Processing Queue.
		 * @throws Exception 
		 */
		public void run() {
			if (false == initialized) {
				throw new IllegalStateException(
						"SimpleTaskScheduler initialization"
								+ " is not yet finished!");
			}
			
			while (initialized) {
				Queue<JobInProgress> queue;
				// lock to WAIT_QUEUE
				synchronized (WAIT_QUEUE) {
					queue = queueManager.findQueue(WAIT_QUEUE);
				}
				if (null == queue) {
					LOG.error(WAIT_QUEUE + " does not exist.");
					throw new NullPointerException(WAIT_QUEUE + " does not exist.");
				}
				// move a job from the wait queue to the processing queue
				JobInProgress j = null;
				try {
					j = queue.removeJob();
				} catch (Exception e) {
					LOG.debug("Fail to get a job from the WAIT_QUEUE," +
							" and will retry again...");
				}
				
				if (j != null) {
					// check wheather the current cluster has enough task slots.
					ClusterStatus cs;
					while (true) {
						try {
							Thread.sleep(2000);
						} catch (Exception e) {
							LOG.debug(e);
						}
						cs = groomServerManager.getClusterStatus(false);
						if (j.getNumBspTask() <= (cs.getMaxClusterTasks() - cs.getCurrentClusterTasks())) {
							break;
						}
					}
					queueManager.addJob(PROCESSING_QUEUE, j);
					LOG.info(j.getJobID() + " is added to the Process-Queue");
					try {
						schedule(j);
					} catch (Exception e) {
						LOG.error("Fail to schedule " + j.getJobID(), e);
					}
				}
			}
		}

		/**
		 * Schedule job to designated GroomServer(s) immediately.
		 * 
		 * @param Targeted GroomServer(s).
		 * @param Job to be scheduled.
		 */
		public void schedule(JobInProgress job) throws Exception {
			int remainTasksLoad = job.getNumBspTask();
			List<JobInProgress> jip_list;
			synchronized (WAIT_QUEUE) {
				jip_list = new ArrayList<JobInProgress>(queueManager.findQueue(WAIT_QUEUE).getJobs());
			}
			for (JobInProgress jip : jip_list) {
				remainTasksLoad += jip.getNumBspTask();
			}
			groomServerManager.setWaitTasks(remainTasksLoad - job.getNumBspTask());
			
			LOG.info("begin to schedule " + job.getJobID());
			// get the new GroomServerStatus Cache
			Collection<GroomServerStatus> glist = groomServerManager.groomServerStatusKeySet();
			TaskInProgress[] tips = job.getTaskInProgress();
			int factor = (int)Math.ceil(tips.length/(double)glist.size());
			
			// cache the assign table, workername : a list of Directive(task)
			HashMap<GroomServerStatus, Directive> assignTable = 
				new HashMap<GroomServerStatus, Directive>();
			LOG.info("#tasks=" + tips.length + " #workers=" + glist.size() + " factor=" + factor);
			int index = 0; boolean over = false;
			for (GroomServerStatus gss: glist) {
				for (int i = 0; i < factor; i++) {
					Task task = tips[index].getTaskToRun(gss);
					LaunchTaskAction action = new LaunchTaskAction(task);
					job.updateWorker(index, task.getTaskID().toString(), gss.getGroomName());
					
					if (assignTable.containsKey(gss)) {
						assignTable.get(gss).addAction(action);
					} else {
						Directive d = new Directive(groomServerManager.currentGroomServerPeers(),
								new ArrayList<GroomServerAction>());
						d.addAction(action);
						assignTable.put(gss, d);
					}
					
					// update the GroomServerStatus cache
					gss.setCurrentTasksCount(gss.getCurrentTasksCount() + 1);
					LOG.info(task.getTaskAttemptId() + " == " + gss.getGroomName());
					index++;
					if (index >= tips.length) {
						over = true;
						break;
					}
				}
				
				if (over) {
					break;
				}
			}
			
			Collection<GroomServerStatus> glist_dist = groomServerManager.groomServerStatusKeySet();
			int maxCounter = 0;
			String name = null;
			for (GroomServerStatus gss: glist_dist) {
				int counter = gss.getCurrentTasksCount();
				if (maxCounter < counter) {
					maxCounter = counter;
					name = gss.getGroomName();
				}
			}
			LOG.info(name + " is assigned " + Integer.toString(maxCounter) + " tasks (max)");
			
			// dispatch tasks to workers
			for (Entry<GroomServerStatus, Directive> e : assignTable.entrySet()) {
				if (job.getStatus().getRunState() == JobStatus.PREP) {
					try {
						WorkerProtocol worker = groomServerManager.findGroomServer(e.getKey());
						long startTime = System.currentTimeMillis();
						worker.dispatch(job.getJobID(), e.getValue());
						long endTime = System.currentTimeMillis();
						LOG.info("schedule to " + e.getKey().getGroomName() 
								+ " cost " + (endTime - startTime) + " ms");
					} catch (IOException ioe) {
						LOG.error("Fail to dispatch tasks to GroomServer "
								+ e.getKey().getGroomName(), ioe);
					}
				}
			}
			
			job.setStartTime();
			job.getStatus().setRunState(JobStatus.LOAD);
		}
	}

	public SimpleTaskScheduler() {
		this.jobListener = new JobListener();
		this.jobProcessor = new JobProcessor();

		//change in version-0.2.4 new function:get the zookeeperaddress in
		// order to connect to zookeeper
		this.conf = new HamaConfiguration();
	}

	@Override
	public void start() {
		this.queueManager = new QueueManager(getConf()); // TODO: need
															// factory?
		this.queueManager.createFCFSQueue(WAIT_QUEUE);
		this.queueManager.createFCFSQueue(PROCESSING_QUEUE);
		this.queueManager.createFCFSQueue(FINISHED_QUEUE);
		groomServerManager.addJobInProgressListener(this.jobListener);
		this.initialized = true;
		this.jobProcessor.start();
	}

	@SuppressWarnings("deprecation")
	@Override
	public void stop() throws Exception {
		this.initialized = false;
		if (null != this.jobListener) {
			groomServerManager.removeJobInProgressListener(this.jobListener);
		}
		this.jobProcessor.stop();
	}

	@Override
	public Collection<JobInProgress> getJobs(String queue) {
		return (queueManager.findQueue(queue)).getJobs();
	}
}
