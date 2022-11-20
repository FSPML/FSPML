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
import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.ipc.JobSubmissionProtocol;
import org.apache.hama.ipc.MasterProtocol;
import org.apache.hama.ipc.WorkerProtocol;
import org.apache.hama.monitor.TaskInformation;
import org.apache.hama.myhama.comm.SuperStepReport;
import org.apache.hama.myhama.util.AggregatorSetOfKmeans;
import org.apache.hama.myhama.util.GradientOfSGD;
import org.apache.hama.Constants;
import org.apache.hama.Constants.TaskState;

/**
 * BSPMaster is responsible to control all the groom servers and to manage bsp
 * jobs.
 */
public class BSPMaster implements JobSubmissionProtocol, MasterProtocol, // InterServerProtocol,
    GroomServerManager {
  public static final Log LOG = LogFactory.getLog(BSPMaster.class);

  private HamaConfiguration conf;

  // Constants
  public static enum State {
    INITIALIZING, RUNNING
  }

  private static final int FS_ACCESS_RETRY_PERIOD = 10000;
  public static final long GROOMSERVER_EXPIRY_INTERVAL = 10 * 60 * 1000;
  static long JOBINIT_SLEEP_INTERVAL = 2000;

  // States
  State state = State.INITIALIZING;
  
  // Attributes
  String masterIdentifier;
  // private Server interServer;
  private Server masterServer;

  // Filesystem
  static final String SUBDIR = "bspMaster";
  FileSystem fs = null;
  Path systemDir = null;
  Path ckDir = null;
  // system directories are world-wide readable and owner readable
  final static FsPermission SYSTEM_DIR_PERMISSION = FsPermission
      .createImmutable((short) 0733); // rwx-wx-wx
  // system files should have 700 permission
  final static FsPermission SYSTEM_FILE_PERMISSION = FsPermission
      .createImmutable((short) 0700); // rwx------

  // Jobs' Meta Data
  private Integer nextJobId = Integer.valueOf(1);
  // private long startTime;
  private int totalSubmissions = 0; // how many jobs has been submitted by
  // clients
  private int currentClusterTasks = 0; // currnetly running tasks
  private int maxClusterTasks=0; // max tasks that the Cluster can run
  private int waitTasks=0;//tasks wait to execute

  private Map<BSPJobID, JobInProgress> jobs = 
	  new TreeMap<BSPJobID, JobInProgress>();
  private TaskScheduler taskScheduler;

  // GroomServers cache
  public volatile ConcurrentMap<GroomServerStatus, WorkerProtocol> GroomServers = 
	  new ConcurrentHashMap<GroomServerStatus, WorkerProtocol>();

  private final List<JobInProgressListener> jobInProgressListeners = 
	  new CopyOnWriteArrayList<JobInProgressListener>();
  
  private CheckTimeOut cto;
  private static long HEART_BEAT_TIMEOUT = 30000;
  /**
   * This thread will run until stopping the cluster. It will check weather
   * the heart beat interval is time-out or not.
   * 
   * @author
   * 
   */
  public class CheckTimeOut extends Thread {
      public void run() {
    	  LOG.info("The threshold of time out about heart beat is: " 
    			  + HEART_BEAT_TIMEOUT);
          while (true) {
              long nowTime = System.currentTimeMillis();
              Iterator<Entry<GroomServerStatus, WorkerProtocol>> ite = 
            	  GroomServers.entrySet().iterator();
              while (ite.hasNext()) {
            	  Entry<GroomServerStatus, WorkerProtocol> e = ite.next();
                  long timeout = nowTime - e.getKey().getLastSeen();
                  if (timeout > HEART_BEAT_TIMEOUT) {
                      LOG.error("[Fault Detective] The worker's time out is catched: " 
                    		  + e.getKey().getGroomName());
                      LOG.error("try to kill jobs on worker=" + e.getKey().getGroomName());
                      List<TaskStatus> tlist = e.getKey().getTaskReports();
        			  for (TaskStatus ts : tlist) {
        				  try {
        					  JobInProgress jip = whichJob(ts.getJobId());
        					  jip.failedJob();
        					  LOG.error("kill job=" + jip.getJobID());
        				  } catch (Exception eof) {
        					  LOG.error("[failedJob]", eof);
        				  }
        			  }
        			  
        			  LOG.error("delete this worker: " + e.getKey().getGroomName());
        			  ite.remove();
                  }
              }

              try {
                  Thread.sleep(HEART_BEAT_TIMEOUT);
              } catch (Exception e) {
                  LOG.error("[CheckTimeOut]", e);
              }
          }
      }
  }
  
  /**
   * Start the BSPMaster process, listen on the indicated hostname/port
   */
  public BSPMaster(HamaConfiguration conf) throws IOException,
      InterruptedException {
    this(conf, generateNewIdentifier());
  }

  BSPMaster(HamaConfiguration conf, String identifier) throws IOException,
      InterruptedException {
    this.conf = conf;
    this.masterIdentifier = identifier;
    // expireLaunchingTaskThread.start();

    // Create the scheduler and init scheduler services
    Class<? extends TaskScheduler> schedulerClass = conf.getClass(
        "bsp.master.taskscheduler", SimpleTaskScheduler.class,
        TaskScheduler.class);
    this.taskScheduler = (TaskScheduler) ReflectionUtils.newInstance(
        schedulerClass, conf);

    String host = getAddress(conf).getHostName();
    int port = getAddress(conf).getPort();
    LOG.info("RPC BSPMaster: host " + host + " port " + port);
    this.masterServer = RPC.getServer(this, host, port, conf);

    while (!Thread.currentThread().isInterrupted()) {
      try {
        if (fs == null) {
          fs = FileSystem.get(conf);
        }
        // clean up the system dir, which will only work if hdfs is out of
        // safe mode
        if (systemDir == null) {
          systemDir = new Path(getSystemDir());
        }
        if (ckDir == null) {
        	ckDir = new Path(getCheckPointDir());
        }

        fs.delete(systemDir, true);
        fs.delete(ckDir, true);
        LOG.info("Cleanup the system and checkpoint directory");
        if (fs.mkdirs(systemDir) && fs.mkdirs(ckDir)) {
            LOG.info("system dir is OK, " + systemDir);
            LOG.info("checkpoint dir is OK," + ckDir);
          break;
        }
        LOG.error("Mkdirs failed to create " + systemDir);
        LOG.info(SUBDIR);

      } catch (AccessControlException ace) {
        LOG.warn("Failed to operate on bsp.system.dir (" + systemDir
            + ") because of permissions.");
        LOG.warn("Manually delete the bsp.system.dir (" + systemDir
            + ") and then start the BSPMaster.");
        LOG.warn("Bailing out ... ");
        throw ace;
      } catch (IOException ie) {
        LOG.info("problem cleaning system directory: " + systemDir, ie);
      }
      Thread.sleep(FS_ACCESS_RETRY_PERIOD);
    }

    if (Thread.currentThread().isInterrupted()) {
      throw new InterruptedException();
    }

    deleteLocalFiles(SUBDIR);
  }

  /**
   * A GroomServer registers with its status to BSPMaster when startup, which
   * will update GroomServers cache.
   * 
   * @param status to be updated in cache.
   * @return true if registering successfully; false if fail.
   */
  @Override
  public boolean register(GroomServerStatus status) throws IOException {
    if (null == status) {
      LOG.error("No groom server status.");
      throw new NullPointerException("No groom server status.");
    }
    Throwable e = null;
    try {
      WorkerProtocol wc = (WorkerProtocol) RPC.waitForProxy(
          WorkerProtocol.class, WorkerProtocol.versionID,
          resolveWorkerAddress(status.getRpcServer()), this.conf);
      if (null == wc) {
        LOG.warn("Fail to create Worker client at host " + status.getPeerName());
        return false;
      }
      // TODO: need to check if peer name has changed
      //add the maxClusterTasks if find new GroomServer
      if(!GroomServers.containsKey(status)){
    	  maxClusterTasks+=status.getMaxTasksCount();
    	  LOG.info(status.getGroomName() + " register and maxTasks is " 
    			  + maxClusterTasks);
      }
      GroomServers.putIfAbsent(status, wc);
    } catch (UnsupportedOperationException u) {
      e = u;
    } catch (ClassCastException c) {
      e = c;
    } catch (NullPointerException n) {
      e = n;
    } catch (IllegalArgumentException i) {
      e = i;
    } catch (Exception ex) {
      e = ex;
    }

    if (null != e) {
      LOG.error("Fail to register GroomServer " + status.getGroomName(), e);
      return false;
    }

    return true;
  }

  private static InetSocketAddress resolveWorkerAddress(String data) {
    return new InetSocketAddress(data.split(":")[0], Integer.parseInt(data
        .split(":")[1]));
  }

  public void updateGroomServersKey(GroomServerStatus old,
      GroomServerStatus newKey) {
    synchronized (GroomServers) {
      WorkerProtocol worker = GroomServers.remove(old);
      GroomServers.put(newKey, worker);
    }
  }
  
  

  @Override
  public boolean report(Directive directive) throws IOException {
	  if (directive.getType().value() != Directive.Type.Response.value()) {
		  throw new IllegalStateException("GroomServer should report()"
				  + " with Response. Current report type:" + directive.getType());
	  }
	  
	  // update GroomServerStatus hold in groomServers cache.
	  GroomServerStatus fstus = directive.getStatus();
	  if (GroomServers.containsKey(fstus)) {
		  GroomServerStatus ustus = null;
		  for (GroomServerStatus old : GroomServers.keySet()) {
			  if (old.equals(fstus)) {
				  ustus = fstus;
				  ustus.setLastSeen(System.currentTimeMillis());
				  updateGroomServersKey(old, ustus);
				  break;
			  }
		  }
		  if (null != ustus) {
			  List<TaskStatus> tlist = ustus.getTaskReports();
			  for (TaskStatus ts : tlist) {
				  this.jobs.get(ts.getJobId()).updateTaskStatus(ts.getTaskId(), ts);
				  JobInProgress jip = whichJob(ts.getJobId());
				  switch(ts.getRunState()) {
				  case RUNNING:
					  if (jip != null && 
							  (jip.getStatus().getRunState() == JobStatus.KILLED 
									  || jip.getStatus().getRunState() == JobStatus.FAILED)) {
						  WorkerProtocol worker = findGroomServer(ustus);
						  KillTaskAction action = new KillTaskAction(ts.getTaskId());
						  Directive d = new Directive(currentGroomServerPeers(),
								  new ArrayList<GroomServerAction>());
						  d.addAction(action);
						  worker.dispatch(jip.getJobID(), d);  
					  }
					  break;
				  case SUCCEEDED:break;
				  case KILLED:break;
				  case FAILED:break;
				  }
				  
				  
			  }
		  } else {
			  throw new RuntimeException("BSPMaster contains GroomServerSatus, "
					  + "but fail to retrieve it.");
		  }
	  }
	  return true;
  }

  private JobInProgress whichJob(BSPJobID id) {
    for (JobInProgress job : taskScheduler
        .getJobs(SimpleTaskScheduler.PROCESSING_QUEUE)) {
      if (job.getJobID().equals(id)) {
        return job;
      }
    }
    return null;
  }

  // /////////////////////////////////////////////////////////////
  // BSPMaster methods
  // /////////////////////////////////////////////////////////////

  // Get the job directory in system directory
  Path getSystemDirectoryForJob(BSPJobID id) {
    return new Path(getSystemDir(), id.toString());
  }
  
  Path getCheckPointDirectoryForJob(BSPJobID id) {
	    return new Path(getCheckPointDir(), id.toString());
  }

  String[] getLocalDirs() throws IOException {
    return conf.getStrings("bsp.local.dir");
  }

  void deleteLocalFiles() throws IOException {
    String[] localDirs = getLocalDirs();
    for (int i = 0; i < localDirs.length; i++) {
      FileSystem.getLocal(conf).delete(new Path(localDirs[i]), true);
    }
  }

  void deleteLocalFiles(String subdir) throws IOException {
    try {
      String[] localDirs = getLocalDirs();
      for (int i = 0; i < localDirs.length; i++) {
        FileSystem.getLocal(conf).delete(new Path(localDirs[i], subdir), true);
      }
    } catch (NullPointerException e) {
      LOG.info(e);
    }
  }

  /**
   * Constructs a local file name. Files are distributed among configured local
   * directories.
   */
  Path getLocalPath(String pathString) throws IOException {
    return conf.getLocalPath("bsp.local.dir", pathString);
  }

  public static BSPMaster startMaster(HamaConfiguration conf)
      throws IOException, InterruptedException {
    return startMaster(conf, generateNewIdentifier());
  }

  public static BSPMaster startMaster(HamaConfiguration conf, String identifier)
      throws IOException, InterruptedException {

    BSPMaster result = new BSPMaster(conf, identifier);
    result.taskScheduler.setGroomServerManager(result);
    result.taskScheduler.start();

    return result;
  }

  public static InetSocketAddress getAddress(Configuration conf) {
    String hamaMasterStr = conf.get("bsp.master.address", "localhost");
    int defaultPort = conf.getInt("bsp.master.port", 40000);

    return NetUtils.createSocketAddr(hamaMasterStr, defaultPort);
  }

  /**
   * BSPMaster identifier
   * 
   * @return String BSPMaster identification number
   */
  private static String generateNewIdentifier() {
    return new SimpleDateFormat("yyyyMMddHHmm").format(new Date());
  }

  public void offerService() throws InterruptedException, IOException {
    // this.interServer.start();
    this.masterServer.start();
    
    this.cto = new CheckTimeOut();
    this.cto.start();
    synchronized (this) {
      state = State.RUNNING;
    }
    LOG.info("Starting RUNNING");

    // this.interServer.join();
    this.masterServer.join();

    LOG.info("Stopped RPC Master server.");
  }

  // //////////////////////////////////////////////////
  // InterServerProtocol
  // //////////////////////////////////////////////////
  @Override
  public long getProtocolVersion(String protocol, long clientVersion)
      throws IOException {
    if (protocol.equals(MasterProtocol.class.getName())) {
      return MasterProtocol.versionID;
    } else if (protocol.equals(JobSubmissionProtocol.class.getName())) {
      return JobSubmissionProtocol.versionID;
    } else {
      throw new IOException("Unknown protocol to BSPMaster: " + protocol);
    }
  }

  // //////////////////////////////////////////////////
  // JobSubmissionProtocol
  // //////////////////////////////////////////////////
  /**
   * This method returns new job id. The returned job id increases sequentially.
   */
  @Override
  public BSPJobID getNewJobId() throws IOException {
    int id;
    synchronized (nextJobId) {
      id = nextJobId;
      nextJobId = Integer.valueOf(id + 1);
    }
    return new BSPJobID(this.masterIdentifier, id);
  }

  @Override
  public JobStatus submitJob(BSPJobID jobID, String jobFile) throws IOException {
    if (jobs.containsKey(jobID)) {
      // job already running, don't start twice
      LOG.info("The job (" + jobID + ") was already submitted");
      return jobs.get(jobID).getStatus();
    }
    
    JobInProgress job = new JobInProgress(jobID, new Path(jobFile), this,
        this.conf);
    return addJob(jobID, job);
  }

  // //////////////////////////////////////////////////
  // GroomServerManager functions
  // //////////////////////////////////////////////////

  @Override
  public ClusterStatus getClusterStatus(boolean detailed) {
    Map<String, String> groomPeersMap = null;

    // give the caller a snapshot of the cluster status
    int numGroomServers = GroomServers.size();
    
    currentClusterTasks=0;
    groomPeersMap = new HashMap<String, String>();
    for (Map.Entry<GroomServerStatus, WorkerProtocol> entry : GroomServers
          .entrySet()) {
      GroomServerStatus s = entry.getKey();
      groomPeersMap.put(s.getGroomName(), s.getPeerName());
      currentClusterTasks+=s.getCurrentTasksCount();
    }
      
    if (detailed) {
      return new ClusterStatus(groomPeersMap, currentClusterTasks, maxClusterTasks,
          state, waitTasks);
    } else {
      return new ClusterStatus(numGroomServers, currentClusterTasks, maxClusterTasks,
          state, waitTasks);
    }
  }
  
  public void setWaitTasks(int waitTasks){
	  this.waitTasks=waitTasks;
  }

  @Override
  public WorkerProtocol findGroomServer(GroomServerStatus status) {
    return GroomServers.get(status);
  }

  @Override
  public Collection<WorkerProtocol> findGroomServers() {
    return GroomServers.values();
  }

  @Override
  public Collection<GroomServerStatus> groomServerStatusKeySet() {
    return GroomServers.keySet();
  }

  @Override
  public void addJobInProgressListener(JobInProgressListener listener) {
    jobInProgressListeners.add(listener);
  }

  @Override
  public void removeJobInProgressListener(JobInProgressListener listener) {
    jobInProgressListeners.remove(listener);
  }

  @Override
  public Map<String, String> currentGroomServerPeers() {
    Map<String, String> tmp = new HashMap<String, String>();
    for (GroomServerStatus status : GroomServers.keySet()) {
      tmp.put(status.getGroomName(), status.getPeerName());
    }
    return tmp;
  }
  

  /**
   * Adds a job to the bsp master. Make sure that the checks are inplace before
   * adding a job. This is the core job submission logic
   * 
   * @param jobId The id for the job submitted which needs to be added
   */
  private synchronized JobStatus addJob(BSPJobID jobId, JobInProgress job) {
    totalSubmissions++;
    synchronized (jobs) {
      jobs.put(job.getProfile().getJobID(), job);
      for (JobInProgressListener listener : jobInProgressListeners) {
        try {
          listener.jobAdded(job);
        } catch (IOException ioe) {
          LOG.error("Fail to alter Scheduler a job is added.", ioe);
        }
      }
    }
    return job.getStatus();
  }

  @Override
  public JobStatus[] jobsToComplete() throws IOException {
    return getJobStatus(jobs.values(), true);
  }

  @Override
  public JobStatus[] getAllJobs() throws IOException {
    LOG.debug("returns all jobs: " + jobs.size());
    return getJobStatus(jobs.values(), false);
  }

  private synchronized JobStatus[] getJobStatus(Collection<JobInProgress> jips,
      boolean toComplete) {
    if (jips == null) {
      return new JobStatus[] {};
    }
    List<JobStatus> jobStatusList = new ArrayList<JobStatus>();
    for (JobInProgress jip : jips) {
      JobStatus status = jip.getStatus();
      // Sets the user name
      status.setUsername(jip.getProfile().getUser());

      if (toComplete) {
        if (status.getRunState() == JobStatus.PREP
            || status.getRunState() == JobStatus.LOAD
            || status.getRunState() == JobStatus.RUNNING) {
          jobStatusList.add(status);
        }
      } else {
        jobStatusList.add(status);
      }
    }

    return jobStatusList.toArray(new JobStatus[jobStatusList.size()]);
  }

  @Override
  public synchronized String getFilesystemName() throws IOException {
    if (fs == null) {
      throw new IllegalStateException("FileSystem object not available yet");
    }
    return fs.getUri().toString();
  }

  /**
   * Return system directory to which BSP store control files.
   */
  @Override
  public String getSystemDir() {
    Path sysDir = new Path(conf.get("bsp.system.dir", "/tmp/hadoop/bsp/system"));
    return sysDir.toString();
    //return fs.makeQualified(sysDir).toString();
  }
  
  public String getCheckPointDir() {
	return conf.get(Constants.CheckPoint.CK_Dir, "/tmp/hadoop/bsp/checkpoint");
  }

  @Override
  public JobProfile getJobProfile(BSPJobID jobid) throws IOException {
    synchronized (this) {
      JobInProgress job = jobs.get(jobid);
      if (job != null) {
        return job.getProfile();
      }
    }
    return null;
  }

  @Override
  public JobStatus getJobStatus(BSPJobID jobid) throws IOException {
    synchronized (this) {
      JobInProgress jip = jobs.get(jobid);
      if (jip != null) {
    	  jip.updateJobStatus();
        return jip.getStatus();
      }
    }
    return null;
  }

  @Override
  public void killJob(BSPJobID jobid) throws IOException {
    JobInProgress job = jobs.get(jobid);

    if (null == job) {
      LOG.info("killJob(): JobId " + jobid.toString() + " is not a valid job");
      return;
    }

    killJob(job);
  }

  private synchronized void killJob(JobInProgress job) {
    LOG.info("Killing job " + job.getJobID());
    job.killJob();
  }

  @Override
  public boolean killTask(TaskAttemptID taskId, boolean shouldFail)
      throws IOException {
    return false;
  }

  public static BSPMaster constructMaster(
      Class<? extends BSPMaster> masterClass, final Configuration conf) {
    try {
      Constructor<? extends BSPMaster> c = masterClass
          .getConstructor(Configuration.class);
      return c.newInstance(conf);
    } catch (Exception e) {
      throw new RuntimeException("Failed construction of " + "Master: "
          + masterClass.toString()
          + ((e.getCause() != null) ? e.getCause().getMessage() : ""), e);
    }
  }

  @SuppressWarnings("deprecation")
  public void shutdown() throws Exception {
	  LOG.info("Prepare to shutdown the master daemon process, please wait...");
      this.taskScheduler.stop();
      LOG.info("stop the scheduler server");
      this.masterServer.stop();
      LOG.info("stop the RPC server on BSPMaster");
      this.cto.stop();
      LOG.info("stop the check thread of heat beat time-out");
  }

  public BSPMaster.State currentState() {
    return this.state;
  }
  
  @Override
  public void buildRouteTable(BSPJobID jobId, TaskInformation statis) {
	  jobs.get(jobId).buildRouteTable(statis);
  }
  
  @Override
  public void registerTask(BSPJobID jobId, TaskInformation statis) {
	  jobs.get(jobId).registerTask(statis);
  }
  
  @Override
  public void beginSuperStep(BSPJobID jobId, int parId) {
	  jobs.get(jobId).beginSuperStep(parId);
  }
  
  @Override
  public void finishSuperStep(BSPJobID jobId, int parId, SuperStepReport ssr) {
	  jobs.get(jobId).finishSuperStep(parId, ssr);
  }
  
  @Override
  public void reportAggregators(BSPJobID jobId, int parId, 
		  AggregatorSetOfKmeans _ask, double taskAgg, double sgl, double vhd) {
	  jobs.get(jobId).reportAggregators(parId, _ask, taskAgg, sgl, vhd);
  }
  
  @Override
  public void saveResultOver(BSPJobID jobId, int parId, int saveRecordNum) {
	  jobs.get(jobId).saveResultOver(parId, saveRecordNum);
  }
  
  @Override
  public void sync(BSPJobID jobId, int parId) {
	  jobs.get(jobId).sync(parId);
  }
  
  @Override
  public void syncArchiveData(BSPJobID jobId, int parId, int arcNum) {
	  jobs.get(jobId).syncArchiveData(parId, arcNum);
  }
  
  @Override
  public void syncLoadData(BSPJobID jobId, int parId, int loadNum) {
	  jobs.get(jobId).syncLoadData(parId, loadNum);
  }

	@Override
	public void reportSGDAggregators(BSPJobID jobId, int parId, GradientOfSGD gradSet, double taskAgg, double sgl,
			double vhd) {
		jobs.get(jobId).reportSGDAggregators(parId, gradSet, taskAgg, sgl, vhd);
		
	}

	@Override
	public void initConvPara(BSPJobID jobId, int parId, int frontMapNum, int outMapNum, int x) {
		// TODO Auto-generated method stub
		jobs.get(jobId).initConvPara(parId, frontMapNum, outMapNum, x);
	}

	@Override
	public void initOutputPara(BSPJobID jobId, int parId, int frontMapNum, int outMapNum, int x) {
		// TODO Auto-generated method stub
		jobs.get(jobId).initOutputPara(parId, frontMapNum, outMapNum, x);
	}

	@Override
	public void updateBiasANDKernels(BSPJobID jobId, int parId, SuperStepReport ssr) {
		// TODO Auto-generated method stub
		jobs.get(jobId).updateBiasANDKernels(parId, ssr);
	}
}
