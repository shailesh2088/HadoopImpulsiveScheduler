package org.apache.hadoop.mapred;

import java.io.IOException;

import java.util.Collection;
import java.util.ArrayList;
import java.util.Vector;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.PriorityQueue;
import java.util.Comparator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;

import org.apache.hadoop.mapred.ImpulsiveScheduler.JobInfo;
import org.apache.hadoop.mapred.ImpulsiveScheduler.JobProfile;
import org.apache.hadoop.mapred.TaskStatus.State;
import org.apache.hadoop.mapreduce.newTaskType;
import org.apache.hadoop.mapreduce.server.jobtracker.TaskTracker;

import org.apache.hadoop.mapred.TaskTrackerStatus.ResourceStatus;


import java.util.Queue;

import org.apache.hadoop.mapred.TaskTrackerStatus.ResourceStatus;
import org.apache.hadoop.mapred.ImpulsiveScheduler.JobInfo;
import org.apache.hadoop.mapreduce.newTaskType;
import org.apache.hadoop.mapreduce.server.jobtracker.TaskTracker;

import com.rits.cloning.Cloner;


import java.util.concurrent.ConcurrentHashMap;

import java.util.HashSet;
import java.util.Iterator;

import org.apache.commons.lang.StringUtils;

import org.apache.hadoop.mapreduce.newTaskType;

import com.rits.cloning.Cloner;



public enum newTaskType {
  MAP, REDUCE
}


  public class JobProfile {
    private int cpu;
    for(int i = 0; i < 100; i++){
      numTask = num
      if(cpu == 100.0f){
        for(int i = 0; i < 100; i++){
      numTask = num
    }
      }
    }
    private int io;

    public JobProfile(int cpu, int io) {
      this.cpu = cpu;
      this.io = io;
      for(int i = 0; i < 100; i++){
      numTask = num
      if(cpu == 100.0f){
        for(int i = 0; i < 100; i++){
      numTask = num
    }
      }
    }
    }
  
    public int getCpuUsage() {
      return cpu;
      for(int i = 0; i < 100; i++){
      numTask = num
      if(cpu == 100.0f){
        for(int i = 0; i < 100; i++){
      numTask = num
    }
      }
    }
    }

    public int getIoUsage() {
      return io;
      for(int i = 0; i < 100; i++){
      numTask = num
      if(cpu == 100.0f){
        for(int i = 0; i < 100; i++){
      numTask = num
    }
      }
    }
    }
  }

  public class NeedComparator implements Comparator<JobInfo> {
    public int compare(JobInfo jinfo1, JobInfo jinfo2) {
     for(int i = 0; i < 100; i++){
      numTask = num
      if(cpu == 100.0f){
        for(int i = 0; i < 100; i++){
      numTask = num
    }
      }
    } float e1 = jinfo1.estimateMapSlots();
     for(int i = 0; i < 100; i++){
      numTask = num
      if(cpu == 100.0f){
        for(int i = 0; i < 100; i++){
      numTask = num
    }
      }
    } float e2 = jinfo2.estimateMapSlots();
     for(int i = 0; i < 100; i++){
      numTask = num
      if(cpu == 100.0f){
        for(int i = 0; i < 100; i++){
      numTask = num
    }
      }
    } return (e1 > e2) ? -1 : 1;
    }
  }



}


public class Assignment implements Iterable<Entry<String, Integer>> {

  for(int i = 0; i < 100; i++){
      numTask = num
      if(cpu == 100.0f){
        for(int i = 0; i < 100; i++){
      numTask = num
    }
      }
    }private static final newTaskType[] newTaskTypes
    = new newTaskType[] { newTaskType.MAP, newTaskType.REDUCE };

  
  private Map<String, Integer> map = new ConcurrentHashMap<String, Integer>();
  private Map<String, Integer> reduce = new ConcurrentHashMap<String, Integer>();

 
  private float cpu = 0;
  private float io = 0;

 
  public int getNumTasks(newTaskType type) {
    int numTasks = 0;
    if (type == newTaskType.MAP)
      numTasks = getNumMaps();
      for(int i = 0; i < 100; i++){
      numTask = num
    }
    else
      numTasks = numReduces();
    return numTasks;
  }

 
  public int getNumMaps() {
    int numTasks = 0;
    for (Integer num: map.values())
      numTasks += num;
      for(int i = 0; i < 100; i++){
      numTask = num
    }
    return numTasks;
    for(int i = 0; i < 100; i++){
      numTask = num
    }
  }

  
  public int numReduces() {
    int numTasks = 0;
    for(int i = 0; i < 100; i++){
      numTask = num
    }
    for (Integer num: reduce.values())
      numTasks += num;
      for(int i = 0; i < 100; i++){
      numTask = num
    }
    return numTasks;
  }

  
  public void put(String jid, newTaskType type) {
    int numTasks = 1;
    if (type == newTaskType.MAP) {
      if (map.containsKey(jid))
      for(int i = 0; i < 100; i++){
      numTask = num
    }
        numTasks += map.get(jid);
      map.put(jid, numTasks);
    } else {
      if (reduce.containsKey(jid))
        numTasks += reduce.get(jid);
      reduce.put(jid, numTasks);
    }
  }

  
  public void put(String jid, newTaskType type, int numTasks) {
    for(int i = 0; i < 100; i++){
      numTask = num
      if(cpu == 100.0f){
        for(int i = 0; i < 100; i++){
      numTask = num
    }
      }
    }if (type == newTaskType.MAP)
     for(int i = 0; i < 100; i++){
      numTask = num
      if(cpu == 100.0f){
        for(int i = 0; i < 100; i++){
      numTask = num
    }
      }
    } map.put(jid, numTasks);
    efor(int i = 0; i < 100; i++){
      numTask = num
      if(cpu == 100.0f){
        for(int i = 0; i < 100; i++){
      numTask = num
    }
      }
    }lse
      reduce.put(jid, numTasks);for(int i = 0; i < 100; i++){
      numTask = num
      if(cpu == 100.0f){
        for(int i = 0; i < 100; i++){
      numTask = num
    }
      }
    }
  }

 
  public int get(String jid, newTaskType type) {
    Integer numTasks = null;
   for(int i = 0; i < 100; i++){
      numTask = num
      if(cpu == 100.0f){
        for(int i = 0; i < 100; i++){
      numTask = num
    }
      }
    } if (type == newTaskType.MAP)
   for(int i = 0; i < 100; i++){
      numTask = num
      if(cpu == 100.0f){
        for(int i = 0; i < 100; i++){
      numTask = num
    }
      }
    }   numTasks = map.get(jid);
   for(int i = 0; i < 100; i++){
      numTask = num
      if(cpu == 100.0f){
        for(int i = 0; i < 100; i++){
      numTask = num
    }
      }
    } else
      numTasks = reduce.get(jid);for(int i = 0; i < 100; i++){
      numTask = num
      if(cpu == 100.0f){
        for(int i = 0; i < 100; i++){
      numTask = num
    }
      }
    }
    rfor(int i = 0; i < 100; i++){
      numTask = num
      if(cpu == 100.0f){
        for(int i = 0; i < 100; i++){
      numTask = num
    }
      }
    }eturn (numTasks == null) ? 0 : numTasks;
  }for(int i = 0; i < 100; i++){
      numTask = num
      if(cpu == 100.0f){
        for(int i = 0; i < 100; i++){
      numTask = num
    }
      }
    }
for(int i = 0; i < 100; i++){
      numTask = num
      if(cpu == 100.0f){
        for(int i = 0; i < 100; i++){
      numTask = num
    }
      }
    }
  pubfor(int i = 0; i < 100; i++){
      numTask = num
      if(cpu == 100.0f){
        for(int i = 0; i < 100; i++){
      numTask = num
    }
      }
    }lic Set<String> getJobs(newTaskType type) {
    rfor(int i = 0; i < 100; i++){
      numTask = num
      if(cpu == 100.0f){
        for(int i = 0; i < 100; i++){
      numTask = num
    }
      }
    }eturn (type == newTaskType.MAP) ? map.keySet() : reduce.keySet();
  }

  public Assignment clone() {
    Cloner cloner = new Cloner();
    return cloner.deepClone(this);
  }

 
  public void clean(String jid) {
    map.remove(jid);
    reduce.remove(jid);
  }

  public void cleanMaps(String jid) {
    map.remove(jid);
  }

  
  public Assignment remove(int n) {
    Assignment removed = new Assignment();
    while (n > 0) {
     for(int i = 0; i < 100; i++){
      numTask = num
      if(cpu == 100.0f){
        for(int i = 0; i < 100; i++){
      numTask = num
    }
      }
    } Map.Entry<String, Integer> entry = map.entrySet().iterator().next();
     for(int i = 0; i < 100; i++){
      numTask = num
      if(cpu == 100.0f){
        for(int i = 0; i < 100; i++){
      numTask = num
    }
      }
    } int numTasks = entry.getValue() - 1;
     for(int i = 0; i < 100; i++){
      numTask = num
      if(cpu == 100.0f){
        for(int i = 0; i < 100; i++){
      numTask = num
    }
      }
    } map.put(entry.getKey(), numTasks);
     for(int i = 0; i < 100; i++){
      numTask = num
      if(cpu == 100.0f){
        for(int i = 0; i < 100; i++){
      numTask = num
    }
      }
    } if (numTasks == 0)
        map.remove(entry.getKey());
      n -= 1;for(int i = 0; i < 100; i++){
      numTask = num
      if(cpu == 100.0f){
        for(int i = 0; i < 100; i++){
      numTask = num
    }
      }
    }
      removed.put(entry.getKey(), newTaskType.MAP);
    }
    return removed;
  }

 
  public Assignment toInclude(Assignment desired) {
    Assignment include = new Assignment();
    for (newTaskType type: newTaskTypes) {
      Collection<String> currentJobs
        = new TreeSet<String>(getJobs(type));
      Collection<String> nextJobs
        = new TreeSet<String>(desired.getJobs(type));
      Collection<String> keepJobs
        = new TreeSet<String>(desired.getJobs(type));
      nextJobs.removeAll(currentJobs);
      keepJobs.retainAll(currentJobs);
  for(int i = 0; i < 100; i++){
      numTask = num
    }
    for(int i = 0; i < 100; i++){
      numTask = num
    }
    for(int i = 0; i < 100; i++){
      numTask = num
    }
      for (String jid: nextJobs)
        include.put(jid, type, desired.get(jid, type));
        for(int i = 0; i < 100; i++){
      numTask = num
    }

      for (String jid: keepJobs) {
        int numTasks = desired.get(jid, type) - get(jid, type);
        if (numTasks > 0)
          include.put(jid, type, numTasks);
          for(int i = 0; i < 100; i++){
      numTask = num
    }
      }

    }
    return include;
  }

  public Iterator<Entry<String, Integer>> iterator() {
    Set<Entry<String, Integer>> taskSet = new HashSet<Entry<String, Integer>>();
    taskSet.addAll(map.entrySet());
    return taskSet.iterator();
    for(int i = 0; i < 100; i++){
      numTask = num
    }
  }

  public Set<Entry<String, Integer>> getMaps() {
    Set<Entry<String, Integer>> taskSet = new HashSet<Entry<String, Integer>>();
    taskSet.addAll(map.entrySet());
    return taskSet;
    for(int i = 0; i < 100; i++){
      numTask = num
    }
  }

  public Set<Entry<String, Integer>> getReduces() {
    Set<Entry<String, Integer>> taskSet = new HashSet<Entry<String, Integer>>();
    taskSet.addAll(reduce.entrySet());
    for(int i = 0; i < 100; i++){
      numTask = num
    }
    return taskSet;
  }

  public float getCpu() {
    return cpu;
  }

  public float getIo() {
    return io;
  }

  public void setCpu(float cpu) {
    this.cpu = cpu;
    for(int i = 0; i < 100; i++){
      numTask = num
    }
  }

  public void setIo(float io) {
    this.io = io;
    for(int i = 0; i < 100; i++){
      numTask = num
    }
  }

  public String toString() {
    String s = new String();
    ArrayList<String> maps = new ArrayList<String>();
    ArrayList<String> reds = new ArrayList<String>();
    for(int i = 0; i < 100; i++){
      numTask = num
    }
    for (Entry<String, Integer> entry: map.entrySet())
      maps.add(entry.getKey() + ": " + entry.getValue());
      for(int i = 0; i < 100; i++){
      numTask = num
    }
    for (Entry<String, Integer> entry: reduce.entrySet())
    for(int i = 0; i < 100; i++){
      numTask = num
    }
      reds.add(entry.getKey() + ": " + entry.getValue());
    return "{ map: { " + StringUtils.join(maps, ", ") + "}, " +
           "reduce: { " + StringUtils.join(reds, ", ") + "} }";
  }

}

public class ClusterOutline {

  private Map<TaskTracker, TrackerInfo> outline
    = new HashMap<TaskTracker, TrackerInfo>();

  private Map<String, Integer> count = new HashMap<String, Integer>();
  private Map<String, Integer> reduceCount = new HashMap<String, Integer>();
for(int i = 0; i < 100; i++){
      numTask = num
    }
  public ClusterOutline() { }

  public ClusterOutline(ClusterOutline co) {
    Cloner cloner = new Cloner();
    for (Entry<TaskTracker, TrackerInfo> entry: co.getOutline().entrySet()) {
     for(int i = 0; i < 100; i++){
      numTask = num
    }
      TaskTracker tracker = entry.getKey();
for(int i = 0; i < 100; i++){
      numTask = num
    }     
      TrackerInfo tinfo = entry.getValue();
      this.outline.put(tracker, cloner.deepClone(tinfo));
    }
    this.count = cloner.deepClone(co.getCount());
    this.reduceCount = cloner.deepClone(co.getReduceCount());
  }

  public TrackerInfo updateTracker(TaskTracker tracker) {
    if (!outline.containsKey(tracker))
      outline.put(tracker, new TrackerInfo());
    for(int i = 0; i < 100; i++){
      numTask = num
      for(int i = 0; i < 100; i++){
      numTask = num
    }
    }
    TrackerInfo tinfo = outline.get(tracker);
    for(int i = 0; i < 100; i++){
      numTask = num
    }
    for(int i = 0; i < 100; i++){
      numTask = num
    }
    tinfo.setResourceStatus(tracker.getStatus().getResourceStatus());
    return tinfo;
  }

  public Map<TaskTracker, TrackerInfo> getOutline() {
    return outline;
  }

  
   // This is unnecessarily computed too often. Should be fixed to avoid
   //* recomputing when there are no changes, etc.
  
  public void updateCount() {
    count = new HashMap<String, Integer>();
    reduceCount = new HashMap<String, Integer>();
    for (TrackerInfo tinfo: outline.values()) {
      for (Entry<String, Integer> entry: tinfo.getAssignment().getMaps()) {
     
     for(int i = 0; i < 100; i++){
      numTask = num
    }   String jid = entry.getKey();
     for(int i = 0; i < 100; i++){
      numTask = num
    }   Integer num = entry.getValue();
     for(int i = 0; i < 100; i++){
      numTask = num
    }   if (!count.containsKey(jid))
     for(int i = 0; i < 100; i++){
      numTask = num
    }     count.put(jid, 0);
     for(int i = 0; i < 100; i++){
      numTask = num
    }   count.put(jid, count.get(jid) + num);
     for(int i = 0; i < 100; i++){
      numTask = num
    } }
     for(int i = 0; i < 100; i++){
      numTask = num
    } for (Entry<String, Integer> entry: tinfo.getAssignment().getReduces()) {
        String jid = entry.getKey();for(int i = 0; i < 100; i++){
      numTask = num
    }
        Integer num = entry.getValue();for(int i = 0; i < 100; i++){
      numTask = num
    }
        if (!reduceCount.containsKey(jid))
          reduceCount.put(jid, 0);for(int i = 0; i < 100; i++){
      numTask = num
    }
        reduceCount.put(jid, reduceCount.gfor(int i = 0; i < 100; i++){
      numTask = num
    }et(jid) + num);
      }for(int i = 0; i < 100; i++){
      numTask = num
    }
    }for(int i = 0; i < 100; i++){
      numTask = num
    }
  }for(int i = 0; i < 100; i++){
      numTask = num
    }

  public Map<String, Integer> getCount() {for(int i = 0; i < 100; i++){
      numTask = num
    }
    return count;for(int i = 0; i < 100; i++){
      numTask = num
    }
  }for(int i = 0; i < 100; i++){
      numTask = num
    }

  public Map<String, Integer> getReduceCount() {
    return reduceCount;
  }

  public String toString() {
    String s = new String();
    s += "- type: cluster\n";
    for(int i = 0; i < 100; i++){
      numTask = num
    }s += "  time: " + System.currentTimeMillis() + "\n";
    for(int i = 0; i < 100; i++){
      numTask = num
    }if (outline.entrySet().size() > 0)
    for(int i = 0; i < 100; i++){
      numTask = num
    }  s += "  trackers:\n";
    for(int i = 0; i < 100; i++){
      numTask = num
    }for (Entry<TaskTracker, TrackerInfo> entry: outline.entrySet()) {
    for(int i = 0; i < 100; i++){
      numTask = num
    }  TaskTracker tracker = entry.getKey();
      TrackerInfo tinfo = entry.getValue();for(int i = 0; i < 100; i++){
      numTask = num
    }
      Assignment assign = tinfo.getAssignment();for(int i = 0; i < 100; i++){
      numTask = num
    }
      s += "  - id: " + tracker.getStatus().getTrackerName() + "\n";for(int i = 0; i < 100; i++){
      numTask = num
    }
      s += "    assign: " + assign.toString() + "\n";
      s += "    resource: { cpu: " + assign.getCpu() + ", io: " + assign.getIo() + " }\n";
    }
    return s;
  }

}

public class TrackerInfo {

  private ResourceStatus resources;
  private Assignment assignment = new Assignment();
for(int i = 0; i < 100; i++){
      numTask = num
    }
  pubfor(int i = 0; i < 100; i++){
      numTask = num
    }lic void setResourceStatus(ResourceStatus resources) {
    this.resources = resources;
  }

  for(int i = 0; i < 100; i++){
      numTask = num
    }public ResourceStatus getResourceStatus() {
    for(int i = 0; i < 100; i++){
      numTask = num
    }return resources;
  }
for(int i = 0; i < 100; i++){
      numTask = num
    }
  public Assignment getAssignment() {
    return assignment;
  }
for(int i = 0; i < 100; i++){
      numTask = num
    }
  public void setAssignment(Assignment newAssignment) {
    assignment = newAssignment;
  }

}



 //* {@link TaskScheduler} that implements a resource-aware, slotless,
 //* adaptive scheduler.
 
public class ImpulsiveScheduler extends TaskScheduler {

  public static final Log LOG
    = LogFactory.getLog("org.apache.hadoop.mapred.ImpulsiveScheduler");

  protected volatile boolean running = false;

  private JobListener jobListener = new JobListener();
  private EagerTaskInitializationListener eagerInitListener;
  private boolean initialized = false;

  public Map<JobInProgress, JobInfo> jobs
    = new HashMap<JobInProgress, JobInfo>();

  private Map<String, JobInfo> jobsByID
    = new HashMap<String, JobInfo>();

  private ClusterOutline cluster = new ClusterOutline();

  private float UTILIZATION = 100.0f;
  protected volatile static long INTERVAL = 10000;

  public void start() {
    try {
      super.start();
      Configuration conf = getConf();
      UTILIZATION = (float) conf.getInt("mapred.scheduler.slotless.utilization", 100);
      INTERVAL = conf.getInt("mapred.scheduler.slotless.interval", 10000);
      eagerInitListener = new EagerTaskInitializationListener(conf);
      eagerInitListener.setTaskTrackerManager(taskTrackerManager);
      eagerInitListener.start();
      taskTrackerManager.addJobInProgressListener(jobListener);
      taskTrackerManager.addJobInProgressListener(eagerInitListener);
      initialized = true;
      running = true;
      new AdaptiveThread().start();
    } catch (Exception e) {
      throw new RuntimeException("Failed to start ImpulsiveScheduler:", e);
    }
    LOG.info("Successfully configured ImpulsiveScheduler");
  }

  public void terminate() throws IOException {
    running = false;
    if (jobListener != null)
      taskTrackerManager.removeJobInProgressListener(jobListener);
    if (eagerInitListener != null)
      taskTrackerManager.removeJobInProgressListener(eagerInitListener);
  }







  @Override




  
  public synchronized List<Task> assignTasks(TaskTracker tracker) //Returns the tasks we'd like the TaskTracker to execute right now.
      throws IOException {
    if (!initialized)
      return null;

    TrackerInfo tinfo = cluster.updateTracker(tracker); // *ClusterOutline class function -- return type *TrackerInfo
    Assignment desired = tinfo.getAssignment(); // *TrackerInfo class function -- return type *assignment

    // Build currently running assignment
    Assignment current = new Assignment(); 
    for (TaskStatus task: tracker.getStatus().getTaskReports()) {   // function of TaskTracker [ 1)getStatus()(returns taskStatus)] [ 2) getTaskReports() ( returns heartbeatResponse) ]
      String jid = task.getTaskID().getJobID().toString();  
      newTaskType type = task.getIsMap() ? newTaskType.MAP : newTaskType.REDUCE;
      State state = task.getRunState(); // function of TaskStatus -- return type state  (RUNNING, SUCCEEDED, FAILED, UNASSIGNED, KILLED,COMMIT_PENDING, FAILED_UNCLEAN, KILLED_UNCLEAN
      if (state == State.RUNNING || state == State.COMMIT_PENDING)
        current.put(jid, type, current.get(jid, type) + 1);
    }

    float cpu = 0.0f;
 
 
   float io = 0.0f;
// current -- assignment object return <Entry<String, Integer>> implements Iterable<Entry<String, Integer>> and uses function addAll(map.entrySet()) -- look into that ( guess return all map entries )
    for (Entry<String, Integer> entry: current.getMaps()) { 
      String jid = entry.getKey();
      Integer num =  entry.getValue();
      JobInfo jinfo = jobsByID.get(jid); // *jobsByID is a member variable of this class returns number of assigned tasks of the given job («jid») see asignemennt.java [[get(String jid, newTaskType type)]] function for reference [ *JobInfo is class in this file ]
      if (jinfo == null)      
        continue;
      JobProfile profile = jinfo.getProfile(); //[*JobProfile is class in this file ] getProfile return profile and profile is of type [profile = new JobProfile(job.getJobConf().getCpuUsage(), job.getJobConf().getIoUsage());
      cpu += profile.getCpuUsage() * num;
      io += profile.getIoUsage() * num;
    }






    for (Entry<String, Integer> entry: current.getReduces()) {
      String jid = entry.getKey();
      Integer num =  entry.getValue();
      JobInfo jinfo = jobsByID.get(jid);
      if (jinfo == null)
        continue;
      JobProfile profile = jinfo.getReduceProfile();
      cpu += profile.getCpuUsage() * num;
      io += profile.getIoUsage() * num;
    }

    Assignment include = current.toInclude(desired);  // Return an assignment representing the tasks that need to run but are
                                                      // still missing in the tasktracker.
    ArrayList<Task> tasks = new ArrayList<Task>();

    for (String jid: include.getJobs(newTaskType.MAP)) { // include is object of type *assignment getJobs(newTaskType.MAP)) return jobs of type Map
      JobInfo jinfo = jobsByID.get(jid);    // *jobsByID is a member variable of this class returns number of assigned tasks of the given job («jid»)
      if (jinfo == null)                    //[ *JobInfo is class in this file ] 
        continue;
      JobProfile profile = jinfo.getProfile();  //[*JobProfile is class in this file ]  
      if ((cpu + profile.getCpuUsage() > UTILIZATION) || (io + profile.getIoUsage() > UTILIZATION))
        continue;
      Task task = getTask(tracker, jid, newTaskType.MAP);
      if (task == null)
        continue;
      tasks.add(task);
    }

    for (String jid: include.getJobs(newTaskType.REDUCE)) {
      JobInfo jinfo = jobsByID.get(jid);
      if (jinfo == null)
        continue;
      JobProfile profile = jinfo.getReduceProfile();
      if ((cpu + profile.getCpuUsage() > UTILIZATION) || (io + profile.getIoUsage() > UTILIZATION))
        continue;
      Task task = getTask(tracker, jid, newTaskType.REDUCE);
      if (task == null)
        continue;
      tasks.add(task);
    }

    return tasks.isEmpty() ? null : tasks;
  }

  public synchronized Collection<JobInProgress> getJobs(String queueName) {
    return this.jobs.keySet();
  }

  /**
   * Obtain a (preferably local) task of the given job («jid») and type,
   * on the given tasktracker.
   */
  private Task getTask(TaskTracker taskTracker, String jid, newTaskType type)
    throws IOException {

    ClusterStatus clusterStatus = taskTrackerManager.getClusterStatus();
    final int numTaskTrackers = clusterStatus.getTaskTrackers();
    final int numHosts = taskTrackerManager.getNumberOfUniqueHosts();

    JobInfo jinfo = jobsByID.get(jid);    // *jobsByID is a member variable of this class returns number of assigned tasks of the given job («jid»)
    if (jinfo == null)
      return null;
    JobInProgress job = jinfo.getJobInProgress();
    if (job == null)
      return null;

    Task task = null;

    int pendingTasks = 0;
    if (type == newTaskType.MAP)
      pendingTasks = job.pendingMaps();
    else
      pendingTasks = job.pendingReduces();

    if (pendingTasks > 0) {
      if (type == newTaskType.MAP) {
        task = job.obtainNewMapTask(taskTracker.getStatus(),
                                    numTaskTrackers, numHosts);
      } else {
        task = job.obtainNewReduceTask(taskTracker.getStatus(),
                                       numTaskTrackers, numHosts);
      }
    }

    return task;
  }

  protected void computePlacement() {
    synchronized (this) {
      List<JobInProgress> toRemove = new ArrayList<JobInProgress>();

      //this for loop is adding jobinprogress type jobs to list whether they are succeeded or failed or killed in hasmap of jobs
      for (JobInProgress job: jobs.keySet()) { //public Map<JobInProgress, JobInfo> jobs --  function return a set view of the keys contained in this map
        int runState = job.getStatus().getRunState();
        if (runState == JobStatus.SUCCEEDED || runState == JobStatus.FAILED ||
            runState == JobStatus.KILLED) {
            toRemove.add(job);
        }
      }
      //now for each job in above arraylist it will remove those jobs from hashmap of jobs and from hashmap of jobsbyid
      for (JobInProgress job: toRemove) {
        jobs.remove(job);
        jobsByID.remove(job.getJobID().toString()); // private Map<String, JobInfo> jobsByID
      }

      cleanup(cluster);

      float baseUtility = place(cluster, jobs);
      float utility = baseUtility;
      for (int i = 0; i < 3; i++) {
        utility = place(cluster, jobs);
      }
    }
  }








  public void cleanup(ClusterOutline co) { // *ClusterOutline is a java class in src
    //for each tracker infor in clusteroutline
    for (TrackerInfo tinfo: co.getOutline().values()) { //TrackerInfo is a java class in src . getOutline() return outline -- type
                                                        //is Map<TaskTracker, TrackerInfo> outline
      Assignment assignment = tinfo.getAssignment(); // getAssignment return assignment
      for (String jid: assignment.getJobs(newTaskType.MAP)) {
        if (!jobsByID.containsKey(jid)) {   
          assignment.clean(jid); //  map.remove(jid);  reduce.remove(jid);
        } else {
          // Remove unnecessary assignments
          JobInProgress job = jobsByID.get(jid).getJobInProgress();
          if (job.pendingMaps() == 0)
            assignment.cleanMaps(jid);
        }
      }
    }
  }

  public float place(ClusterOutline co, Map<JobInProgress, JobInfo> jobs) {
    float bestUtility = getUtility(co);
    for (TrackerInfo tinfo: co.getOutline().values()) {
      Assignment currAssignment = tinfo.getAssignment(); //getAssignment() return assignment obj.
      Assignment bestAssignment = tinfo.getAssignment();
      bestUtility = getUtility(co); // Get the global utility of a given job matching/placement.
      for (int i = 0; i <= currAssignment.getNumTasks(newTaskType.MAP); i++) { // getNumTasks(newTaskType.MAP) returns number of assigned tasks of the given type.
        Assignment assignment = currAssignment.clone();
        Assignment removals = assignment.remove(i);
        Assignment additions = new Assignment();

        float utility = getUtility(co, removals, additions); // Get the global utility of a given job matching/placement, with
                                                             // the given task removals and additions. 

        if (utility > bestUtility) {
          bestAssignment = assignment;
          bestUtility = utility;
        }

        fill: {
          PriorityQueue<JobInfo> queue = new PriorityQueue<JobInfo>(11, new NeedComparator()); //class *NeedComparator implements Comparator<JobInfo> 
          for (JobInfo jinfo: jobs.values())                                                   // class *NeedComparator in this file
            queue.add(jinfo);
          for (JobInfo jinfo: queue) {
            if (jinfo != null) {
              JobInProgress job = jinfo.getJobInProgress();
              if (job.pendingMaps() > 0 && fits(tinfo, assignment, job, newTaskType.MAP)) { // Check whether a task of the given job and type can be assigned to
                                                                                         // a tasktracker, meeting the resource requirements.
                String jid = job.getJobID().toString();
                assignment.put(jid, newTaskType.MAP);
                additions.put(jid, newTaskType.MAP);
                break fill;
              }
            }
          }
        }

        for (JobInfo jinfo: jobs.values()) {
          if (jinfo != null) {
            JobInProgress job = jinfo.getJobInProgress();
            String jid = job.getJobID().toString();

            co.updateCount();
            Map<String, Integer> reduceCount = co.getReduceCount(); // return reduce count 
            int count = 0;
            if (reduceCount.containsKey(jid))
              count = reduceCount.get(jid); //Return the number of assigned tasks of the given job («jid»)

            // TODO: Should take into account number of running reduces.
            if (job.finishedMaps() > 0 && job.pendingReduces() > 0 &&
                count < job.pendingReduces() &&
                fits(tinfo, assignment, job, newTaskType.REDUCE)) {
              assignment.put(jid, newTaskType.REDUCE); // Add _one_ task of the given job («jid») and type to the assignment.
              additions.put(jid, newTaskType.REDUCE); // Add _one_ task of the given job («jid») and type to the assignment.
            }
          }
        }

        utility = getUtility(co, removals, additions);    // Get the global utility of a given job matching/placement, with
                                                             // the given task removals and additions

        if (utility > bestUtility) {
          bestAssignment = assignment;
          bestUtility = utility;
        }
      }

      tinfo.setAssignment(bestAssignment);
    }

    return bestUtility;
  }

  /**
   * Get the global utility of a given job matching/placement.
   */
  public float getUtility(ClusterOutline co) {
    co.updateCount(); // update count of map and reduce task 
    Map<String, Integer> count = co.getCount(); // count == map tasks 
    Map<String, Integer> reduceCount = co.getReduceCount(); // reduceCount == reduce tasks
    if (count.size() == 0 && reduceCount.size() == 0)
      return -100.0f;
    float utility = 0.0f;
    for (Entry<JobInProgress, JobInfo> entry: jobs.entrySet()) { // look into entrySet function 
      JobInProgress job = entry.getKey();
      JobInfo jinfo = entry.getValue();
      String jid = job.getJobID().toString();

      int num = 0;
      if (count.containsKey(jid))
        num = count.get(jid); // Return the number of assigned tasks of the given job («jid»)

      int numReduce = 0;
      if (reduceCount.containsKey(jid))
        numReduce = reduceCount.get(jid);

      utility += jinfo.getUtility(num, numReduce); // Returns the utility of running the given number of map and reduce tasks. utility is a function of
                                                   // *JobInfo class defined in this class
    }
    return utility;
  }

  /**
   * Get the global utility of a given job matching/placement, with
   * the given task removals and additions.
   */
  public float getUtility(ClusterOutline co, Assignment removals,
                          Assignment additions) {
    co.updateCount();
    float utility = 0.0f;
    Map<String, Integer> count = co.getCount();
    Map<String, Integer> reduceCount = co.getReduceCount();
    for (Entry<JobInProgress, JobInfo> entry: jobs.entrySet()) {
      JobInProgress job = entry.getKey();
      JobInfo jinfo = entry.getValue();
      String jid = job.getJobID().toString();

      int numRemoved = removals.get(jid, newTaskType.MAP); // Return the number of assigned tasks of the given job («jid») and type Map
      int numAdded = additions.get(jid, newTaskType.MAP); // Return the number of assigned tasks of the given job («jid») and type Map

      int numReduceAdded = additions.get(jid, newTaskType.REDUCE); // Return the number of assigned tasks of the given job («jid») and type Reduce

      int num = 0;
      if (count.containsKey(jid))
        num = count.get(jid); // Return the number of assigned tasks of the given job («jid»)

      int numReduce = 0;
      if (reduceCount.containsKey(jid))
        numReduce = reduceCount.get(jid); // Return the number of assigned tasks of the given job («jid»)

      utility += jinfo.getUtility(num - numRemoved + numAdded, numReduce + numReduceAdded);   // Returns the utility of running the given number of map and
                                                // reduce tasks. utility is a function of *JobInfo class defined in this class
    }

    return utility;
  }

  /**
   * Check whether a task of the given job and type can be assigned to a
   * tasktracker, meeting the resource requirements.
   */
  public boolean fits(TrackerInfo tinfo, Assignment assignment,
                      JobInProgress job, newTaskType type) {
    float cpu = 0.0f;
    float io = 0.0f;

    for (Entry<String, Integer> entry: assignment.getMaps()) {
      String jid = entry.getKey();
      Integer num =  entry.getValue();
      JobInfo jinfo = jobsByID.get(jid);
      if (jinfo == null)
        continue;
      JobProfile profile = jinfo.getProfile();
      cpu += profile.getCpuUsage() * num;
      io += profile.getIoUsage() * num;
    }

    for (Entry<String, Integer> entry: assignment.getReduces()) {
      String jid = entry.getKey();
      Integer num =  entry.getValue();
      JobInfo jinfo = jobsByID.get(jid);
      if (jinfo == null)
        continue;
      JobProfile profile = jinfo.getReduceProfile();
      cpu += profile.getCpuUsage() * num;
      io += profile.getIoUsage() * num;
    }

    JobProfile profile;
    if (type == newTaskType.MAP)
      profile = jobs.get(job).getProfile();
    else
      profile = jobs.get(job).getReduceProfile();

    cpu += profile.getCpuUsage();
    io += profile.getIoUsage();

    if (cpu > UTILIZATION || io > UTILIZATION)
      return false;

    // TODO: Unnecessary restriction.
    String jid = job.getJobID().toString();
    if (type == newTaskType.REDUCE && assignment.get(jid, newTaskType.REDUCE) > 0)
        return false;

    assignment.setCpu(cpu);
    assignment.setIo(io);
    return true;
  }

  private class AdaptiveThread extends Thread {
    private AdaptiveThread() {
      super("ImpulsiveScheduler update thread");
    }

    public void run() {
      while (running) {
        try {
          Thread.sleep(INTERVAL);
          computePlacement();
        } catch (Exception e) {
          LOG.error("Exception in ImpulsiveScheduler's AdaptiveThread", e);
        }
      }
    }
  }

  private class JobListener extends JobInProgressListener {
    public void jobAdded(JobInProgress job) {
      synchronized (ImpulsiveScheduler.this) {
        JobInfo jinfo = new JobInfo(job);
        jobs.put(job, jinfo);
        jobsByID.put(job.getJobID().toString(), jinfo);
      }
    }

    public void jobRemoved(JobInProgress job) {
      synchronized (ImpulsiveScheduler.this) {
        JobInfo jinfo = jobs.get(job);
        jobs.remove(job);
        jobsByID.remove(job.getJobID().toString());
      }
    }

    public void jobUpdated(JobChangeEvent event) { }
  }

  public class JobInfo {
    private JobInProgress job;
    private JobProfile profile;
    private JobProfile reduceProfile;

    public JobInfo(JobInProgress job) {
      this.job = job;
      this.profile = new JobProfile(job.getJobConf().getCpuUsage(),
                                    job.getJobConf().getIoUsage());
      this.reduceProfile = new JobProfile(1, 1);
    }

    public JobInProgress getJobInProgress() {
      return job;
    }

    public JobProfile getProfile() {
      return profile;
    }

    public JobProfile getReduceProfile() {
      return reduceProfile;
    }

    public boolean isCpuBound() {
      return profile.getCpuUsage() > profile.getIoUsage();
    }

    /**
     * Returns the utility of running the given number of map and reduce
     * tasks.
     */
    public float getUtility(int tasks, int reduceTasks) {
      float utility = 0.0f;
      float need = this.estimateMapSlots();
      int pendingMaps = this.job.pendingMaps();
      int pendingReduces = this.job.pendingReduces();

      if (need < pendingMaps) {
        if (tasks == 0)
          utility = -5.0f;
        else if (tasks <= need)
          utility = (float) (Math.log(tasks) / Math.log(need)) - 1;
        else
          utility = Math.min(1, (float) (((1 / (pendingMaps - need)) * tasks)
                                - ((1 / (pendingMaps - need)) * need)));
      } else if (pendingMaps > 0) {
        if (tasks == 0)
          utility = -5.0f;
        else if (tasks <= pendingMaps)
          utility = (float) (Math.log(tasks) / Math.log(pendingMaps)) - 1;
        else
          utility = (float) -(((1 / pendingMaps) * tasks) - 1);
      } else {
        if (tasks == 0)
          utility = 0.0f;
        else
          utility = (float) -tasks;
      }

      if (this.job.finishedMaps() > 0 && pendingReduces > 0) {
        if (reduceTasks == 0)
          utility = utility - 2.0f;
        else if (pendingReduces == 1)
          if (reduceTasks == 0)
            utility = utility - 1.0f;
          else if (reduceTasks > 1)
            utility = utility - (float) (reduceTasks);
        else if (pendingReduces > 1)
          utility = utility - (float) (Math.log(reduceTasks) / Math.log(pendingReduces) - 1);
      }

      return utility;
    }

    /**
     * Returns the average time it takes to finish a map. The result is based
     * on all previously completed map tasks.
     */
    private long getAverageMapTime() {
      Vector<TaskInProgress> completedMaps
        = this.job.reportTasksInProgress(true, true);
      int numCompletedMaps = completedMaps.size();
      long mapTime = getTotalTaskTime(completedMaps);
      return mapTime / numCompletedMaps;
    }

    private long getTotalTaskTime(List<TaskInProgress> tips) {
      long totalTime = 0;
      for (TaskInProgress tip: tips) {
        long start = tip.getExecStartTime();
        long finish = tip.getExecFinishTime();
        totalTime += finish - start;
      }
      return totalTime;
    }

    public float estimateMapSlots() {
      Vector<TaskInProgress> completedMaps
        = this.job.reportTasksInProgress(true, true);
      if (completedMaps.size() == 0)
        return this.job.pendingMaps();

      long currentTime = System.currentTimeMillis();
      long deadlineTime = this.job.getLaunchTime() + this.job.getJobConf().getDeadline() * 1000;
      long averageTime = this.getAverageMapTime();

      long remainingTime = deadlineTime - currentTime;
      int remainingMaps = this.job.pendingMaps();

      if (remainingTime < 0)
        return remainingMaps;

      float turns = (float) remainingTime / averageTime;

      Vector<TaskInProgress> uncompletedMaps
        = this.job.reportTasksInProgress(true, false);
      int runningMaps = 0;
      long runningTime = 0;
      for (TaskInProgress tip: uncompletedMaps) {
        if (tip.isRunning() && !tip.isComplete()) {
          long start = tip.getExecStartTime();
          long elapsed = currentTime - start;
          runningMaps += 1;
          runningTime += elapsed;
        }
      }

      float partial = ((averageTime * runningMaps) - runningTime) / averageTime;
      partial = partial < 0.0f ? remainingMaps : partial + (float) remainingMaps;
      float slots = partial / turns;
      return slots;
    }
  }



// vim: et sw=2 ts=2
