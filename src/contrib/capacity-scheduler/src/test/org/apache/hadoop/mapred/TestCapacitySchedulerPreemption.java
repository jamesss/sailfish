/**
 * Copyright 2011 Yahoo Corporation.  All rights reserved.
 * This file is part of the Sailfish project.
 *
 * Licensed under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 */
package org.apache.hadoop.mapred;

// XXX junit3?
//import org.junit.Test;
//import static org.junit.Assert.*;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.CapacityTaskScheduler.ReduceSchedulingMgr;
import org.apache.hadoop.mapred.CapacityTaskScheduler.TaskSchedulingMgr;
import org.apache.hadoop.mapred.CapacityTaskScheduler.QueueSchedulingInfo;
import org.apache.hadoop.mapred.CapacityTaskScheduler.TaskSchedulingInfo;
import org.apache.hadoop.mapred.CapacityTaskScheduler;
import org.apache.hadoop.mapred.JobTracker;
import org.apache.hadoop.mapred.TaskTrackerManager;

import junit.framework.TestCase;
import static org.mockito.Mockito.*;

public class TestCapacitySchedulerPreemption extends TestCase {

  public Map<String,JobInProgress> runPreemptionTest(
      Map<String,QueueSchedulingInfo> queues,
      JobQueuesManager jqm) throws Exception {
    ClusterStatus clusterStatus = new ClusterStatus(
        25,  // tasktrackers
        0,   // blacklists
        JobTracker.TASKTRACKER_EXPIRY_INTERVAL,
        200, // running maps
        100, // running reduces
        200, // max maps
        100, // max reduces
        JobTracker.State.RUNNING);
    return runPreemptionTest(queues, jqm, clusterStatus);
  }
  public Map<String,JobInProgress> runPreemptionTest(
      Map<String,QueueSchedulingInfo> queues,
      JobQueuesManager jqm,
      ClusterStatus clusterStatus) throws Exception {
    final CapacityTaskScheduler mockCS = mock(CapacityTaskScheduler.class);
    final Configuration conf = new Configuration(false);
    conf.setBoolean("capacity.scheduler.preempt", true);
    conf.setFloat("capacity.scheduler.preempt.fraction", 0.01f);

    JobStatus runningJob = new JobStatus();
    runningJob.setRunState(JobStatus.RUNNING);

    Map<String,JobInProgress> jip = new HashMap<String,JobInProgress>();
    for (String qn : queues.keySet()) {
      JobInProgress job = mock(JobInProgress.class);
      when(job.getStatus()).thenReturn(runningJob);
      List<JobInProgress> jc = Collections.singletonList(job);
      when(jqm.getRunningJobQueue(eq(qn))).thenReturn(jc);
      jip.put(qn, job);
    }

    when(mockCS.getConf()).thenReturn(conf);
    when(mockCS.getJobQueuesManager()).thenReturn(jqm);

    TaskTrackerManager mockTTM = mock(TaskTrackerManager.class);
    when(mockTTM.getClusterStatus()).thenReturn(clusterStatus);
    mockCS.taskTrackerManager = mockTTM;

    ReduceSchedulingMgr rsm = new ReduceSchedulingMgr(mockCS);
    rsm.initialize(queues);
    rsm.setPreemptionTarget();

    return jip;
  }

  public void testPreemption1() throws Exception {
    // Q1(60/50), Q2(40/30), Q3(0/20)
    // Q1(50/50), Q2(30/30), Q3(20/20)
    JobQueuesManager jqm = mock(JobQueuesManager.class);
    Map<String,QueueSchedulingInfo> queues =
      new HashMap<String,QueueSchedulingInfo>();
    QueueSchedulingInfo q1 = new QueueSchedulingInfo("queue1", 0.5f, -1, jqm);
    q1.mapTSI = null; // XXX not supported; check for NPE
    q1.reduceTSI = new TaskSchedulingInfo(50, // capacity
                                          60, // numReduceTasks
                                          60, // numSlotsOccupied
                                          0); // numPendingTasks
    queues.put("queue1", q1);

    QueueSchedulingInfo q2 = new QueueSchedulingInfo("queue2", 0.3f, -1, jqm);
    q2.mapTSI = null;
    q2.reduceTSI = new TaskSchedulingInfo(30, 40, 40, 0);
    queues.put("queue2", q2);

    QueueSchedulingInfo q3 = new QueueSchedulingInfo("queue3", 0.2f, -1, jqm);
    q3.mapTSI = null;
    q3.reduceTSI = new TaskSchedulingInfo(20, 0, 0, 20);
    queues.put("queue3", q3);

    Map<String,JobInProgress> jobs = runPreemptionTest(queues, jqm);
    JobInProgress jip = jobs.get("queue1");
    verify(jip).setReducePreemptionTarget(10);
    jip = jobs.get("queue2");
    verify(jip).setReducePreemptionTarget(10);
  }

  public void testPreemption2() throws Exception {
    // Q1(60/50), Q2(40/30), Q3(0/20)
    // Q1(55/50), Q2(35/30), Q3(10/20)
    JobQueuesManager jqm = mock(JobQueuesManager.class);
    Map<String,QueueSchedulingInfo> queues =
      new HashMap<String,QueueSchedulingInfo>();
    QueueSchedulingInfo q1 = new QueueSchedulingInfo("queue1", 0.5f, -1, jqm);
    q1.mapTSI = null; // XXX not supported; check for NPE
    q1.reduceTSI = new TaskSchedulingInfo(50, // capacity
                                          60, // numReduceTasks
                                          60, // numSlotsOccupied
                                          0); // numPendingTasks
    queues.put("queue1", q1);

    QueueSchedulingInfo q2 = new QueueSchedulingInfo("queue2", 0.3f, -1, jqm);
    q2.mapTSI = null;
    q2.reduceTSI = new TaskSchedulingInfo(30, 40, 40, 0);
    queues.put("queue2", q2);

    QueueSchedulingInfo q3 = new QueueSchedulingInfo("queue3", 0.2f, -1, jqm);
    q3.mapTSI = null;
    q3.reduceTSI = new TaskSchedulingInfo(20, 0, 0, 10);
    queues.put("queue3", q3);

    Map<String,JobInProgress> jobs = runPreemptionTest(queues, jqm);
    JobInProgress jip = jobs.get("queue1");
    verify(jip).setReducePreemptionTarget(5);
    jip = jobs.get("queue2");
    verify(jip).setReducePreemptionTarget(5);
    jip = jobs.get("queue3");
    verify(jip).setReducePreemptionTarget(0);
  }

  public void testPreemption3() throws Exception {
    // Q1(2000/1000), Q2(0/1000)
    // Q1(1000/1000), Q2(1000/1000)
    JobQueuesManager jqm = mock(JobQueuesManager.class);
    Map<String,QueueSchedulingInfo> queues =
      new HashMap<String,QueueSchedulingInfo>();
    QueueSchedulingInfo q1 = new QueueSchedulingInfo("queue1", 0.5f, -1, jqm);
    q1.mapTSI = null; // XXX not supported; check for NPE
    q1.reduceTSI = new TaskSchedulingInfo(1000, // capacity
                                          2000, // numReduceTasks
                                          2000, // numSlotsOccupied
                                          0); // numPendingTasks
    queues.put("queue1", q1);

    QueueSchedulingInfo q2 = new QueueSchedulingInfo("queue2", 0.5f, -1, jqm);
    q2.mapTSI = null;
    q2.reduceTSI = new TaskSchedulingInfo(1000, 0, 0, 1);
    queues.put("queue2", q2);

    ClusterStatus cluster = new ClusterStatus(
        1000,  // tasktrackers
        0,   // blacklists
        JobTracker.TASKTRACKER_EXPIRY_INTERVAL,
        600, // running maps
        2000, // running reduces
        6000, // max maps
        2000, // max reduces
        JobTracker.State.RUNNING);
    Map<String,JobInProgress> jobs = runPreemptionTest(queues, jqm, cluster);
    JobInProgress jip = jobs.get("queue1");
    verify(jip).setReducePreemptionTarget(1);
    jip = jobs.get("queue2");
    verify(jip).setReducePreemptionTarget(0);
  }

  // spare capacity, no need to preempt (w/ disabled queue)
  public void testPreemptionZero() throws Exception {
    // Q1(60/50), Q2(0/50), Q3(0/0)
    // Q1(60/50), Q2(10/50), Q3(0/0)
    JobQueuesManager jqm = mock(JobQueuesManager.class);
    Map<String,QueueSchedulingInfo> queues =
      new HashMap<String,QueueSchedulingInfo>();
    QueueSchedulingInfo q1 = new QueueSchedulingInfo("queue1", 0.5f, -1, jqm);
    q1.mapTSI = null; // XXX not supported; check for NPE
    q1.reduceTSI = new TaskSchedulingInfo(50, // capacity
                                          60, // numReduceTasks
                                          60, // numSlotsOccupied
                                          0); // numPendingTasks
    queues.put("queue1", q1);

    QueueSchedulingInfo q2 = new QueueSchedulingInfo("queue2", 0.5f, -1, jqm);
    q2.mapTSI = null;
    q2.reduceTSI = new TaskSchedulingInfo(50, 0, 0, 10);
    queues.put("queue2", q2);

    QueueSchedulingInfo q3 = new QueueSchedulingInfo("queue3", 0.0f, -1, jqm);
    q3.mapTSI = null;
    q3.reduceTSI = new TaskSchedulingInfo(0, 0, 0, 0);
    queues.put("queue3", q3);

    ClusterStatus cluster = new ClusterStatus(
        25,  // tasktrackers
        0,   // blacklists
        JobTracker.TASKTRACKER_EXPIRY_INTERVAL,
        200, // running maps
        60, // running reduces
        200, // max maps
        100, // max reduces
        JobTracker.State.RUNNING);
    Map<String,JobInProgress> jobs = runPreemptionTest(queues, jqm, cluster);
    JobInProgress jip = jobs.get("queue1");
    verify(jip).setReducePreemptionTarget(0);
    jip = jobs.get("queue2");
    verify(jip).setReducePreemptionTarget(0);
    jip = jobs.get("queue3");
    verify(jip, never()).setReducePreemptionTarget(anyInt());
  }

}
