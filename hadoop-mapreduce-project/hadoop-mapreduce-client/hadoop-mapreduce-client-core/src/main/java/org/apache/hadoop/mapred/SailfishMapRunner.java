/**
 * Copyright 2010 Yahoo Corporation.  All rights reserved.
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
// package com.yahoo.labs.sailfish;

package org.apache.hadoop.mapred;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.NumberFormat;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.HttpURL;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapRunner;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.MapTask.OldOutputCollector;
import org.apache.hadoop.util.ReflectionUtils;

public class SailfishMapRunner<K1, V1, K2, V2> extends
    MapRunner<K1, V1, K2, V2> {

  static final private Log LOG = LogFactory.getLog(SailfishMapRunner.class);
  JobConf hadoopJob;
  int taskNumber;
  private Mapper<K1, V1, K2, V2> mapper;
  // private Reducer<K1, V1, K2, V2> combiner;
  SetupHelper setupHelper;
  boolean mapIgnoresInput = false;

  @Override
  public void configure(JobConf job) {
    this.mapper = ReflectionUtils.newInstance(job.getMapperClass(), job);
    this.hadoopJob = job;
    this.taskNumber = str2taskNumber(this.hadoopJob.get("mapred.task.id"));
    Class<? extends Reducer> combinerClaz = hadoopJob.getClass(
        "sailfish.mapred.combiner.class", null, Reducer.class);
    this.mapIgnoresInput = job.getBoolean("sailfish.job.map_ignores_input", false);
    if (combinerClaz != null) {
      // this.combiner = ReflectionUtils.newInstance(combinerClaz, job);
      LOG.warn("Sailfish does not support combiners.  Combiner support is disabled");
    }
  }



  @Override
  public void run(RecordReader<K1, V1> input, OutputCollector<K2, V2> output,
      Reporter reporter) throws IOException {
    try {

      setupHelper = new SetupHelper();
      setupHelper.setup(hadoopJob);

      //
      // Create the output collector to trap the output and pass it to iappender
      //
      SailfishMapCollector smc = new SailfishMapCollector(hadoopJob, reporter);
      smc.setOutputStream(setupHelper.iappender.getOutputStream());
      OldOutputCollector theCollector = new OldOutputCollector(smc, hadoopJob);
      
      /* If we supported combiners, then enable this code
              OldOutputCollector mc = new OldOutputCollector(smc, hadoopJob);
      if (combiner != null) {
        combineCollector = ...
        theCollector = new OldOutputCollector(combineCollector, hadoopJob);
      } else {
        theCollector = mc;
      }
      */

      String debugJobId = hadoopJob.get("sailfish.mapred.debug_job.id", null);
      if (debugJobId == null) {
        // this means we are not debugging...otherwise, mapper exits straightaway
        K1 key = input.createKey();
        V1 value = input.createValue();
        if (mapIgnoresInput) {
          mapper.map(key, value, theCollector, reporter);
        } else {
          while (input.next(key, value)) {
            mapper.map(key, value, theCollector, reporter);
          }
        }
      }
      /*
      if (combineCollector != null) {
        combineCollector.flush();
      }
      */
      // Tell the map collector to send shutdown sequence to kappender
      smc.close();
      // get a full heartbeat cycle with Hadoop
      reporter.progress();
      setupHelper.close();
      // We don't do anything to Hadoop's output collector.  Once this method returns,
      // hadoop's output collector logic will work in notifying JT of completion.
    }
    finally {
      mapper.close();
    }
  }

  // Refactoring the helper code into a separate class so that we can re-use with mapreduce apis.
  static public class SetupHelper {
    JobConf hadoopJob;
    public Process iappender;
    public ChildOutputGrabber iappenderStdout;

    public void setup(JobConf jc) throws IOException {
      hadoopJob = jc;
      notifyWorkbuilder();
      startIAppender();
    }

    public void close() throws IOException {
      int childExitCode;
      // wait for child to exit...
      LOG.info("Waiting for iappender to exit...");
      while (true) {
        try {
          childExitCode = iappender.waitFor();
          if (childExitCode != 0) {
            throw new IOException("iappender exited badly: exit code = " + childExitCode);
          }
          break;
        } catch (InterruptedException ie) {
          LOG.info("Interrupted while waiting for iappender to exit...");
        }
      }
    }
    private void notifyWorkbuilder() throws IOException {
      // job tracker-jobid
      String jobid = hadoopJob.get("sailfish.mapred.debug_job.id", null);
      if (jobid == null)
        jobid = hadoopJob.get("mapred.job.id");
      String jobname = hadoopJob.get("mapred.job.tracker.http.address") + "-" + jobid;
      // String workbuilderHP = "http://" + hadoopJob.get("sailfish.job.workbuilder.host") + ":" + hadoopJob.get("sailfish.job.workbuilder.port");
      String workbuilderPort = System.getenv("SAILFISH_WORKBUILDER_PORT");
      // !#! Fix me...replace localhost with AM nodename
      String workbuilderHP = "http://localhost" + ":" + workbuilderPort;
      HttpURL url = new HttpURL(workbuilderHP);

      url.setPath("/mapperstart/");
      url.setQuery(new String[] { "jobid", "m", "r" },
          new String[] { jobname, Integer.toString(hadoopJob.getNumMapTasks()),
          Integer.toString(hadoopJob.getNumReduceTasks())});

      int code = -1;
      String errorStr = null;

      for (int i = 0; i < 5; i++) {
        try {
          HttpClient c = new HttpClient();
          HttpMethod method = new GetMethod(url.toString());
          c.executeMethod(method);
          code = method.getStatusCode();
          errorStr = method.getResponseBodyAsString();
          break;
        } catch (Exception e) {
          try {
            // sleep and retry the connect
            Thread.currentThread().sleep(30 * 1000);
          } catch(InterruptedException ie){ }
        }
      }
      if (code != 200) {
        throw new IOException("unable to notify workbuilder: " + errorStr);
      }
    }

    private void startIAppender() throws IOException {
      String jobId = hadoopJob.get("mapred.job.id");
      StringBuilder basedir = new StringBuilder("/jobs/" + jobId);
      int mapperId = str2taskNumber(hadoopJob.get("mapred.task.id"));
      int attempt = str2taskAttemptNumber(hadoopJob.get("mapred.task.id"));

      // !#! Fix me...localhost to AM host and workbuilderPort as member variables?
      String workbuilderPort = System.getenv("SAILFISH_WORKBUILDER_PORT");
      ProcessBuilder pb = new ProcessBuilder(hadoopJob.get("sailfish.kappender.path"),
          "-l", hadoopJob.get("sailfish.kfs.buffer.limit", "0"),
          "-M", hadoopJob.get("sailfish.kfs.metaserver.host"),
          "-P", hadoopJob.get("sailfish.kfs.metaserver.port"),
          "-L", hadoopJob.get("sailfish.kappender.loglevel", "INFO"),
          "-J", hadoopJob.get("mapred.job.tracker.http.address") + "-" + jobId,
          // "-w", hadoopJob.get("sailfish.job.workbuilder.host"),
          // "-x", hadoopJob.get("sailfish.job.workbuilder.port"),
          "-w", "localhost", // !#! Fix me
          "-x", workbuilderPort,
          "-B", basedir.toString(),
          "-n", Integer.toString(hadoopJob.getNumReduceTasks()),
          "-i", Integer.toString(mapperId),
          "-a", Integer.toString(attempt));

      pb.redirectErrorStream(true);

      iappender = pb.start();

      iappenderStdout = new ChildOutputGrabber(iappender.getInputStream());
      iappenderStdout.start();

    }
  }

  // Thread that grabs output from kappender and dumps to log
  static class ChildOutputGrabber extends Thread {
    BufferedReader childStdout;
    ChildOutputGrabber(InputStream is) {
      childStdout = new BufferedReader(new InputStreamReader(is));
    }
    @Override
    public void run() {
      // Grab the child stdout and dump to log
      try {
        String line;
        while ((line = childStdout.readLine()) != null) {
          LOG.info("kappender: " + line);
        }
      } catch (Exception e) {
        LOG.warn("Child exited?");
      }
    }
  }

  @Override
  protected Mapper<K1, V1, K2, V2> getMapper() {
    // TODO Auto-generated method stub
    return mapper;
  }

  /*
   * Code taken from org.apache.hadoop.mapreduce.TaskAttemptID.forName()
   */
  static String SEPARATOR = "_";
  static String ATTEMPT = "attempt";

  public static int str2taskNumber(String str) throws IllegalArgumentException {
    if (str == null)
      return 0;
    try {
      String[] parts = str.split(SEPARATOR);
      if (parts.length == 6) {
        if (parts[0].equals(ATTEMPT)) {
          return Integer.parseInt(parts[4]);
        }
      }
    } catch (Exception ex) {
      // fall below
    }
    throw new IllegalArgumentException("TaskAttemptId string : " + str
        + " is not properly formed");
  }

  public static int str2taskAttemptNumber(String str) throws IllegalArgumentException {
    if (str == null)
      return 0;
    try {
      String[] parts = str.split(SEPARATOR);
      if (parts.length == 6) {
        if (parts[0].equals(ATTEMPT)) {
          return Integer.parseInt(parts[5]);
        }
      }
    } catch (Exception ex) {
      // fall below
    }
    throw new IllegalArgumentException("TaskAttemptId string : " + str
        + " is not properly formed");
  }

  private static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();
  static {
    NUMBER_FORMAT.setMinimumIntegerDigits(5);
    NUMBER_FORMAT.setGroupingUsed(false);
  }

  private synchronized String getOutputName(int partition) {
    return "part-" + NUMBER_FORMAT.format(partition);
  }
}
