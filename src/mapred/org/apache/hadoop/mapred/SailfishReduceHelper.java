/**
 * Copyright 2010 Yahoo Corporation. All rights reserved. 
 * 
 * This file is part of the Sailfish project.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 * 
 */
package org.apache.hadoop.mapred;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.HttpURL;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapred.Task.TaskReporter;
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.util.Progressable;

class SailfishReduceHelper {
  static final private Log LOG = LogFactory.getLog(SailfishReduceHelper.class);

  // Thread that grabs output from imerger and dumps to log
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
          LOG.info("imerger: " + line);
        }
      } catch (Exception e) {
        LOG.warn("Child exited?");
      }
    }
  }

  static class Heartbeater extends Thread {
    Progressable reporter;
    private boolean done = false;
    private Integer mutex;

    Heartbeater(Progressable r) {
      reporter = r;
      mutex = new Integer(1);
    }

    @Override
    public void run() {
      // keep heartbeating hadoop until we are asked to stop
      while (!isDone()) {
        try {
          reporter.progress();
          // once a minute report progress
          Thread.sleep(60 * 1000);
        } catch (Exception e) {
        }
      }
      LOG.info("Heartbeater is done...exiting");
    }

    private boolean isDone() {
      synchronized (mutex) {
        return done;
      }
    }

    public void setDone() {
      synchronized (mutex) {
        done = true;
      }
    }
  }

  /*
   * Helper class that handles the interaction with the workbuilder and fires up
   * the imerger. This is code that is common to both the "old" and the "new"
   * reduce values iterators.
   */
  static class IMergerHelper {
    int partition;
    JobConf hadoopJob;
    String jobId;
    String debugJobId;
    DataInputStream imergerStdout;
    Process imerger;
    ChildOutputGrabber imergerStderr;

    IMergerHelper(int partition, JobConf jc, String jobId, String debugJobId) {
      this.partition = partition;
      this.hadoopJob = jc;
      this.jobId = jobId;
      this.debugJobId = debugJobId;
      if (this.debugJobId == null)
        this.debugJobId = jobId;
    }

    void notifyWorkbuilder() throws IOException {
      if (partition != 0)
        return;
      // reducer for partition 0 notifies the workbuilder to begin building the
      // plan
      // same as the mapside: jobtracker-jobid
      String jobname = hadoopJob.get("mapred.job.tracker.http.address") + "-"
          + jobId;
      String workbuilderHP = "http://"
          + hadoopJob.get("sailfish.job.workbuilder.host") + ":"
          + hadoopJob.get("sailfish.job.workbuilder.port");
      HttpURL url = new HttpURL(workbuilderHP);

      url.setPath("/buildplan/");
      url.setQuery(new String[] { "jobid", "reducerid" }, new String[] {
          jobname, Integer.toString(partition) });

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
          } catch (InterruptedException ie) {
          }
        }
      }
      if (code != 200) {
        throw new IOException("unable to notify workbuilder: " + errorStr);
      }
    }

    DataInputStream startIMerger() throws IOException {
      StringBuilder basedir = new StringBuilder("/jobs/" + debugJobId);
      ProcessBuilder pb = new ProcessBuilder(
          hadoopJob.get("sailfish.imerger.path"), "-M",
          hadoopJob.get("sailfish.kfs.metaserver.host"), "-P",
          hadoopJob.get("sailfish.kfs.metaserver.port"), "-J",
          hadoopJob.get("mapred.job.tracker.http.address") + "-" + jobId, "-w",
          hadoopJob.get("sailfish.job.workbuilder.host"), "-x",
          hadoopJob.get("sailfish.job.workbuilder.port"), "-B",
          basedir.toString(), "-i", Integer.toString(partition));

      imerger = pb.start();

      imergerStderr = new ChildOutputGrabber(imerger.getErrorStream());
      imergerStderr.start();
      // give the input stream a bigger buffer...256K
      imergerStdout = new DataInputStream(new BufferedInputStream(
          imerger.getInputStream(), 262144));
      return imergerStdout;
    }

    int getMaxRecords() throws IOException {
      int maxRecordCount = imergerStdout.readInt();
      LOG.info("For partition: " + partition + " expect a maximum of "
          + maxRecordCount);
      return maxRecordCount;
    }
  }

  /**
   * All we need here is what is done in ValuesIterator---except that, that
   * piece of code has an iterator that reads from a file. To simplify, we have
   * copied that bit of code here and made tweaks: fire up imerger and read from
   * its stdout.
   */
  static class SailfishReduceValuesIterator<KEY, VALUE> implements
      Iterator<VALUE> {
    private KEY key; // current key
    private KEY nextKey;
    private VALUE value; // current value
    private VALUE nextValue;
    private boolean hasNext; // more w/ this key
    private boolean more; // more in file
    private RawComparator<KEY> comparator;
    private TaskReporter reporter;
    private Progress reducePhase;
    private Deserializer<KEY> keyDeserializer;
    private Deserializer<VALUE> valDeserializer;
    // buffers to read key/value from socket
    private byte[] dataBuffer = new byte[4096];
    private byte[] valueBuffer = new byte[4096];
    private DataInputBuffer keyIn = new DataInputBuffer();
    private DataInputBuffer valueIn = new DataInputBuffer();
    private DataInputBuffer keyInCopy = new DataInputBuffer();
    private DataInputBuffer valueInCopy = new DataInputBuffer();
    private JobConf hadoopJob;
    private int maxRecordCount;
    private int totalRecordsRecd = 0;
    private int partition;
    private IMergerHelper imergerHelper;
    DataInputStream imergerStdout;
    private Counters.Counter reduceInputValueCounter;
    private String jobId;
    private String debugJobId;

    @SuppressWarnings("unchecked")
    public SailfishReduceValuesIterator(int partition,
        RawComparator<KEY> comparator, Class<KEY> keyClass,
        Class<VALUE> valClass, JobConf conf, TaskReporter reporter,
        Progress reducePhase, Counters.Counter riv) throws IOException {
      this.partition = partition;
      this.comparator = comparator;
      this.reporter = reporter;
      this.reducePhase = reducePhase;
      SailfishSerialization sfs = new SailfishSerialization();
      this.keyDeserializer = (Deserializer<KEY>) sfs
          .getDeserializer(keyClass);
      SerializationFactory serializationFactory = new SerializationFactory(conf);
      // this.keyDeserializer = serializationFactory.getDeserializer(keyClass);
      this.keyDeserializer.open(keyIn);
      this.valDeserializer = serializationFactory.getDeserializer(valClass);
      this.valDeserializer.open(this.valueIn);
      this.hadoopJob = conf;
      this.reduceInputValueCounter = riv;
      this.debugJobId = hadoopJob.get("sailfish.mapred.debug_job.id", null);
      this.jobId = hadoopJob.get("mapred.job.id");

      this.imergerHelper = new IMergerHelper(partition, conf, jobId, debugJobId);
      imergerHelper.notifyWorkbuilder();
      imergerStdout = imergerHelper.startIMerger();

      reporter.setStatus("Waiting to get max. records...");

      Heartbeater hb = new Heartbeater(reporter);
      hb.start();

      try {
        maxRecordCount = imergerHelper.getMaxRecords();
        hb.setDone();
      } catch (IOException e) {
        hb.setDone();
        throw e;
      }

      reporter.setStatus("Reduce starting");

      value = nextValue = null;

      getNextKV();

      key = nextKey;
      nextKey = null; // force new instance creation
      hasNext = more = (key != null);
    }

    // / Iterator methods
    public boolean hasNext() {
      return hasNext;
    }

    private int ctr = 0;

    public VALUE next() {
      if (!hasNext) {
        throw new NoSuchElementException("iterate past last value");
      }
      // Swap: getNextKV() will deserialize in-place in the object pointed to by
      // nextValue
      VALUE tmpValue = value;
      value = nextValue;
      nextValue = tmpValue;
      try {
        getNextKV();
      } catch (IOException ie) {
        throw new RuntimeException("problem advancing post rec#" + ctr, ie);
      }
      reduceInputValueCounter.increment(1);
      reporter.progress();
      return value;
    }

    public void remove() {
      throw new RuntimeException("not implemented");
    }

    // / Auxiliary methods

    /** Start processing next unique key. */
    void nextKey() throws IOException {
      // read until we find a new key
      while (hasNext) {
        getNextKV();
      }
      ++ctr;

      // move the next key to the current one
      KEY tmpKey = key;
      key = nextKey;
      nextKey = tmpKey;
      hasNext = more;
    }

    void close() {

    }

    /** True iff more keys remain. */
    boolean more() {
      return more;
    }

    /** The current key. */
    KEY getKey() {
      return key;
    }

    DataInputBuffer getKeyInputBuffer() {
      /*
       * LOG.info("keyinput buffer copy position = " + keyInCopy.getPosition() +
       * " ; length = " + keyInCopy.getLength());
       */
      return keyInCopy;
    }

    DataInputBuffer getValueInputBuffer() {
      return valueInCopy;
    }

    Progress getProgress() {
      return reducePhase;
    }

    /**
     * read the next key /value
     */
    private void getNextKV() throws IOException {
      int dataLen;
      try {
        // format: <key len><key><data len><data>
        dataLen = imergerStdout.readInt();
        if (dataLen == 0) {
          // we are all done; sanity check the close sequence
          int v = imergerStdout.readInt();
          if (v != 0xDEADDEAD) {
            LOG.info("Unknown close signature: " + v + "; expecting: "
                + 0xDEADDEAD);
          }
          v = imergerStdout.readInt();
          LOG.info("End of stream: # of records recd: " + totalRecordsRecd
              + " ; # of records sent by imerger: " + v);
          // so that when we report progress, it looks sensible
          maxRecordCount = totalRecordsRecd;
          hasNext = false;
          more = false;
          key = null;
          nextValue = null;
          return;
        }
        if (dataLen > dataBuffer.length) {
          dataBuffer = new byte[dataLen + 256];
        }
        imergerStdout.readFully(dataBuffer, 0, dataLen);
        keyInCopy.reset(dataBuffer, 0, dataLen);
        keyIn.reset(dataBuffer, 0, dataLen);
        /*
         * LOG.info("keyinput buffer position = " + keyIn.getPosition() +
         * " ; length = " + keyIn.getLength());
         */
        nextKey = keyDeserializer.deserialize(nextKey);
        hasNext = key != null && (comparator.compare(key, nextKey) == 0);
        dataLen = imergerStdout.readInt();
        if (dataLen > valueBuffer.length) {
          valueBuffer = new byte[dataLen + 256];
        }
        imergerStdout.readFully(valueBuffer, 0, dataLen);
        valueInCopy.reset(valueBuffer, 0, dataLen);
        valueIn.reset(valueBuffer, 0, dataLen);
        nextValue = valDeserializer.deserialize(nextValue);
        totalRecordsRecd++;
        if (totalRecordsRecd > maxRecordCount) {
          LOG.warn("Got too many records?: got = " + totalRecordsRecd
              + " ; expecting = " + maxRecordCount);
          throw new RuntimeException("Received too many records");
        }
      } catch (Exception e) {
        throw new IOException("Unable to get next KV", e);
      }
    }

    public void informReduceProgress() {
      if (maxRecordCount == 0) {
        reducePhase.set((float) 1.0);
        reporter.progress();
        return;
      }
      float progress = (float) Math.min(1.0, (float) totalRecordsRecd
          / (float) maxRecordCount);
      reducePhase.set(progress);
      reporter.progress();
    }
  }

  // This only works with the "new" style API
  static class SailfishNewReduceValuesIterator<KEY, VALUE> implements
      Iterator<VALUE> {
    private RawComparator<KEY> comparator;
    private TaskReporter reporter;
    private Progress reducePhase;
    private byte[] dataBuffer = new byte[4096];
    private byte[] valueBuffer = new byte[4096];
    private DataInputBuffer keyIn = new DataInputBuffer();
    private DataInputBuffer valueIn = new DataInputBuffer();

    private JobConf hadoopJob;
    private int maxRecordCount;
    private int totalRecordsRecd = 0;
    boolean hasMoreKeys = true;
    private int partition;
    private IMergerHelper imergerHelper;
    DataInputStream imergerStdout;
    private Counters.Counter reduceInputValueCounter;
    private String jobId;
    private String debugJobId;

    public SailfishNewReduceValuesIterator(int partition,
        RawComparator<KEY> comparator, Class<KEY> keyClass,
        Class<VALUE> valClass, JobConf conf, TaskReporter reporter,
        Progress reducePhase, Counters.Counter riv) throws IOException {
      this.partition = partition;
      this.comparator = comparator;
      this.reporter = reporter;
      this.reducePhase = reducePhase;
      this.hadoopJob = conf;
      this.reduceInputValueCounter = riv;
      this.debugJobId = hadoopJob.get("sailfish.mapred.debug_job.id", null);
      this.jobId = hadoopJob.get("mapred.job.id");
      this.imergerHelper = new IMergerHelper(partition, conf, jobId, debugJobId);
      imergerHelper.notifyWorkbuilder();
      imergerStdout = imergerHelper.startIMerger();

      reporter.setStatus("Waiting to get max. records...");

      Heartbeater hb = new Heartbeater(reporter);
      hb.start();

      try {
        maxRecordCount = imergerHelper.getMaxRecords();
        hb.setDone();
      } catch (IOException e) {
        hb.setDone();
        throw e;
      }

      reporter.setStatus("Reduce starting");

    }

    DataInputBuffer getKeyInputBuffer() {
      /*
       * LOG.info("keyinput buffer copy position = " + keyInCopy.getPosition() +
       * " ; length = " + keyInCopy.getLength());
       */
      return keyIn;
    }

    DataInputBuffer getValueInputBuffer() {
      return valueIn;
    }

    Progress getProgress() {
      return reducePhase;
    }

    /**
     * read the next key /value
     */
    public void getNextKV() throws IOException {
      int dataLen;
      try {
        // format: <key len><key><data len><data>
        dataLen = imergerStdout.readInt();
        if (dataLen == 0) {
          // we are all done; sanity check the close sequence
          int v = imergerStdout.readInt();
          if (v != 0xDEADDEAD) {
            LOG.info("Unknown close signature: " + v + "; expecting: "
                + 0xDEADDEAD);
          }
          v = imergerStdout.readInt();
          LOG.info("End of stream: # of records recd: " + totalRecordsRecd
              + " ; # of records sent by imerger: " + v);
          hasMoreKeys = false;
          // so that when we report progress, it looks sensible
          maxRecordCount = totalRecordsRecd;
          return;
        }
        if (dataLen > dataBuffer.length) {
          dataBuffer = new byte[dataLen + 256];
        }
        imergerStdout.readFully(dataBuffer, 0, dataLen);
        keyIn.reset(dataBuffer, 0, dataLen);
        dataLen = imergerStdout.readInt();
        if (dataLen > valueBuffer.length) {
          valueBuffer = new byte[dataLen + 256];
        }
        imergerStdout.readFully(valueBuffer, 0, dataLen);
        valueIn.reset(valueBuffer, 0, dataLen);
        totalRecordsRecd++;
        if (totalRecordsRecd > maxRecordCount) {
          LOG.warn("Got too many records?: got = " + totalRecordsRecd
              + " ; expecting = " + maxRecordCount);
          throw new RuntimeException("Received too many records");
        }
      } catch (Exception e) {
        throw new IOException("Unable to get next KV", e);
      }
    }

    public void informReduceProgress() {
      if (maxRecordCount == 0) {
        reducePhase.set((float) 1.0);
        reporter.progress();
        return;
      }
      float progress = (float) Math.min(1.0, (float) totalRecordsRecd
          / (float) maxRecordCount);
      reducePhase.set(progress);
      reporter.progress();
    }

    @Override
    public boolean hasNext() {
      return hasMoreKeys;
    }

    @Override
    public VALUE next() {
      return null;
    }

    @Override
    public void remove() {
      // TODO Auto-generated method stub

    }

    public void close() {

    }
  }

}
