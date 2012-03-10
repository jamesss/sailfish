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
package com.yahoo.labs.sailfish;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * This format is special: it is used by the benchmark job to get
 * tasks from Hadoop.  The mapper generates the data.  All we need is a single
 * file in the input dir to get the parameters setup right.
 * @author sriramr
 *
 */
public class BenchmarkInputFormat extends TextInputFormat {
  protected static final Log LOG = LogFactory.getLog(BenchmarkInputFormat.class.getName());
  @SuppressWarnings("deprecation")
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    TextInputFormat input = ReflectionUtils.newInstance(TextInputFormat.class, job);

    int numMaps = job.getNumMapTasks();
    LOG.info("# of maps from job: " + numMaps + "; # of splits for hadoop: " + numSplits);
    // For the benchmark case, the job generates data; we just need tasks
    // So, just get the info for 1 split and then fill in dummy for the rest
    InputSplit[] splits = input.getSplits(job, 1);
    InputSplit[] desiredSplits = new InputSplit[numSplits];
    LOG.info("splits.length = " + splits.length + "; desiredSplits.length = " + desiredSplits.length);
    System.arraycopy(splits, 0, desiredSplits, 0, splits.length);
    // fill in dummy values for the remaining tasks that we need
    for (int i = splits.length; i < numSplits; i++) {
      desiredSplits[i] = new FileSplit(((FileSplit)splits[0]).getPath(), 0, splits[0].getLength(), job);
    }

    return desiredSplits;
  }

  @SuppressWarnings("deprecation")
  public RecordReader getRecordReader(InputSplit genericSplit, JobConf jobConf,
      Reporter reporter) throws IOException {
    // TODO Auto-generated method stub
    return super.getRecordReader(genericSplit, jobConf, reporter);
  }
}
