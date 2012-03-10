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
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;


public class BenchmarkInputFormatNew extends TextInputFormat {
  protected static final Log LOG = LogFactory.getLog(BenchmarkInputFormatNew.class.getName());
  public List<InputSplit> getSplits(JobContext job) throws IOException {

    int numSplits = job.getConfiguration().getInt("mapred.map.tasks", 1);
    // For the benchmark case, the job generates data; we just need tasks
    // So, just get the info for 1 split and then fill in dummy for the rest
    List<InputSplit> desiredSplits = super.getSplits(job);
    // fill in dummy values for the remaining tasks that we need
    for (int i = desiredSplits.size(); i < numSplits; i++) {
      desiredSplits.add(desiredSplits.get(0));
    }
    return desiredSplits;
  }

  public boolean isSplitable(JobContext context, Path file) {
    return false;
  }

}
