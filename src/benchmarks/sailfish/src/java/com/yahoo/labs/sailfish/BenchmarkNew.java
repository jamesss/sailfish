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
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Benchmark program rewritten using the "new" MapReduce api.
 */
public class BenchmarkNew {
  public static class BenchmarkMapper
  extends Mapper<LongWritable, Text, Text, Text>{
    static final private Log LOG = LogFactory.getLog(BenchmarkMapper.class);
    long seed;
    long numRecords;
    int keyLen, valLen;
    byte[] data;
    BenchmarkKV keyValueGenerator;

    public void setup(Context jobContext) {
      Configuration jc = jobContext.getConfiguration();

      numRecords = jc.getLong("sailfish.job.benchmark.records_per_mapper", 10);
      data = new byte[jc.getInt("benchmark.max_record_length", 100)];
      keyLen = jc.getInt("benchmark.key_length", 10);
      valLen = jc.getInt("benchmark.value_length", 90);
      String taskId = jc.get("mapred.task.id");
      seed = Long.parseLong(taskId.split("_")[4]) * (long) numRecords;
      // keyValueGenerator = new RandomKV(seed, jc.getInt("benchmark.max_record_length", 100));
      keyValueGenerator = new DaytonaSortKV(seed, jc.getInt("benchmark.max_record_length", 100));
    }

    public void map(LongWritable key, Text value, Context context
    ) throws IOException, InterruptedException {
      // along the lines of Daytona sort, generate 10-byte key, 90-byte value
      Text k, v;

      k = new Text();
      v = new Text();
      for (long i = 0; i < numRecords; i++) {
        keyValueGenerator.generateKey(keyLen);
        k.set(keyValueGenerator.getData(), 0, keyLen);
        keyValueGenerator.generateValue(i + seed, valLen);
        v.set(keyValueGenerator.getData(), 0, valLen);

        context.write(k, v);
        context.progress();
      }
      LOG.info("Done with generating: " + numRecords);
    }
  }

  public static class BenchmarkReducer
  extends Reducer<Text, Text, Text, Text> {
    static final private Log LOG = LogFactory.getLog(BenchmarkReducer.class);
    int keyLen, valLen;
    boolean saveOutput;

    public void setup(Context jobContext) {
      Configuration jc = jobContext.getConfiguration();

      keyLen = jc.getInt("benchmark.key_length", 10);
      valLen = jc.getInt("benchmark.value_length", 90);
      saveOutput = jc.getBoolean("benchmark.save_output", false);
    }

    public void reduce(Text key, Iterable<Text> values,
        Context context
    ) throws IOException, InterruptedException {
      // Don't emit any output for now
      // validate that key/values are sane
      byte[] keyBytes = key.getBytes();
      if (keyBytes.length != keyLen) {
        throw new IOException("Got key with bogus length = " + keyBytes.length);
      }
      for (Text v : values) {
        byte[] valueBytes = v.getBytes();
        if (valueBytes.length != valLen) {
          throw new IOException("Got value with bogus length = " + valueBytes.length);
        }
        if (saveOutput)
          context.write(key, v);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: benchmarknew <in> <out>");
      System.exit(2);
    }
    // These sailfish configs need to be in hadoop-site
    boolean useIfiles = false;
    // set this to false if you want to use Hadoop sort for intermediate data
    conf.setBoolean("sailfish.mapred.job.use_ifile", useIfiles);
    // If we are using I-files, then we get dynamic-reduce (unless that is turned off)
    conf.setBoolean("sailfish.mapred.job.dynamic_reduce", useIfiles);

    Job job = new Job(conf, "benchmark");
    job.setJarByClass(BenchmarkNew.class);
    job.setMapperClass(BenchmarkMapper.class);
    job.setReducerClass(BenchmarkReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setInputFormatClass(BenchmarkInputFormatNew.class);
    BenchmarkInputFormatNew.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
