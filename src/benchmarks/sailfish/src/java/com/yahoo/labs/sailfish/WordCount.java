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
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

/**
 * WordCount job that can be used to test Sailfish.
 * @author sriramr
 *
 */
public class WordCount extends SailfishJob {

  protected boolean parseOptions(String[] args) throws Exception {
    return super.parseOptions(args);
  }
  @Override
  public void job(String... args) {

    jobConf.setMapperClass(WordCountMap.class);
    jobConf.setReducerClass(WordCountReduce.class);

    jobConf.setInputFormat(TextInputFormat.class);
    jobConf.setOutputFormat(TextOutputFormat.class);
    jobConf.setOutputKeyClass(Text.class);
    jobConf.setOutputValueClass(IntWritable.class);

    try {
      final FileSystem fs = FileSystem.get(jobConf);
      Path outputPath = new Path(outputDir);
      if (fs.exists(outputPath))
        fs.delete(outputPath, true);
    } catch (Exception e) { }

    FileInputFormat.setInputPaths(jobConf, new Path(inputDir));
    FileOutputFormat.setOutputPath(jobConf, new Path(outputDir));
  }

  public static class WordCountMap extends MapReduceBase implements
      Mapper<LongWritable, Text, Text, IntWritable> {
    private final IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void configure(JobConf jc) {

    }

    public void map(LongWritable key, Text value,
        OutputCollector<Text, IntWritable> collector, Reporter reporter)
        throws IOException {
      // TODO application code
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        collector.collect(word, one);
      }
    }
  }

  public static class WordCountReduce extends MapReduceBase implements
      Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();

    public void configure(JobConf jc) {

    }

    public void reduce(Text key, Iterator<IntWritable> values,
        OutputCollector<Text, IntWritable> output, Reporter reporter)
        throws IOException {
      // TODO application code
      int sum = 0;
      while (values.hasNext()) {
        sum += values.next().get();
      }
      result.set(sum);
      output.collect(key, result);
    }
  }




}
