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

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Iterator;
import java.util.Random;
import java.util.StringTokenizer;

import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
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
import org.apache.hadoop.mapred.lib.IdentityMapper;

import com.yahoo.labs.sailfish.WordCount.WordCountMap;
import com.yahoo.labs.sailfish.WordCount.WordCountReduce;


/**
 * Simple program that generates key/value pairs and can be used to test Sailfish.
 * The mapper generates random data (ala Daytona sort input) and the data makes it
 * way to the reducer.  The reducer throws the data away after doing some minor validation.
 * The # of mappers/reducers as well as the # of records to generate are parametrizable.
 *
 * @author sriramr
 *
 */
public class Benchmark extends SailfishJob {

  static final private Log LOG = LogFactory.getLog(Benchmark.class);
  int totalBytesPow2 = 8;
  boolean readInput = false;
  boolean writeOutput = false;

  protected void setupOptions() {
    super.setupOptions();
    jobOptions.addOption(createOption("B", "numBytes",
        "Total # of bytes to generate (default = 8---512)", "int (code applies power of 2)", 1,
        false));
    jobOptions.addOption(createBoolOption("readInput", "mapper: read input from file"));
    jobOptions.addOption(createBoolOption("writeOutput", "reducer: write output to file"));
  }

  protected boolean parseOptions(String[] args) throws Exception {
    try {
      super.parseOptions(args);
      totalBytesPow2 = Integer.parseInt(cmdLine.getOptionValue("B", "8"));
      if (cmdLine.hasOption("readInput")) {
        readInput = true;
      }
      if (cmdLine.hasOption("writeOutput")) {
        writeOutput = true;
      }
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  public void job(String... args) {

    jobConf.setMapperClass(BenchmarkMap.class);
    jobConf.setReducerClass(BenchmarkReduce.class);

    // Use our own format so that we can get tasks for the mappers
    jobConf.setInputFormat(BenchmarkInputFormat.class);
    // This is not required.  Let Hadoop figure this out.
    // jobConf.setOutputFormat(TextOutputFormat.class);
    jobConf.setOutputKeyClass(Text.class);
    jobConf.setOutputValueClass(Text.class);
    if (!readInput) {
      // Tell the framework to call map without bothering about input
      jobConf.setBoolean("sailfish.job.map_ignores_input", true);
    } else {
      jobConf.setBoolean("sailfish.job.map_ignores_input", false);
      // do a pass thru
      jobConf.setMapperClass(BenchmarkIdentityMap.class);
      // disable splitting here so that each task processes one file
      jobConf.setInputFormat(BenchmarkTextInputFormat.class);
    }

    if (writeOutput) {
      // Tell the reducer to save output
      jobConf.setBoolean("benchmark.save_output", true);
    }

    long bytesPerMapper = (long) (((long) 1) << totalBytesPow2) / (long) maxMappers;
    long recsPerMapper = bytesPerMapper / 100;
    jobConf.setLong("sailfish.job.benchmark.records_per_mapper", recsPerMapper);
    LOG.info("# of records per mapper: " + recsPerMapper);
    try {
      final FileSystem fs = FileSystem.get(jobConf);
      Path outputPath = new Path(outputDir);
      if (fs.exists(outputPath))
        fs.delete(outputPath, true);
      if (!readInput) {
        // Create a dummy file in the input dir.  We can use the info about
        // this file to create map tasks.
        Path dummyFile = new Path(inputDir + "/dummy.txt");
        if (!fs.exists(dummyFile)) {
          FSDataOutputStream fd = fs.create(dummyFile);
          fd.writeBytes("This is a dummy file".toString());
          fd.close();
        }
      }
    } catch (Exception e) { }

    FileInputFormat.setInputPaths(jobConf, new Path(inputDir));
    FileOutputFormat.setOutputPath(jobConf, new Path(outputDir));
  }

  static byte[] computeChecksum(String filename) throws Exception {
    InputStream fis = new FileInputStream(filename);

    byte[] buffer = new byte[1024];
    MessageDigest complete = MessageDigest.getInstance("MD5");
    int numRead;
    do {
      numRead = fis.read(buffer);
      if (numRead > 0) {
        complete.update(buffer, 0, numRead);
      }
    } while (numRead != -1);
    fis.close();
    return complete.digest();
  }

  // see this How-to for a faster way to convert
  // a byte array to a HEX string
  static String getMD5Checksum(String filename) throws Exception {
    byte[] b = computeChecksum(filename);
    String result = "";
    for (int i = 0; i < b.length; i++) {
      result += Integer.toString((b[i] & 0xff) + 0x100, 16).substring(1);
    }
    return result;
  }

  public static class BenchmarkMap extends MapReduceBase implements
      Mapper<LongWritable, Text, Text, Text> {

    long seed;
    long numRecords;
    int keyLen, valLen;
    BenchmarkKV keyValueGenerator;
    String kappenderMD5;

    public void configure(JobConf jc) {
      numRecords = jc.getLong("sailfish.job.benchmark.records_per_mapper", 10);

      String taskId = jc.get("mapred.task.id");
      seed = Long.parseLong(taskId.split("_")[4]) * (long) numRecords;
      keyValueGenerator = new RandomKV(seed, jc.getInt("benchmark.max_record_length", 100));
      // keyValueGenerator = new DaytonaSortKV(seed, jc.getInt("benchmark.max_record_length", 100));
      keyLen = jc.getInt("benchmark.key_length", 10);
      valLen = jc.getInt("benchmark.value_length", 90);
      kappenderMD5 = jc.get("sailfish.kappender.md5", "");
      LOG.info("Seeding random generator with: " + seed);
    }

    void validateMD5() throws Exception {
      if (kappenderMD5 == "")
        return;
      String binaryCksum = Benchmark.getMD5Checksum("/grid/0/dev/sriramr/code/mrhash/kfs/bin/sailfish/kappender");
      if (!binaryCksum.equalsIgnoreCase(kappenderMD5)) {
        throw new IOException("Cksum mismatch in kappender binary: expect = "
            + kappenderMD5 + "; computed = " + binaryCksum + ";");
      }
    }

    public void map(LongWritable key, Text value,
        OutputCollector<Text, Text> collector, Reporter reporter)
        throws IOException {
      try {
        validateMD5();
      } catch (Exception e) {
        throw new IOException("Unable to validate kappender MD5:", e);
      }

      // along the lines of Daytona sort, generate 10-byte key, 90-byte value
      Text k, v;

      k = new Text();
      v = new Text();
      for (long i = 0; i < numRecords; i++) {
        keyValueGenerator.generateKey(keyLen);
        k.set(keyValueGenerator.getData(), 0, keyLen);
        keyValueGenerator.generateValue(i + seed, valLen);
        v.set(keyValueGenerator.getData(), 0, valLen);

        collector.collect(k, v);
        reporter.progress();
      }
      LOG.info("Done with generating: " + numRecords);
    }
  }

  public static class BenchmarkIdentityMap extends MapReduceBase implements
  Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text value,
        OutputCollector<Text, Text> collector, Reporter reporter)
    throws IOException {
      String[] parts = value.toString().split("\t");
      Text k = new Text(parts[0]);
      Text v = new Text(parts[1]);
      collector.collect(k, v);
    }
  }

  public static class BenchmarkReduce extends MapReduceBase implements
      Reducer<Text, Text, Text, Text> {
    static final private Log LOG = LogFactory.getLog(BenchmarkReduce.class);
    int keyLen, valLen;
    boolean saveOutput;

    public void configure(JobConf jc) {
      keyLen = jc.getInt("benchmark.key_length", 10);
      valLen = jc.getInt("benchmark.value_length", 90);
      saveOutput = jc.getBoolean("benchmark.save_output", false);
    }

    public void reduce(Text key, Iterator<Text> values,
        OutputCollector<Text, Text> output, Reporter reporter)
        throws IOException {
      // Don't emit any output for now
      // validate that key/values are sane
      byte[] keyBytes = key.getBytes();
      if (keyBytes.length != keyLen) {
        throw new IOException("Got key with bogus length = " + keyBytes.length);
      }
      while (values.hasNext()) {
        Text v = values.next();
        byte[] valueBytes = v.getBytes();
        if (valueBytes.length != valLen) {
          throw new IOException("Got value with bogus length = " + valueBytes.length);
        }
        if (saveOutput)
          output.collect(key, v);
      }
    }
  }
}
