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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SailfishMapRunner;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public abstract class SailfishJob implements Tool {

  static final private Log LOG = LogFactory.getLog(SailfishJob.class);
  private Configuration config;
  protected JobConf jobConf;
  private JobClient jobClient;
  private RunningJob jobRunning;
  private JobID jobId;
  protected Options jobOptions;
  protected int maxMappers;
  protected int maxReducers;
  protected int childTaskMemory;
  protected int ioSortMB;
  protected String inputDir;
  protected String outputDir;
  protected boolean sendKey = false;
  protected boolean useIfiles = true;
  protected boolean disableReduce = false;
  // Contains the result of the parsed output
  protected CommandLine cmdLine;

  @Override
  public void setConf(Configuration conf) {
    config = conf;
  }

  @Override
  public Configuration getConf() {
    return jobConf;
  }

  Option createOption(String opt, String longOpt, String desc, String argName,
      int max, boolean required) {
    return OptionBuilder.withArgName(argName).hasArgs(max).withDescription(
        desc).isRequired(required).withLongOpt(longOpt).create(opt);
  }

  Option createBoolOption(String name, String desc) {
    return OptionBuilder.withDescription(desc).create(name);
  }

  // Setup a bunch of default options
  protected void setupOptions() {
    jobOptions = new Options();

    jobOptions
        .addOption(createBoolOption(
            "useStockHadoop",
            "use Stock Hadoop (default = false; we use Ifiles for storing intermediate data)"));
    jobOptions.addOption(createBoolOption("help", "print this help message"));
    jobOptions.addOption(createBoolOption("sendkey", "send key with each record (default=false)"));
    jobOptions.addOption(createBoolOption("disableReduce", "disable reduce phase of a job (default=false)"));
    jobOptions.addOption(createOption("m", "numMappers",
        "# of map tasks (default = 1)", "int", 1, false));
    jobOptions.addOption(createOption("r", "numReducers",
        "# of reduce tasks (default = 1)", "int", 1, false));
    jobOptions.addOption(createOption("i", "inputDir",
        "Dir containing input files for job", "String", 1, true));
    jobOptions.addOption(createOption("o", "outputDir",
        "Dir containing output files for job", "String", 1, true));
    jobOptions.addOption(createOption("M", "childJvmRAM",
        "For hadoop jobs, Xmx value in MB for child (map/reduce) tasks (default = 512)", "int", 512, false));
    jobOptions.addOption(createOption("S", "io.sort.mb",
        "For hadoop jobs, size of the sort buffer (default = 256MB)", "int", 256, false));

  }

  protected boolean parseOptions(String[] args) throws Exception {
    try {
      CommandLineParser parser = new BasicParser();
      cmdLine = parser.parse(jobOptions, args);
      inputDir = cmdLine.getOptionValue("i");
      outputDir = cmdLine.getOptionValue("o");
      maxMappers = Integer.parseInt(cmdLine.getOptionValue("m", "1"));
      maxReducers = Integer.parseInt(cmdLine.getOptionValue("r", "1"));
      childTaskMemory = Integer.parseInt(cmdLine.getOptionValue("M", "512"));
      ioSortMB = Integer.parseInt(cmdLine.getOptionValue("S", "256"));
      if (cmdLine.hasOption("useStockHadoop")) {
        LOG.info("Running with stock Hadoop; ifiles disabled");
        useIfiles = false;
      }
      if (cmdLine.hasOption("sendkey")) {
        sendKey = true;
      }
      if (cmdLine.hasOption("disableReduce")) {
        disableReduce = true;
      }
      return true;
    } catch (ParseException pe) {
      LOG.fatal("Unable to parse options!");
      return false;
    }
  }

  void printUsage() {
    // automatically generate the help statement
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp( "SailfishJob <mapper class> <reducer class> <options>", jobOptions );
  }
  public abstract void job(String... args);

  @Override
  public int run(String[] args) throws Exception {

    if (!parseOptions(args)) {
      printUsage();
      return -1;
    }

    jobConf = new JobConf(config);

    // set this to false if you want to use Hadoop sort for intermediate data
    jobConf.setBoolean("sailfish.mapred.job.use_ifile", useIfiles);
    // If we are using I-files, then we get dynamic-reduce (unless that is turned off)
    jobConf.setBoolean("sailfish.mapred.job.dynamic_reduce", useIfiles);

    jobConf.setNumMapTasks(maxMappers);
    // set the desired # of reduce tasks; if we are doing dynamic_reduce,
    // then the infra takes care of getting the right # of tasks
    jobConf.setNumReduceTasks(maxReducers);

    jobConf.set("sailfish.mapred.workbuilder", "localhost:12456");

    // call app for some parsing...; app can set things in jobconf that
    // we pick up here
    job();

    // jobConf.set("sailfish.kfs.buffer.limit", "0");

    if (sendKey) {
      jobConf.setBoolean("sailfish.mapred.job.send_key", true);
    }
    if (disableReduce) {
      jobConf.setBoolean("sailfish.mapred.job.disable_reduce", true);
    }
    if (jobConf.getCombinerClass() != null) {
      jobConf.setClass("sailfish.mapred.combiner.class",
          jobConf.getCombinerClass(), Reducer.class);
      // Clear it so that Hadoop doesn't try to do the combiner
      jobConf.setCombinerClass(null);
    }
    // If there are no reduce tasks, use stock Hadoop; simplifies our code
    if (jobConf.getNumReduceTasks() == 0) {
      jobConf.setBoolean("sailfish.mapred.job.use_ifile", false);
      // No I-files => no dynamic reduce
      jobConf.setBoolean("sailfish.mapred.job.dynamic_reduce", false);
    }
    if (jobConf.getBoolean("sailfish.mapred.job.use_ifile", true)) {
      // Use our maprunner only if the application wants to use I-files.
      jobConf.setMapRunnerClass(SailfishMapRunner.class);
      // Since we also have kappender, give us a bit more room
      // jobConf.setInt("mapred.job.map.memory.mb", 4096);
      // groupby uses a bit of memory...so, get us a bit of room
      // jobConf.setInt("mapred.job.reduce.memory.mb", 4096);
    } else {
      jobConf.set("mapred.child.java.opts", "-server -Xmx" + childTaskMemory +"m -Djava.net.preferIPv4Stack=true");
      jobConf.set("io.sort.mb", Integer.toString(ioSortMB));
    }

    jobConf.setUseNewMapper(false);
    jobConf.setUseNewReducer(false);
    jobConf.setSpeculativeExecution(false);
    jobConf.setJarByClass(SailfishJob.class);

    boolean error = true;

    jobClient = new JobClient(jobConf);

    try {
      jobRunning = jobClient.submitJob(jobConf);
      jobId = jobRunning.getID();

      LOG.info("getLocalDirs(): " + Arrays.asList(jobConf.getLocalDirs()));
      LOG.info("Running job: " + jobId);
      // jobInfo();
      String lastReport = null;

      while (!jobRunning.isComplete()) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
        }
        jobRunning = jobClient.getJob(jobId);
        String report = null;
        report = " map " + Math.round(jobRunning.mapProgress() * 100)
            + "%  reduce " + Math.round(jobRunning.reduceProgress() * 100)
            + "%";

        if (!report.equals(lastReport)) {
          LOG.info(report);
          lastReport = report;
        }
      }
      if (!jobRunning.isSuccessful()) {
        // jobInfo();
        LOG.error("Job not Successful!");
        return 1;
      }
      LOG.info("Job complete: " + jobId);
      LOG.info("Output: " /* + output_ */);
      error = false;
    } catch (FileNotFoundException fe) {
      LOG.error("Error launching job , bad input path : " + fe.getMessage());
      return 2;
    } catch (InvalidJobConfException je) {
      LOG.error("Error launching job , Invalid job conf : " + je.getMessage());
      return 3;
    } catch (IOException ioe) {
      LOG.error("Error Launching job : " + ioe.getMessage());
      return 4;
    } finally {
      if (error && (jobRunning != null)) {
        LOG.info("killJob...");
        jobRunning.killJob();
      }
      jobClient.close();
    }
    return 0;
  }



  public static void main(String[] args) throws Exception {
    if (args.length < 1) {
        System.out.println("Usage: SailfishJob <job class name> <job args> <job options>");
        return;
    }
    Class<?> claz = Class.forName(args[0]);
    Class<? extends SailfishJob> mainClass = claz.asSubclass(SailfishJob.class);
    SailfishJob theJob = (SailfishJob) mainClass.newInstance();
    theJob.setupOptions();
    String[] jobArgs = args;
    jobArgs = (jobArgs.length > 1) ? Arrays.copyOfRange(args, 1, jobArgs.length) : null;

    int returnStatus = ToolRunner.run(theJob, args);
    if (returnStatus != 0) {
        System.err.println("Sailfish Job Failed!");
        System.exit(returnStatus);
    }
}


}
