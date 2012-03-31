package org.apache.hadoop.mapreduce.v2.app.job.event;

import org.apache.hadoop.mapreduce.v2.api.records.JobId;

public class JobSetWorkbuilderPortEvent extends JobEvent {
  private final int workbuilder_port;

  public JobSetWorkbuilderPortEvent(JobId jobID, int workbuilder_port) {
    super(jobID, JobEventType.JOB_SET_WORKBUILDER_PORT);
    this.workbuilder_port = workbuilder_port;
  }

  public int getWorkBuilderPort() {
    return workbuilder_port;
  }
}
