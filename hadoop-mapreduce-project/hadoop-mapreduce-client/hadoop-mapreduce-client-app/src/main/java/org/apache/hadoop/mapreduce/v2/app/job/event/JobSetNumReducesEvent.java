package org.apache.hadoop.mapreduce.v2.app.job.event;

import org.apache.hadoop.mapreduce.v2.api.records.JobId;

public class JobSetNumReducesEvent extends JobEvent {

  private final int numReduces;

  public JobSetNumReducesEvent(JobId jobID, int numReduces) {
    super(jobID, JobEventType.JOB_SET_REDUCES);
    this.numReduces = numReduces;
  }

  public int getNumReduces() {
    return numReduces;
  }
}
