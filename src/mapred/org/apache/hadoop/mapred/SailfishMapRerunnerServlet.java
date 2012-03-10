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

package org.apache.hadoop.mapred;

import java.io.IOException;
import java.io.OutputStream;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/*
 * Workbuilder notifies the JT that a map task has to be re-run.
 * This happens because a chunk containing data generated by that mapper
 * is now un-retrievable (that chunk is lost); so, we need to regenerate
 * the lost data.
 */
public class SailfishMapRerunnerServlet extends HttpServlet {
  private static final long serialVersionUID = 947685736282794L;
  @Override
  // XXX: Should be a Post(); however, getting post with libcurl is a pain
  public void doGet(HttpServletRequest request, HttpServletResponse response)
  throws ServletException, IOException {
    OutputStream out = response.getOutputStream();
    ServletContext context = getServletContext();
    JobTracker tracker = (JobTracker) context.getAttribute("job.tracker");

    String requestJobID = request.getParameter("jobid");
    if (requestJobID == null) {
        response.sendError(HttpServletResponse.SC_BAD_REQUEST,
                                   "Argument jobid is required");
        return;
    }

    String outString = null;

    synchronized(tracker) {
      JobID jobIdObj = JobID.forName(requestJobID);
      JobInProgress job = tracker.getJob(jobIdObj);
      if (job == null) {
        outString = requestJobID + ":" + "NOTFOUND";
      } else {
        boolean result = false;

        String mapperId = request.getParameter("id");

        try {
          TaskID mapTaskId = new TaskID(jobIdObj, true, Integer.valueOf(mapperId));
          TaskInProgress tip = tracker.getTip(mapTaskId);
          TaskAttemptID taid = tip.getSuccessfulTaskid();

          if (tip != null) {
            job.fetchFailureNotification(tip, taid, tip.machineWhereTaskRan(taid));
            result = true;
          }
        } catch (Exception e) {
          result = false;
        }
        outString = requestJobID + ":" + (result ? "SUCCEEDED" : "FAILED");
      }
    }

    out.write(outString.getBytes());

    out.close();
  }
}