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

@SuppressWarnings("serial")
class SailfishNumTasksPreemptServlet extends HttpServlet {

  JobTracker getJobTracker() {
    ServletContext context = getServletContext();
    return (JobTracker) context.getAttribute("job.tracker");
  }

  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    OutputStream out = response.getOutputStream();
    try {
      JobTracker tracker = getJobTracker();

      String requestJobID = request.getParameter("jobid");
      if (requestJobID == null) {
          response.sendError(HttpServletResponse.SC_BAD_REQUEST,
                                     "Argument jobid is required");
          return;
      }
      String taskType = request.getParameter("type");
      if (!("map".equals(taskType) || "reduce".equals(taskType))) {
        response.sendError(HttpServletResponse.SC_BAD_REQUEST,
            "Argument type is required");
        return;
      }

      JobInProgress job = null;
      StringBuilder buf = new StringBuilder();
      @SuppressWarnings("deprecation") // spurious
      JobID jobIdObj = JobID.forName(requestJobID);
      synchronized (tracker) {
        // all JIP transitions are irrelevant; just pull field
        job = tracker.getJob(jobIdObj);
      }

      if (job == null) {
        response.sendError(HttpServletResponse.SC_NOT_FOUND, "jobid not found");
        return;
      }

      buf.append(requestJobID)
         .append(":")
         .append(String.valueOf("map".equals(taskType)
               ? job.getMapPreemptionTarget()
               : job.getReducePreemptionTarget()));
      out.write(buf.toString().getBytes());
    } finally {
      out.close();
    }
  }
}
