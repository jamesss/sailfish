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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class SailfishNumReduceSlotsOverCapacityServlet extends HttpServlet {
  // Workbuilder calls in here asking how many reduce slots it should give up.
  // We return back the # of slots the job is over capacity. The wb then asks
  // some # of reducers to exit. This allows us to do pre-emption.
  private static final long serialVersionUID = 957655739282794L;

  public static final Log LOG =
    LogFactory.getLog(SailfishNumReduceSlotsOverCapacityServlet.class);

  @Override
  // XXX: Should be a Post(); however, getting post with libcurl is a pain
  public void doGet(HttpServletRequest request, HttpServletResponse response)
  throws ServletException, IOException {
    OutputStream out = response.getOutputStream();
    ServletContext context = getServletContext();
    JobTracker tracker = (JobTracker) context.getAttribute("job.tracker");

    String requestJobID = request.getParameter("jobid");
    LOG.info("JobID: " + requestJobID);
    if (requestJobID == null) {
        response.sendError(HttpServletResponse.SC_BAD_REQUEST,
                                   "Argument jobid is required");
        return;
    }

    String status = null;
    String outString = null;
    synchronized (tracker) {
      JobID jobIdObj = JobID.forName(requestJobID);
      JobInProgress job = tracker.getJob(jobIdObj);
      if (job == null) {
        response.sendError(HttpServletResponse.SC_NOT_FOUND,
            "Couldn't find JIP: " + jobIdObj);
        return;
      }
      int numSlotsOverCapacity = job.getReducePreemptionTarget();
      outString = requestJobID + ":" + numSlotsOverCapacity;
    }
    LOG.info("RESPONSE: " + outString);

    out.write(outString.getBytes());

    out.close();
  }
}

