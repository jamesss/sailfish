/**
 * Copyright (c) 2011 Yahoo! Inc. All rights reserved. 
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapred;

import java.io.IOException;
import java.io.OutputStream;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Given a jobid, this Servlet returns the status of that job.
 * @author sriramr
 *
 */
public class JobInfoServlet extends HttpServlet {
  private static final long serialVersionUID = 324485738282754L;
  
  @Override
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

    String status = null;
    String outString = null;
    synchronized(tracker) {
      JobID jobIdObj = JobID.forName(requestJobID);
      JobInProgress job = tracker.getJob(jobIdObj);
      if (job == null) {
        outString = requestJobID + ":" + "NOTFOUND";
      } else {
        if (job.getStatus().getRunState() == JobStatus.RUNNING)
          outString = requestJobID + ":" + "RUNNING";
        if (job.getStatus().getRunState() == JobStatus.FAILED)
          outString = requestJobID + ":" + "FAILED";
        if (job.getStatus().getRunState() == JobStatus.SUCCEEDED)
          outString = requestJobID + ":" + "SUCCEEDED";
        if (job.getStatus().getRunState() == JobStatus.KILLED)
          outString = requestJobID + ":" + "KILLED";
      }
    }

    out.write(outString.getBytes());

    out.close();
  } 
}
