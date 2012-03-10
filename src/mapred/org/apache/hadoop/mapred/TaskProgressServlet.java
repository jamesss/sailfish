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

import javax.servlet.ServletException;
import javax.servlet.ServletContext;
import javax.servlet.http.*;


/**
 * A simple servlet to dump job counters
 */
public class TaskProgressServlet extends HttpServlet {
  
  /**
   * 
   */
  private static final long serialVersionUID = 229274736282892L;

  /**
   * Get the counters via http.
   */
  public void doGet(HttpServletRequest request, 
                    HttpServletResponse response
                    ) throws ServletException, IOException {

    OutputStream out = response.getOutputStream();
    ServletContext context = getServletContext();
    JobTracker jt = (JobTracker) context.getAttribute("job.tracker");
    String requestJobID = request.getParameter("jobid");
    if (requestJobID == null) {
      response.sendError(HttpServletResponse.SC_BAD_REQUEST, 
      "Argument jobid is required");
      return;
    }
    JobID jobId = JobID.forName(requestJobID);

    JobInProgress jp = jt.getJob(jobId);
    if (jp == null) {
        String outString = requestJobID + ":" + "NOTFOUND";
        out.write(outString.getBytes());
    } else {  
        
        TaskInProgress mapTasks[] = jp.getMapTasks();
        for (TaskInProgress mapTIP : mapTasks) {
          TaskStatus[] taskStatuss = jt.getTaskStatuses(mapTIP.getTIPId());
          for (TaskStatus ts : taskStatuss) {
            if (ts.getRunState() == TaskStatus.State.RUNNING) {
              double progress = ts.getProgress();
              String outString = ts.getTaskID() + "\t" + progress + "\n";
              out.write(outString.getBytes());
            }  
          }  
        }
        
        TaskInProgress reduceTasks[] = jp.getReduceTasks();
        for (TaskInProgress reduceTIP : reduceTasks) {
          TaskStatus[] taskStatuss = jt.getTaskStatuses(reduceTIP.getTIPId());
          for (TaskStatus ts : taskStatuss) {
            if (ts.getRunState() == TaskStatus.State.RUNNING) {
              double progress = ts.getProgress();
              String outString = ts.getTaskID() + "\t" + progress + "\n";
              out.write(outString.getBytes());
            }  
          }
        }
    }    
    out.close();
  }
}
