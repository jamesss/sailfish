/**
 * Copyright 2010 Yahoo Corporation.  All rights reserved.
 * This file is part of the Sailfish project.
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
import java.util.Collection;
import java.util.Iterator;

import javax.servlet.ServletException;
import javax.servlet.ServletContext;
import javax.servlet.http.*;


import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.Counters.Group;

/**
 * A simple servlet to dump job counters
 */
public class JobCountersServlet extends HttpServlet {
  
  /**
   * 
   */
  private static final long serialVersionUID = 229274736282792L;

  /**
   * Get the counters via http.
   */
  public void doGet(HttpServletRequest request, 
                    HttpServletResponse response
                    ) throws ServletException, IOException {

    OutputStream out = response.getOutputStream();
    ServletContext context = getServletContext();
    JobTracker jt = (JobTracker) context.getAttribute("job.tracker");
    
    synchronized (jt) {
      String requestJobID = request.getParameter("jobid");
      if (requestJobID == null) {
        response.sendError(HttpServletResponse.SC_BAD_REQUEST, 
        "Argument jobid is required");
        return;
      }

      JobID jobIdObj = JobID.forName(requestJobID);
      JobInProgress job = jt.getJob(jobIdObj);

      if (job == null) {
        String outString = requestJobID + ":" + "NOTFOUND";
        out.write(outString.getBytes());
      } else {  
        TaskInProgress mapTasks[] = job.getMapTasks();
        for (TaskInProgress mapTIP : mapTasks) {
          Counters counters = mapTIP.getCounters();
          StringBuilder sb = new StringBuilder(mapTIP.getTIPId() + "\n");
          Collection<String> groupNames = counters.getGroupNames();
          for (String groupName : groupNames) {
            Group group = counters.getGroup(groupName);
            sb.append("\n\t" + group.getDisplayName());
            Iterator<Counter> subCounters = group.iterator();
            while (subCounters.hasNext()) {
              Counter subCounter = subCounters.next();
              sb.append("\n\t\t" + subCounter.getDisplayName() + "="
                  + subCounter.getCounter());
              String outString = mapTIP.getTIPId() + ":" + group.getDisplayName()
              + ":" + subCounter.getDisplayName() + "="
              + subCounter.getCounter() + "\n";
              out.write(outString.getBytes());
            }
            //out.write(sb.toString().getBytes());
          }
        }

        TaskInProgress reduceTasks[] = job.getReduceTasks();
        for (TaskInProgress reduceTIP : reduceTasks) {
          Counters counters = reduceTIP.getCounters();
          StringBuilder sb = new StringBuilder(reduceTIP.getTIPId() + "\n");
          Collection<String> groupNames = counters.getGroupNames();
          for (String groupName : groupNames) {
            Group group = counters.getGroup(groupName);
            sb.append("\n\t" + group.getDisplayName());
            Iterator<Counter> subCounters = group.iterator();
            while (subCounters.hasNext()) {
              Counter subCounter = subCounters.next();
              sb.append("\n\t\t" + subCounter.getDisplayName() + "="
                  + subCounter.getCounter());
              String outString = reduceTIP.getTIPId() + ":" + group.getDisplayName()
              + ":" + subCounter.getDisplayName() + "="
              + subCounter.getCounter() + "\n";
              out.write(outString.getBytes());
            }
          }
          //out.write(sb.toString().getBytes());
        }
      }    
    }
    out.close();
  }
}
