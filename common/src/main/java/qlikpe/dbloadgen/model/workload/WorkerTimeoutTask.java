/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package qlikpe.dbloadgen.model.workload;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Timer;
import java.util.TimerTask;

public class WorkerTimeoutTask extends TimerTask {
    private final static Logger LOG = LogManager.getLogger(WorkerTimeoutTask.class);

    private final Thread t;
    private final Timer timer;
    private final String name;

    WorkerTimeoutTask(String name, Thread t, Timer timer){
        this.name = name;
        this.t = t;
        this.timer = timer;
    }

    @Override
    public void run() {
        LOG.debug("thread timer has expired: {}", name);
        if (t != null && t.isAlive()) {
            t.interrupt();
            timer.cancel();
        } else {
            LOG.debug("thread timer: {} if condition failed", name);
        }
    }
}
