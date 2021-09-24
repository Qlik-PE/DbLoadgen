package qlikpe.dbloadgen.model.workload;

import java.util.Timer;
import java.util.TimerTask;

public class WorkerTimeoutTask extends TimerTask {
    private final Thread t;
    private final Timer timer;

    WorkerTimeoutTask(Thread t, Timer timer){
        this.t = t;
        this.timer = timer;
    }

    @Override
    public void run() {
        if (t != null && t.isAlive()) {
            t.interrupt();
            timer.cancel();
        }
    }
}
