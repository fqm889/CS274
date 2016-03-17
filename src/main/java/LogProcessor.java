import java.util.ArrayList;

/**
 * Created by sicongfeng on 16/2/19.
 */
public class LogProcessor {
    // pass in the Log of Scheduler, process log every time the Scheduler calls it
    // executes on its own list, scheduler may access its list and decide which one to send

    public Log log;
    public PT pt;
    public ArrayList<Txns> waitinglist;

    public LogProcessor(Log log, PT pt) {
        this.log = log;
        this.pt = pt;
    }

    public void checkLog() {}

    public void processLog() {
        ArrayList<Txns> ls = log.getLog();
        for (Txns l : ls) {
            if ((l.isProcessed() && !l.isPending()) || l.isLocal()) { continue; }
            for (Txns t : pt.getPt()) {
                if (l.checkForConflict(t)) {
                    t.abort();
                }
            }
            if (l.getState()==txnsState.COMMIT) {
                l.write();
            }
        }
    }

}
