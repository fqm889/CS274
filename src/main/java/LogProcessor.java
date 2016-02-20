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

    public LogProcessor(Log log, PT pt) {}

    public void checkLog() {}

    public void processLog() {}
}
