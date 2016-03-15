import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by sicongfeng on 16/2/19.
 */
public class Scheduler {
    public Server receiver; // run in separate thread, for receiving data from other DC
    public Server sender; // run in separate thread, for sending data to other DC

    public Client client; // run in separate thread, for receiving data from client
    public LogProcessor logProcessor; // run in separate thread, deal with log
    public PTProcessor ptProcessor; // run in separate thread, deal with PT
    public Log log; // keep book, thread safe
    public PT pt; // keep book, thread safe

    public ArrayList<ArrayList<Timestamp>> T;
    public HashMap<String, Integer> DC2Num;

    public Scheduler() {
        // start receiver

        // start sender

        // start client

        // prepare Log and PT

        // prepare Processor
    }

    public void algorithm() {
        while (true) {
            // check receiver for new request from other DC

            // check client for new request from client

            // logProcessor

            // ptProcessor

            // send request to other DC
        }
    }

    public static void main(String[] args) {
        Scheduler scheduler = new Scheduler();
        scheduler.algorithm();
    }
}
