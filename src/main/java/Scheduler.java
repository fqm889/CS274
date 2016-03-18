import Operations.Operation;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by sicongfeng on 16/2/19.
 */
public class Scheduler {
    public ScheServer receiver; // run in separate thread, for receiving data from other DC
    public Server sender; // run in separate thread, for sending data to other DC

    public ClientServer client; // run in separate thread, for receiving data from client
    public LogProcessor logProcessor; // run in separate thread, deal with log
    public PTProcessor ptProcessor; // run in separate thread, deal with PT
    public Log log; // keep book, thread safe
    public PT pt; // keep book, thread safe

    public String DCName;
    public int DCNum;

    public ArrayList<ArrayList<Timestamp>> T;
    public HashMap<String, Integer> DC2Num;
    public ConcurrentHashMap<String, Txns> currentTxns;

    public Scheduler() {
        // start receiver
        try {
            receiver = new ScheServer("0.0.0.0", 8888, this);
        } catch (IOException e) {
            e.printStackTrace();
        }
        Thread tServer = new Thread(receiver);
        tServer.start();

        try {
            client = new ClientServer("0.0.0.0", 9999, this);
        } catch (IOException e) {
            e.printStackTrace();
        }
        Thread cServer = new Thread(client);
        cServer.start();
        // start sender

        // start client

        // prepare Log and PT
        log = new Log();
        pt = new PT();

        // prepare Processor
        logProcessor = new LogProcessor(log, pt);
        ptProcessor = new PTProcessor(log, pt);
        currentTxns = new ConcurrentHashMap<String, Txns>();
    }

    public void algorithm() {
        DBS db = new DBS("127.0.0.1", 9999);
        try {
            db.start();
        } catch (DBException e) {
            e.printStackTrace();
        }
        int count = 0;

        while (true) {
            // check receiver for new request from other DC
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (count++<10)
                db.read("a", "a", new HashSet<String>(), new HashMap<String, ByteIterator>());
            // check client for new request from client

            // logProcessor

            // ptProcessor

            // send request to other DC
        }
    }

    public void addTxn(String txn_id) {
        System.out.println("Into Addtxn");
        if (!currentTxns.contains(txn_id)) {
            System.out.println("Add Txn2222222222222");
            currentTxns.put(txn_id, new Txns());
        }
        System.out.println("Finishing AddTxn");
    }

    public QueryResult addOneOpTxn(String txn_id, Operation op) {
        System.out.println("Add One Op" + txn_id);
        if (currentTxns.containsKey(txn_id)) {
            System.out.println("current txn");
            Txns txns = currentTxns.get(txn_id);
            // txns.AddOp(op);
            // execute read operations immediately
            return new QueryResult(txn_id, "1", null);
        }
        return null;
    }

    public static void main(String[] args) {
        Scheduler scheduler = new Scheduler();
        System.out.println("START");
        scheduler.algorithm();
    }
}
