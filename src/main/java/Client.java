import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by sicongfeng on 16/2/19.
 */
public class Client {
    // need to implement interface with YCSB

    public LinkedBlockingQueue<ClientReq> q; // might be necessary for accepting requests

    public boolean sendToScheduler(Txns t) { return false; }

    public void listen() {}; // Accepts YCSB requests

}

