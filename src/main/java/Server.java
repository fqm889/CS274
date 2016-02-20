import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by sicongfeng on 16/2/19.
 */
public class Server {
    public LinkedBlockingQueue<Request> q;
    LatencySim latencySim; // simulates latency here

    public Server() {}

    public void start() {
        // Continuously listening to DC requests, if any arrives, immediately send to scheduler
        // while listening:
        //   if req arrive:
        //     sendToScheduler(req)
    }

    public void sendToScheduler(Request req) {
        // Once received request from other DC, immediately send to scheduler
    }

    public Request checkForReq() {
        // check queue to see if there is requests to be sent
        return null;
    }

    public void sendToDC(Request req) {
        // Once found request for other DC, immediately send
    }
}
