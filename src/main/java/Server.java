import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.*;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by sicongfeng on 16/2/19.
 */
class PendingKey {
    public final SelectionKey key;
    // operation: can be register for write or close the selectionkey
    public final int Op;

    PendingKey(SelectionKey key, int op) {
        this.key = key;
        Op = op;
    }

    public static final int OP_WRITE = -1;
}


public class Server implements Runnable {
    public LinkedBlockingQueue<Request> q;
    public ConcurrentHashMap<String, Request> activeTxns;
    public ServerSocketChannel serverChannel;
    public Selector selector;
    public ConcurrentLinkedQueue<PendingKey> pending = new ConcurrentLinkedQueue<PendingKey>();
    LatencySim latencySim; // simulates latency here

    public Server(String ip, int port) throws IOException {
        this.selector = Selector.open();
        this.serverChannel = ServerSocketChannel.open();
        serverChannel.configureBlocking(false);
        serverChannel.socket().bind(new InetSocketAddress(ip, port));
        serverChannel.register(selector, SelectionKey.OP_ACCEPT);
    }

    private void accept(SelectionKey key) {
        ServerSocketChannel ch = (ServerSocketChannel) key.channel();
        SocketChannel s;
        try {
            while ((s = ch.accept()) != null) {
                s.configureBlocking(false);
                SelectionKey k = s.register(selector, SelectionKey.OP_READ);
            }
        } catch (Exception e) {

        }
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

    public void run() {
        // Continuously listening to DC requests, if any arrives, immediately send to scheduler
        // while listening:
        //   if req arrive:
        //     sendToScheduler(req)
        while (true) {
            try {
                // send current keys in queue to scheduler
                if (selector.select() <= 0) {
                    continue;
                }
                Set<SelectionKey> selectedKeys = selector.selectedKeys();
                for (SelectionKey key : selectedKeys) {
                    if (!key.isValid()) {
                        continue;
                    }
                    if (key.isAcceptable()) {
                        accept(key);
                    }
                    // Yet need to implement sending method
                }
                selectedKeys.clear();
            } catch (ClosedSelectorException ignore) {
                return;
            } catch (Throwable e) {
            }
        }
    }
}
