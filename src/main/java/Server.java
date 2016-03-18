import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.Iterator;
import java.util.LinkedList;
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
    public Scheduler scheduler;

    public LinkedBlockingQueue<Request> q;

    public ServerSocketChannel serverChannel;
    public Selector selector;
    public ConcurrentLinkedQueue<PendingKey> pending = new ConcurrentLinkedQueue<PendingKey>();

    LatencySim latencySim; // simulates latency here

    public Server(String ip, int port, Scheduler scheduler) throws IOException {
        this.scheduler = scheduler;
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
                System.out.println("Accepted.");
                s.configureBlocking(false);
                SelectionKey k = s.register(selector, SelectionKey.OP_READ, new Attn());
            }
        } catch (Exception e) {

        }
    }

    public byte[] decode(ByteBuffer buffer) {
        // Once received request from other DC, immediately send to scheduler
        return null;
    }

    public Request checkForReq() {
        // check queue to see if there is requests to be sent
        return null;
    }


    public void doRead(SelectionKey key) {
        SocketChannel ch = (SocketChannel) key.channel();
        System.out.println("Got channel");
        try {
            Attn attn = (Attn) key.attachment();
            System.out.println("Got Attn");
            ByteBuffer buffer = attn.buffer;
            buffer.clear();
            int read = ch.read(buffer);
            if (read == -1) {
                // close key
            }
            else if (read > 0) {
                buffer.flip();
                byte[] res = decode(buffer);
                System.out.println("Finished decode");
                if (res != null)
                    attn.toWrite.add(ByteBuffer.wrap(res));
                pending.add(new PendingKey(key, SelectionKey.OP_WRITE));
                System.out.println("Add to pending");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void doWrite(SelectionKey key) {
        SocketChannel ch = (SocketChannel) key.channel();
        Attn attn = (Attn) key.attachment();
        System.out.println("Got channel in Write");
        synchronized (attn) {
            LinkedList<ByteBuffer> toWrites = attn.toWrite;
            int size = toWrites.size();
            if (size == 1) {
                try {
                    ch.write(toWrites.get(0));
                } catch (IOException e) {
//                    e.printStackTrace();
                }
            } else if (size > 0) {
                ByteBuffer buffers[] = new ByteBuffer[size];
                toWrites.toArray(buffers);
                try {
                    ch.write(buffers, 0, buffers.length);
                } catch (IOException e) {
//                    e.printStackTrace();
                }
            }
            Iterator<ByteBuffer> ite = toWrites.iterator();
            while (ite.hasNext()) {
                if (!ite.next().hasRemaining()) {
                    ite.remove();
                }
            }
        }
        key.interestOps(SelectionKey.OP_READ);
    }

    public void run() {
        // Continuously listening to DC requests, if any arrives, immediately send to scheduler
        // while listening:
        //   if req arrive:
        //     sendToScheduler(req)
        while (true) {
            try {
                // send current keys in queue to scheduler
                PendingKey k;
                while (!pending.isEmpty()) {
                    System.out.println("Pending not empty");
                    k = pending.poll();
                    if (k.Op == SelectionKey.OP_WRITE) {
                        k.key.interestOps(SelectionKey.OP_WRITE);
                    }
                }

                if (selector.select() <= 0) {
                    continue;
                }
                Set<SelectionKey> selectedKeys = selector.selectedKeys();
                for (SelectionKey key : selectedKeys) {
                    System.out.println("Selector Found");
                    if (!key.isValid()) {
                        continue;
                    }
                    if (key.isAcceptable()) {
                        System.out.println("doAccept");
                        accept(key);
                    }
                    else if (key.isReadable()) {
                        System.out.println("doRead");
                        doRead(key);
                    }
                    else if (key.isWritable()) {
                        System.out.println("doWrite");
                        doWrite(key);
                    }
                }
                selectedKeys.clear();
            } catch (ClosedSelectorException ignore) {
                return;
            } catch (Throwable e) {
            }
        }
    }
}
