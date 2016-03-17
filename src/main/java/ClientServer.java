import com.google.protobuf.InvalidProtocolBufferException;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by sicongfeng on 16/3/15.
 */
public class ClientServer extends Server {
    public ConcurrentHashMap<String, Request> activeTxns;

    public ClientServer(String ip, int port, Scheduler scheduler) throws IOException {
        super(ip, port, scheduler);
    }

    public void decode() {
        byte[] b = new byte[buffer.remaining()];
        buffer.get(b);
        try {
            ClientRequestOuterClass.ClientRequest clientRequest = ClientRequestOuterClass.ClientRequest.parseFrom(b);
            String opType = clientRequest.getType();
            dispatchType(opType, clientRequest);

            // fetch data from DB and return
            // then register for write in Server.doRead

        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void doRead(SelectionKey key) {
        SocketChannel ch = (SocketChannel) key.channel();
        try {
            buffer.clear();
            int read = ch.read(buffer);
            if (read == -1) {
                // close key
            }
            else if (read > 0) {
                buffer.flip();
                decode();
                key.interestOps(SelectionKey.OP_WRITE);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void dispatchType(String opType, ClientRequestOuterClass.ClientRequest clientRequest) {
        if (opType.equals("COMMIT")) {

        }
    }

    public void encode() {

    }

    @Override
    public void doWrite(SelectionKey key) {

    }
}
