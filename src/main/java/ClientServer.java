import Operations.*;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by sicongfeng on 16/3/15.
 */
public class ClientServer extends Server {
    public ConcurrentHashMap<String, Request> activeTxns;

    public ClientServer(String ip, int port, Scheduler scheduler) throws IOException {
        super(ip, port, scheduler);
    }

    public byte[] decode(ByteBuffer buffer) {
        ByteString b = ByteString.copyFrom(buffer);
        byte[] res = null;
        try {
            ClientRequestOuterClass.ClientRequest clientRequest = ClientRequestOuterClass.ClientRequest.parseFrom(b);
            res = dispatchType(clientRequest);

            // then register for write in Server.doRead

        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
        return res;
    }

    @Override
    public void doRead(SelectionKey key) {
        SocketChannel ch = (SocketChannel) key.channel();
        try {
            Attn attn = (Attn) key.attachment();
            ByteBuffer buffer = attn.buffer;
            buffer.clear();
            int read = ch.read(buffer);
            if (read == -1) {
                // close key
            }
            else if (read > 0) {
                buffer.flip();
                byte[] res = decode(buffer);
                attn.toWrite.add(ByteBuffer.wrap(res));
                key.interestOps(SelectionKey.OP_WRITE);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public byte[] dispatchType(ClientRequestOuterClass.ClientRequest clientRequest) {
        String opType = clientRequest.getType();
        String id = clientRequest.getId();
        Operation op = null;

        if (opType.equals("START")) {
            scheduler.addTxn(id);
        }
        else if (opType.equals("READ")) {
            op = new ReadOp();
        }
        else if (opType.equals("SCAN")) {
            op = new ScanOp();
        }
        else if (opType.equals("INSERT")) {
            op = new InsertOp();
        }
        else if (opType.equals("UPDATE")) {
            op = new UpdateOp();
        }
        else if (opType.equals("COMMIT")) {
            op = new CommitOp();
        }
        else if (opType.equals("ABORT")) {
            op = new AbortOp();
        }

        QueryResult result = scheduler.addOneOpTxn(id, op);

        return result.toByteArray();
    }

    public void encode( ) {

    }

    @Override
    public void doWrite(SelectionKey key) {
        SocketChannel ch = (SocketChannel) key.channel();
        Attn attn = (Attn) key.attachment();
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
    }
}
