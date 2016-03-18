import Operations.*;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.io.ByteArrayInputStream;
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

    public ClientServer(String ip, int port, Scheduler scheduler) throws IOException {
        super(ip, port, scheduler);
    }

    public byte[] decode(ByteBuffer buffer) {
        System.out.println("Got decode");
        ByteString b = ByteString.copyFrom(buffer);
        byte[] res = null;
        try {
//            ByteArrayInputStream bs = new ByteArrayInputStream(buffer.array());
            ClientRequestOuterClass.ClientRequest clientRequest = ClientRequestOuterClass.ClientRequest.parseFrom(b);
            res = dispatchType(clientRequest);

            // then register for write in Server.doRead

        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("Finishing decode");
        return res;
    }

    public byte[] dispatchType(ClientRequestOuterClass.ClientRequest clientRequest) {
        String opType = clientRequest.getType();
        String id = clientRequest.getId();
        System.out.println("Dispatch id:"+id+" type: "+opType);
        Operation op = null;

        if (opType.equals("START")) {
            System.out.println("START!!");
            scheduler.addTxn(id);
        }
        else if (opType.equals("READ")) {
            System.out.println(clientRequest.getTable());
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
        System.out.println("Before add op");
        QueryResult result = scheduler.addOneOpTxn(id, op);
        System.out.println("Result got11111");
        byte[] res = result.toByteArray();
        System.out.println("toByteArray got");
        return res;
    }
}
