import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.LinkedList;

/**
 * Created by sicongfeng on 16/3/15.
 */
public class ScheServer extends Server {
    public ScheServer(String ip, int port, Scheduler scheduler) throws IOException {
        super(ip, port, scheduler);
    }

    public byte[] decode(ByteBuffer buffer) {
        ByteString b = ByteString.copyFrom(buffer);
        byte[] res = null;
        try {
            ServerRequest.Log log = ServerRequest.Log.parseFrom(b);
            res = updateScheduler(log);

            // then register for write in Server.doRead

        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
        return res;
    }

    public byte[] updateScheduler(ServerRequest.Log log) {
        return null;
    }
}
