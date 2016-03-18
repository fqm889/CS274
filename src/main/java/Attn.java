import java.nio.ByteBuffer;
import java.util.LinkedList;

/**
 * Created by sicongfeng on 16/3/17.
 */
public class Attn {
    public ByteBuffer buffer;
    public LinkedList<ByteBuffer> toWrite;

    public Attn() {
        buffer = ByteBuffer.allocateDirect(1024*64);
        toWrite = new LinkedList<ByteBuffer>();
    }
}
