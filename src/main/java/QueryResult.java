import com.google.protobuf.ByteString;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Vector;

/**
 * Created by sicongfeng on 16/3/17.
 */
public class QueryResult {
    public String id;
    public String type;
    public Object result;

    public QueryResult(String id, String type, Object result) {
        this.id = id;
        this.type = type;
        this.result = result;
    }

    private ClientRespondOuterClass.FieldValue makefv(String f, ByteString v) {
        ClientRespondOuterClass.FieldValue.Builder fvBuilder = ClientRespondOuterClass.FieldValue.newBuilder();
        fvBuilder.setField(f).setValue(v);
        return fvBuilder.build();
    }

    public byte[] toByteArray() {
        System.out.println("toByteArray");
        ByteArrayOutputStream bo = new ByteArrayOutputStream();
        ClientRespondOuterClass.ClientRespond.Builder crBuilder = ClientRespondOuterClass.ClientRespond.newBuilder();

        if (type.equals("READ")) {
            HashMap<String,ByteString> res = (HashMap<String, ByteString>) result;
            ClientRespondOuterClass.MapFieldValue.Builder mfvBuilder = ClientRespondOuterClass.MapFieldValue.newBuilder();

            Iterator it = res.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<String, ByteString> pair = (Map.Entry) it.next();
                mfvBuilder.addFv(makefv(pair.getKey(), pair.getValue()));
            }

            ClientRespondOuterClass.MapFieldValue mfv = mfvBuilder.build();
            ClientRespondOuterClass.ClientRespond cr = crBuilder.setFv(mfv).setType(type).setId(id).setStatus("SUCCESS").build();
            try {
                cr.writeDelimitedTo(bo);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return bo.toByteArray();
        }

        else if (type.equals("SCAN")) {
            Vector<HashMap<String,ByteString>> res = (Vector<HashMap<String, ByteString>>) result;
            ClientRespondOuterClass.MapFieldValue.Builder mfvBuilder = ClientRespondOuterClass.MapFieldValue.newBuilder();

            Iterator<HashMap<String, ByteString>> it = res.iterator();
            while (it.hasNext()) {
                Iterator it2 = it.next().entrySet().iterator();
                while (it2.hasNext()) {
                    Map.Entry<String, ByteString> pair = (Map.Entry) it2.next();
                    mfvBuilder.addFv(makefv(pair.getKey(), pair.getValue()));
                }
                ClientRespondOuterClass.MapFieldValue mfv = mfvBuilder.build();
                crBuilder.addVfv(mfv);
            }
            ClientRespondOuterClass.ClientRespond cr = crBuilder.build();
            try {
                cr.writeDelimitedTo(bo);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return bo.toByteArray();
        }

        else {
            ClientRespondOuterClass.ClientRespond cr = crBuilder.setType("99999999").setId(id).setStatus("SUCCESS").build();
            System.out.println("Writing back ClientRespond");
            try {
                cr.writeDelimitedTo(bo);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return bo.toByteArray();
        }
    }
}
