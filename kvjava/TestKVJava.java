import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import java.lang.String;

public class TestKVJava {
   static {
        System.loadLibrary("KVJava");
   }

	public static void main(String args[]) {
		KVJava kv = new KVJava("/tmp/spool");
		HashMap h = new HashMap();
		h.put("user", "troy");
		h.put("id", new Integer(10));
		kv.write(h);

    HashMap m = kv.read();
    for (Map.Entry<String, String> entry : m.entrySet()) {
        String key = entry.getKey();
        String val = entry.getValue();
        System.out.println(key + ": " + val);
    }
	}
}
