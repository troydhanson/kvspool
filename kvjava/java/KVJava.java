
import java.util.HashMap;
import java.lang.String;

public class KVJava {
    static {
        System.load("/usr/local/lib/libKVJavaC.so");
    }
    private long rsp;
    private long wsp;
    private String dir;
    
    public KVJava(){
        dir = "/home/test/spool";
    }
    public static void main(String args[]){
        KVJava kvj = new KVJava();
        HashMap frame = new HashMap();
        frame.put("yoda", "val");
        for(int i =0;i<1000;i++){
            frame.put("int",new Integer(i));
            //kvj.putFrame(frame);
            
            HashMap v = kvj.getFrame(true);
            System.out.println(v.get("iter") + " " + v.get("when"));
        }
        kvj.close();
    }
    
    public native HashMap<String,String> getFrame(boolean block);
    public native void putFrame(HashMap<String,String> frame);
    public native void close();
}
