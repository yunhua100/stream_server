import net.sf.json.JSON;
import net.sf.json.JSONObject;
import net.sf.json.xml.XMLSerializer;

public class TestDD {
    public static void main(String[] args) {
        JSONObject jo = new JSONObject();
        jo.put("key","value");
        System.out.println(jo.getJSONObject("keee"));

    }

}
