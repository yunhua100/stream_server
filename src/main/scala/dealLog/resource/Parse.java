package dealLog.resource;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import dealLog.model.Constants;
import dealLog.util.TimeUtil;
import dealLog.util.XMLLogParse;
import logger.IcopLogger;
import logger.IcopLoggerFactory;

public class Parse {
    private static IcopLogger logger = IcopLoggerFactory.getLogger(Parse.class);
    public static JSONObject parse(String data){
        JSONObject dataObject = null;
        try {
            dataObject = JSON.parseObject(data);
            JSONObject beat = dataObject.getJSONObject("beat");
            if(beat != null){
                dataObject.put(Constants.HOST_NAME(),beat.getString("hostname"));
            }
            dataObject.put(Constants.COLLECT_TIME(), dataObject.getString(Constants.TIMESTAMP()));
            dataObject.put(Constants.PROCESS_TIME(), TimeUtil.getCurrentTimestamp());

            String message = dataObject.getString(Constants.MESSAGE());
            if(message == null){
                logger.warn("log parse error-" + data);
                return dataObject;
            }
            int xmlStart = message.indexOf("<");
            int xmlEnd = message.lastIndexOf(">");
            if(xmlStart == xmlEnd)
                return dataObject;
            String json = XMLLogParse.xmlParse(message.substring(xmlStart, xmlEnd + 1));
            dataObject.putAll(JSON.parseObject(json));
            if(message.contains("SEND") || message.contains("send") || message.contains("Send")){
                dataObject.put(Constants.MSG_STATE(),2);
            }else if(message.contains("RECV") || message.contains("recv") || message.contains("Recv")){
                dataObject.put(Constants.MSG_STATE(), 1);
            }

            String systemName = dataObject.getString(Constants.SYSTEM_NAME());
            String timestamp = null;
            if("nesb".equals(systemName)){
                int timeStart = message.indexOf("TIME");
                int timeEnd = message.indexOf("]" ,timeStart);
                timestamp = TimeUtil.timeStampFormat(message.substring(timeStart+5,timeEnd), "yyyy/MM/dd HH:mm:ss.SSS");
            }else if("pmsp".equals(systemName)){
                int timeStart = message.indexOf(" ");
                int timeEnd = timeStart + 24;
                timestamp = TimeUtil.timeStampFormat(message.substring(timeStart+1,timeEnd), "yyyy-MM-dd HH:mm:ss,SSS");
            }
            dataObject.put(Constants.TIMESTAMP(), timestamp);
            dataObject.put(Constants.PARSE_STATUS(), 1);
        } catch (Exception e) {
            logger.error(e);
        }
        return dataObject;
    }
}
