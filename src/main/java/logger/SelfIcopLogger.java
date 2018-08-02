package logger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SelfIcopLogger implements IcopLogger {
	private Logger logger;
	private final static String errorFomate = "ERROR %S:";
	public SelfIcopLogger(Class<?> class1) {
		this.logger = LoggerFactory.getLogger(class1);
	}

	public void error(Exception e) {
		logger.error("", e);
		//System.out.println(e.getStackTrace());
	}

	public void error(String msg) {
		logger.error(msg);
		//System.out.println(msg);
	}

	public void error(Integer errorCode){
		String errorStr = String.format(errorFomate, errorCode);
		logger.error(errorStr);
		//System.out.println(errorStr);
	}
	
	public void error(Integer errorCode, Exception e){
		String errorStr = String.format(errorFomate, errorCode);
		logger.error(errorStr, e);
		//System.out.println(e.getStackTrace());
	}
	
	public void debug(String msg){
		logger.debug(msg);
		//System.out.println(new Date() + " : " + msg);
	}

	public void error(String msg, Exception e) {
		logger.error(msg, e);
		//System.out.println(msg);
		//System.out.println(e.getStackTrace());
	}

	public void error(Integer errorCode, String msg) {
		logger.error(msg, msg);
		//System.out.println(errorCode+":"+msg);
	}

	public void info(String msg) {
		logger.info(msg);
		//logger.trace(msg);
	}

	public void warn(String msg) {
		logger.warn(msg);
	}
	public void warn(Exception e) {
		logger.warn(e.getMessage());
	}
}
