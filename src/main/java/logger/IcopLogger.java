package logger;

public interface IcopLogger {

	void debug(String msg);

	void error(String msg, Exception e);
	
	void error(Integer errorCode, String msg);
	
	void error(Integer errorCode, Exception e);

	void error(Integer errorCode);

	void error(String msg);

	void error(Exception e);

	void info(String msg);

	void warn(String msg);
	
	void warn(Exception e);

}
