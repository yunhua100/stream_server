package logger;

public class IcopLoggerFactory {
	public static IcopLogger getLogger(){
		return new SelfIcopLogger(IcopLoggerFactory.class);
	}
	
	public static IcopLogger getLogger(Class<?> class1){
		return new SelfIcopLogger(class1);
	}
}
