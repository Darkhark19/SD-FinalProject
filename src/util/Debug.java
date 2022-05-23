package util;

import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

public class Debug {
	public static String TP1 = "tp1";
			
	static {
		System.setProperty("java.util.logging.SimpleFormatter.format", "%4$s: %5$s\n");
	}

	public static void setLogLevel(Level newLvl, String packagePrefix ) {
		Logger rootLogger = LogManager.getLogManager().getLogger("");		
		Handler[] handlers = rootLogger.getHandlers();
		rootLogger.setLevel(newLvl);
		for (Handler h : handlers) {
			h.setLevel(newLvl);
			h.setFilter( (r) -> r.getLoggerName().startsWith(packagePrefix));
		}

	}
	
}
