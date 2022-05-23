package util;

public class Token {

	private static String token;
	
	public static void set(String t) {
		token = t;
	}
	
	public static String get() {
		return token == null ? "" : token ;
	}
	
	public boolean matches(String t) {
		return token != null && token.equals( t );
	}
}
