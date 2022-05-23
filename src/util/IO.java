package util;

import java.io.File;
import java.nio.file.Files;

final public class IO {

	public static void write(File out, byte[] data) {
		try {
			Files.write(out.toPath(), data);
		} catch (Exception x) {
			x.printStackTrace();
		}
	}

	public static byte[] read(File from) {
		try {
			return Files.readAllBytes(from.toPath());
		} catch (Exception x) {
			x.printStackTrace();
			return null;
		}
	}

	public static boolean delete(File file) {
		try {
			if (file.exists()) {
				Files.delete(file.toPath());
				return true;
			}
		} catch (Exception x) {
			x.printStackTrace();
		}
		return false;
	}
}
