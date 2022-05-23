package tp1.api.service.java;

public interface Files {
	public static String SERVICE_NAME = "files";
	
	Result<byte[]> getFile(String fileId, String token);

	Result<Void> deleteFile(String fileId, String token);
	
	Result<Void> writeFile(String fileId, byte[] data, String token);

	Result<Void> deleteUserFiles(String userId, String token);
	
}
