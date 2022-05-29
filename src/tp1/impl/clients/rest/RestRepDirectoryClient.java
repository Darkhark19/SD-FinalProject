package tp1.impl.clients.rest;

import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.GenericType;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import tp1.api.FileInfo;
import tp1.api.service.java.Directory;
import tp1.api.service.java.Result;
import tp1.api.service.rest.RestDirectory;
import tp1.api.service.rest.RestRepDirectory;

import java.net.URI;
import java.util.List;

public class RestRepDirectoryClient extends RestClient implements Directory {
    private Long version;
    public RestRepDirectoryClient(URI uri, String path) {
        super(uri, path);
        version = 0L;
    }

    @Override
    public Result<FileInfo> writeFile(String filename, byte[] data, String userId, String password) {
        Response r = target.path(userId)
                .path(filename)
                .queryParam(RestDirectory.PASSWORD, password)
                .request()
                .header(RestRepDirectory.HEADER_VERSION, version)
                .accept(MediaType.APPLICATION_JSON)
                .post(Entity.entity( data, MediaType.APPLICATION_OCTET_STREAM));
        return super.toJavaResult(r, new GenericType<FileInfo>() {});
    }

    @Override
    public Result<Void> deleteFile(String filename, String userId, String password) {
        return null;
    }

    @Override
    public Result<Void> shareFile(String filename, String userId, String userIdShare, String password) {
        return null;
    }

    @Override
    public Result<Void> unshareFile(String filename, String userId, String userIdShare, String password) {
        return null;
    }

    @Override
    public Result<byte[]> getFile(String filename, String userId, String accUserId, String password) {
        return null;
    }

    @Override
    public Result<List<FileInfo>> lsFile(String userId, String password) {
        return null;
    }

    @Override
    public Result<Void> deleteUserFiles(String userId, String password, String token) {
        return null;
    }
}
