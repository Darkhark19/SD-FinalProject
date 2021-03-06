package tp1.impl.servers.rest;

import jakarta.inject.Singleton;
import tp1.api.service.java.Files;
import tp1.api.service.rest.RestFiles;
import tp1.impl.servers.common.JavaFiles;
import tp1.impl.servers.common.dropbox.DropBoxFiles;

import java.util.logging.Logger;

@Singleton
public class DropboxFilesResource extends RestResource implements RestFiles {
    private static Logger Log = Logger.getLogger(DropboxFilesResource.class.getName());

    final Files impl;

    public DropboxFilesResource(boolean flag,String key, String secret, String token)  {
        impl = new DropBoxFiles(flag,key,secret,token);
    }

    @Override
    public void writeFile(String fileId, byte[] data, String token) {
        //Log.info(String.format("DB writeFile: fileId = %s, data.length = %d, token = %s \n", fileId, data.length, token));
        super.resultOrThrow( impl.writeFile(fileId, data, token));
    }

    @Override
    public void deleteFile(String fileId, String token) {
        //Log.info(String.format("DB deleteFile: fileId = %s, token = %s \n", fileId, token));

        super.resultOrThrow( impl.deleteFile(fileId, token));
    }

    @Override
    public byte[] getFile(String fileId, String token) {
        //Log.info(String.format("DB getFile: fileId = %s,  token = %s \n", fileId, token));

        return resultOrThrow( impl.getFile(fileId, token));
    }

    @Override
    public void deleteUserFiles(String userId, String token) {
        //Log.info(String.format("DB deleteUserFiles: userId = %s, token = %s \n", userId, token));

        super.resultOrThrow( impl.deleteUserFiles(userId, token));
    }
}
