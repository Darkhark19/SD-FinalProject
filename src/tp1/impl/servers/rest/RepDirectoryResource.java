package tp1.impl.servers.rest;

import tp1.api.FileInfo;
import tp1.api.service.java.Directory;
import tp1.api.service.rest.RestDirectory;
import tp1.api.service.rest.RestRepDirectory;
import tp1.impl.servers.common.JavaDirectory;
import tp1.impl.servers.common.kafka.RepDirectory;
import tp1.api.service.java.Result.ErrorCode;
import java.util.List;
import java.util.logging.Logger;

import static tp1.impl.clients.Clients.FilesClients;

public class RepDirectoryResource extends RestResource implements RestRepDirectory {
    private static Logger Log = Logger.getLogger(DirectoryResources.class.getName());

    private static final String REST = "/rest/";

    final Directory impl;

    public RepDirectoryResource() {
        impl = new RepDirectory();
    }

    @Override
    public FileInfo writeFile(Long version, String filename, byte[] data, String userId, String password) {
        return super.repThrow(impl.writeFile(filename,data,userId,password), version);
    }

    @Override
    public void deleteFile(Long version, String filename, String userId, String password) {
        super.repThrow(impl.deleteFile(filename, userId, password),version);
    }

    @Override
    public void shareFile(Long version, String filename, String userId, String userIdShare, String password) {
        super.repThrow(impl.shareFile(filename,userId,userIdShare,password),version);
    }

    @Override
    public void unshareFile(Long version, String filename, String userId, String userIdShare, String password) {
        super.repThrow(impl.unshareFile(filename, userId, userIdShare, password),version);
    }

    @Override
    public byte[] getFile(Long version, String filename, String userId, String accUserId, String password) {
        var res = impl.getFile(filename, userId, accUserId, password);
        if (res.error() == ErrorCode.REDIRECT) {
            String location = res.errorValue();
            if (!location.contains(REST))
                res = FilesClients.get(location).getFile(JavaDirectory.fileId(filename, userId), password);
        }
        return super.repThrow(res,version);
    }

    @Override
    public List<FileInfo> lsFile(Long version, String userId, String password) {
        long T0 = System.currentTimeMillis();
        try {

            Log.info(String.format("REST lsFile: userId = %s, password = %s\n", userId, password));

            return super.repThrow(impl.lsFile(userId, password),version);
        } finally {
            System.err.println("TOOK:" + (System.currentTimeMillis() - T0));
        }
    }

    @Override
    public void deleteUserFiles(Long version, String userId, String password, String token) {
        super.repThrow(impl.deleteUserFiles(userId,password,token),version);
    }
}
