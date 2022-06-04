package tp1.impl.servers.common;

import static tp1.api.service.java.Result.ErrorCode.*;
import static tp1.api.service.java.Result.error;
import static tp1.api.service.java.Result.ok;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Comparator;

import com.google.gson.Gson;
import tp1.api.service.java.Files;
import tp1.api.service.java.Result;
import tp1.impl.servers.common.kafka.KafkaPublisher;
import tp1.impl.servers.common.kafka.sync.SyncPoint;
import util.Hash;
import util.IO;
import util.Token;
import util.Version;

public class JavaFiles implements Files {

    static final long TOKEN_TIME = 10000;
    static final String DELIMITER = "$$$";
    static final String NEW_DELMITER = "--";
    private static final String ROOT = "/tmp/";

    final String token = Token.get();

    public JavaFiles() {
        new File(ROOT).mkdirs();
    }

    @Override
    public Result<byte[]> getFile(String fileId, String token) {
        String[] t = token.split(NEW_DELMITER);//t //t[0] == time t[1] == h(m+k)
        if (System.currentTimeMillis() - Long.parseLong(t[0]) < TOKEN_TIME) {
            String tok = fileId + this.token;
            String hashToken = Hash.of(tok);
            if (hashToken.equals(t[1])) {
                fileId = fileId.replace(DELIMITER, "/");
                byte[] data = IO.read(new File(ROOT + fileId));
                return data != null ? ok(data) : error(NOT_FOUND);
            }
            return error(FORBIDDEN);
        }
        return error(BAD_REQUEST);
    }

    @Override
    public Result<Void> deleteFile(String fileId, String token) {
        String[] t = token.split(NEW_DELMITER);//t[0] = m , //t[1] == time t[2] == h(m+k)

        if (System.currentTimeMillis() - Long.parseLong(t[0]) < TOKEN_TIME) {
            String tok = fileId + this.token;
            String hashToken = Hash.of(tok);
            if (hashToken.equals(t[1])) {
                fileId = fileId.replace(DELIMITER, "/");
                boolean res = IO.delete(new File(ROOT + fileId));
                return res ? ok() : error(NOT_FOUND);
            }
            return error(FORBIDDEN);
        }
        return error(BAD_REQUEST);
    }

    @Override
    public Result<Void> writeFile(String fileId, byte[] data, String token) {
        String[] t = token.split(NEW_DELMITER);//t[0] = m , //t[0] == time t[1] == h(m+k)
        if (System.currentTimeMillis() - Long.parseLong(t[0]) < TOKEN_TIME) {
            String tok = fileId + this.token;
            String hashToken = Hash.of(tok);
            if (hashToken.equals(t[1])) {
                fileId = fileId.replace(DELIMITER, "/");
                File file = new File(ROOT + fileId);
                file.getParentFile().mkdirs();
                IO.write(file, data);
                return ok();
            }
            return error(FORBIDDEN);
        }
        return error(BAD_REQUEST);
    }

    @Override
    public Result<Void> deleteUserFiles(String userId, String token) {
        String[] t = token.split(NEW_DELMITER);//t[0] = m , //t[1] == time t[2] == h(m+k)
        if (System.currentTimeMillis() - Long.parseLong(t[0]) < TOKEN_TIME) {
            String tok = userId + this.token;
            String hashToken = Hash.of(tok);
            if (hashToken.equals(t[1])) {
                File file = new File(ROOT + userId);
                try {
                    java.nio.file.Files.walk(file.toPath())
                            .sorted(Comparator.reverseOrder())
                            .map(Path::toFile)
                            .forEach(File::delete);
                } catch (IOException e) {
                    e.printStackTrace();
                    return error(INTERNAL_ERROR);
                }
                return ok();
            }
            return error(FORBIDDEN);
        }
        return error(BAD_REQUEST);
    }

    public static String fileId(String filename, String userId) {
        return userId + JavaFiles.DELIMITER + filename;
    }
}
