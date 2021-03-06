package tp1.impl.servers.common.dropbox;

import com.github.scribejava.core.builder.ServiceBuilder;
import com.github.scribejava.core.model.OAuth2AccessToken;
import com.github.scribejava.core.model.OAuthRequest;
import com.github.scribejava.core.model.Response;
import com.github.scribejava.core.model.Verb;
import com.github.scribejava.core.oauth.OAuth20Service;
import com.google.gson.Gson;
import jakarta.ws.rs.core.GenericType;
import org.pac4j.scribe.builder.api.DropboxApi20;
import tp1.api.service.java.Files;
import tp1.api.service.java.Result;
import jakarta.ws.rs.core.Response.Status;
import tp1.impl.servers.common.dropbox.msgs.*;
import util.Hash;
import util.Token;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static tp1.api.service.java.Result.ErrorCode.BAD_REQUEST;
import static tp1.api.service.java.Result.error;


public class DropBoxFiles implements Files {

   /* private String apiKey ;
    private String apiSecret;
    private String accessTokenStr;*/

    private static final String CREATE_FOLDER_V2_URL = "https://api.dropboxapi.com/2/files/create_folder_v2";
    private static final String DOWNLOAD_URL = "https://content.dropboxapi.com/2/files/download";
    private static final String LIST_FOLDER_URL = "https://api.dropboxapi.com/2/files/list_folder";
    private static final String LIST_FOLDER_CONTINUE_URL = "https://api.dropboxapi.com/2/files/list_folder/continue";
    private static final String UPLOAD_FILE_URL = "https://content.dropboxapi.com/2/files/upload";
    private static final String DELETE_V2_URL = "https://api.dropboxapi.com/2/files/delete_v2";

    private static final String CONTENT_TYPE_HDR = "Content-Type";
    private static final String JSON_CONTENT_TYPE = "application/json; charset=utf-8";
    private static final String OCTET_STREAM_CONTENT_TYPE = "application/octet-stream";
    private static final String DROPBOX_API_ARG = "Dropbox-API-Arg";

    private final Gson json;
    private final OAuth20Service service;
    private final OAuth2AccessToken accessToken;

    private static final String ROOT = "/tmp/";
    static final String DELIMITER = "$$$";
    static final String NEW_DELMITER = "--";
    private static final String OVERWRITE = "overwrite";
    private static final String TMP = "/tmp";
    static final long TOKEN_TIME = 10000;
    final String token = Token.get();

    public DropBoxFiles(boolean flag, String key, String secret, String token) {
        json = new Gson();
        accessToken = new OAuth2AccessToken(token);
        service = new ServiceBuilder(key).apiSecret(secret).build(DropboxApi20.INSTANCE);

        if (flag) {
            var delete = new OAuthRequest(Verb.POST, DELETE_V2_URL);
            delete.addHeader(CONTENT_TYPE_HDR, JSON_CONTENT_TYPE);
            delete.setPayload(json.toJson(new PathArgs(TMP)));
            service.signRequest(accessToken, delete);
            try {
                Response r = service.execute(delete);

                if (r.getCode() != Status.OK.getStatusCode())
                    throw new RuntimeException(String.format("Failed to delete directory: %s, Status: %d, \nReason: %s\n", ROOT, r.getCode(), r.getBody()));
            } catch (Exception e) {
                e.printStackTrace();
            }

            var createFolder = new OAuthRequest(Verb.POST, CREATE_FOLDER_V2_URL);
            createFolder.addHeader(CONTENT_TYPE_HDR, JSON_CONTENT_TYPE);
            createFolder.setPayload(json.toJson(new CreateFolderV2Args(TMP, false)));
            service.signRequest(accessToken, createFolder);
            Response r = null;
            try {
                r = service.execute(createFolder);
                if (r.getCode() != Status.OK.getStatusCode())
                    throw new RuntimeException(String.format("Failed to create directory: %s, Status: %d, \nReason: %s\n", ROOT, r.getCode(), r.getBody()));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

    @Override
    public Result<Void> writeFile(String fileId, byte[] data, String token) {
        String filename = fileId;
        fileId = fileId.replace(DELIMITER, "/");
        var createFile = new OAuthRequest(Verb.POST, UPLOAD_FILE_URL);
        createFile.addHeader(CONTENT_TYPE_HDR, OCTET_STREAM_CONTENT_TYPE);

        createFile.addHeader(DROPBOX_API_ARG, json.toJson(
                new CreateFileArgs(ROOT + fileId, OVERWRITE, false, false, false)));

        createFile.setPayload(json.toJson(data).getBytes());
        return execute(createFile, filename, token);
    }

    @Override
    public Result<byte[]> getFile(String fileId, String token) {
        String filename = fileId;
        fileId = fileId.replace(DELIMITER, "/");
        OAuthRequest getFile = new OAuthRequest(Verb.POST, DOWNLOAD_URL);
        getFile.addHeader(CONTENT_TYPE_HDR, OCTET_STREAM_CONTENT_TYPE);
        getFile.addHeader(DROPBOX_API_ARG, json.toJson(new PathArgs(ROOT + fileId)));
        service.signRequest(accessToken, getFile);
        try {
            String[] t = token.split(NEW_DELMITER);
            if (System.currentTimeMillis() - Long.parseLong(t[0]) < TOKEN_TIME) {
                String tok = filename + this.token;
                String hashToken = Hash.of(tok);
                if (hashToken.equals(t[1])) {
                    Response r = service.execute(getFile);

                    if (r.getCode() != Status.OK.getStatusCode())
                        return Result.error(statusToErrorCode(Status.fromStatusCode(r.getCode())));
                    byte[] content = json.fromJson(r.getBody(), byte[].class);
                    return Result.ok(content);
                }
                return Result.error(Result.ErrorCode.FORBIDDEN);
            }
            return error(BAD_REQUEST);
        } catch (Exception e) {
            e.printStackTrace();
            return Result.error(Result.ErrorCode.INTERNAL_ERROR);
        }

    }

    @Override
    public Result<Void> deleteFile(String fileId, String token) {
        String filename = fileId;
        fileId = fileId.replace(DELIMITER, "/");
        var delete = new OAuthRequest(Verb.POST, DELETE_V2_URL);
        delete.addHeader(CONTENT_TYPE_HDR, JSON_CONTENT_TYPE);

        delete.setPayload(json.toJson(new PathArgs(ROOT + fileId)));

        return execute(delete, filename, token);
    }


    @Override
    public Result<Void> deleteUserFiles(String userId, String token) {
        List<String> directoryContents = new ArrayList<String>();
        var listDirectory = new OAuthRequest(Verb.POST, LIST_FOLDER_URL);
        listDirectory.addHeader(CONTENT_TYPE_HDR, JSON_CONTENT_TYPE);
        listDirectory.setPayload(json.toJson(new ListFolderArgs(ROOT + userId)));

        service.signRequest(accessToken, listDirectory);
        try {
            String[] t = token.split(NEW_DELMITER);//t[0] = m , //t[0] == time t[1] == h(m+k)
            String tok = userId + this.token;
            String hashToken = Hash.of(tok);
            if (System.currentTimeMillis() - Long.parseLong(t[0]) > TOKEN_TIME) {
                return error(BAD_REQUEST);
            } else if (!hashToken.equals(t[1])) {
                return Result.error(Result.ErrorCode.FORBIDDEN);
            } else {
                Response r = service.execute(listDirectory);

                if (r.getCode() != Status.OK.getStatusCode())
                    return Result.error(statusToErrorCode(Status.fromStatusCode(r.getCode())));

                var reply = json.fromJson(r.getBody(), ListFolderReturn.class);
                reply.getEntries().forEach(e -> directoryContents.add(e.toString()));

                while (reply.has_more()) {
                    listDirectory = new OAuthRequest(Verb.POST, LIST_FOLDER_CONTINUE_URL);
                    listDirectory.addHeader(CONTENT_TYPE_HDR, JSON_CONTENT_TYPE);

                    // In this case the arguments is just an object containing the cursor that was
                    // returned in the previous reply.
                    listDirectory.setPayload(json.toJson(new ListFolderContinueArgs(reply.getCursor())));
                    service.signRequest(accessToken, listDirectory);

                    r = service.execute(listDirectory);

                    if (r.getCode() != Status.OK.getStatusCode())
                        return Result.error(statusToErrorCode(Status.fromStatusCode(r.getCode())));

                    reply = json.fromJson(r.getBody(), ListFolderReturn.class);
                    reply.getEntries().forEach(e -> directoryContents.add(e.toString()));

                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            return Result.error(Result.ErrorCode.INTERNAL_ERROR);
        }
        for (String fileName : directoryContents) {
            var delete = new OAuthRequest(Verb.POST, DELETE_V2_URL);
            delete.addHeader(CONTENT_TYPE_HDR, JSON_CONTENT_TYPE);
            delete.setPayload(json.toJson(new PathArgs(ROOT + userId + "/" + fileName)));
            service.signRequest(accessToken, delete);
            try {
                service.execute(delete);
            } catch (Exception e) {

            }
        }
        return Result.ok();
    }

    public static String fileId(String filename, String userId) {
        return userId + DELIMITER + filename;
    }

    private <T> Result<T> execute(OAuthRequest request, String filename, String token) {
        String[] t = token.split(NEW_DELMITER);//t[0] == time t[1] == h(m+k)
        String tok = filename + this.token;
        String hashToken = Hash.of(tok);
        if (System.currentTimeMillis() - Long.parseLong(t[0]) > TOKEN_TIME) {
            return error(BAD_REQUEST);
        } else if (!hashToken.equals(t[1])) {
            return Result.error(Result.ErrorCode.FORBIDDEN);
        } else {
            service.signRequest(accessToken, request);
            try {
                Response r = service.execute(request);
                if (r.getCode() != Status.OK.getStatusCode())
                    return Result.error(statusToErrorCode(Status.fromStatusCode(r.getCode())));
                return Result.ok();
            } catch (Exception e) {
                e.printStackTrace();
                return Result.error(Result.ErrorCode.INTERNAL_ERROR);
            }
        }


    }

    private Result.ErrorCode statusToErrorCode(jakarta.ws.rs.core.Response.Status status) {
        return switch (status) {
            case OK, NO_CONTENT -> Result.ErrorCode.OK;
            case CONFLICT, NOT_FOUND -> Result.ErrorCode.NOT_FOUND;
            case FORBIDDEN -> Result.ErrorCode.FORBIDDEN;
            case BAD_REQUEST -> Result.ErrorCode.BAD_REQUEST;
            case NOT_IMPLEMENTED -> Result.ErrorCode.NOT_IMPLEMENTED;
            default -> Result.ErrorCode.INTERNAL_ERROR;
        };
    }
}
