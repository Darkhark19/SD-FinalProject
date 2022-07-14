package tp1.impl.servers.rest;

import com.google.gson.Gson;

import jakarta.ws.rs.GET;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import tp1.api.FileInfo;
import tp1.api.service.java.Directory;
import tp1.api.service.java.Result;
import tp1.api.service.rest.RestRepDirectory;
import tp1.impl.servers.common.JavaDirectory;
import tp1.impl.servers.common.kafka.*;
import tp1.api.service.java.Result.ErrorCode;
import tp1.impl.servers.common.kafka.sync.SyncPoint;
import util.Hash;
import util.Token;

import java.util.List;
import java.util.logging.Logger;

import static tp1.impl.clients.Clients.FilesClients;

public class RepDirectoryResource extends RestResource implements RestRepDirectory {
    private static Logger Log = Logger.getLogger(DirectoryResources.class.getName());

    private static final String WRITE = "write";
    private static final String DELETE = "delete";
    private static final String GET = "get";
    private static final String SHARE = "share";
    private static final String UNSHARE = "unshare";
    private static final String LIST = "list";
    private static final String DELETES = "deletes";

    static final String NEW_DELMITER = "--";
    private static final String REST = "/rest/";

    static final String FROM_BEGINNING = "earliest";
    public static final String TOPIC = "directory";
    static final String KAFKA_BROKERS = "kafka:9092";

    final Directory senderImpl;
    final Directory receiverImpl;
    final KafkaSubscriber receiver;

    final Gson json;
    final ReplicationManager replicationManager;


    public RepDirectoryResource(ReplicationManager rep) {
        AdminClientConfig.configNames();
        json = new Gson();
        senderImpl = new RepDirectory();
        receiverImpl = new JavaDirectory();

        this.receiver = KafkaSubscriber.createSubscriber(KAFKA_BROKERS, List.of(TOPIC),FROM_BEGINNING);
        replicationManager = rep;
        this.receiver.start(false, new DirectoryRecordProcessor(receiverImpl,replicationManager));
    }

    @Override
    public FileInfo writeFile(Long version, String filename, byte[] data, String userId, String password) {
        Result<FileInfo> r =  senderImpl.writeFile(filename,data,userId,password);
        return super.resultOrThrow(r);
    }

    @Override
    public void deleteFile(Long version, String filename, String userId, String password) {
        super.resultOrThrow(senderImpl.deleteFile(filename, userId, password));
    }

    @Override
    public void shareFile(Long version, String filename, String userId, String userIdShare, String password) {
        super.resultOrThrow(senderImpl.shareFile(filename,userId,userIdShare,password));
    }

    @Override
    public void unshareFile(Long version, String filename, String userId, String userIdShare, String password) {
        super.resultOrThrow(senderImpl.unshareFile(filename, userId, userIdShare, password));
    }

    @Override
    public byte[] getFile(Long version, String filename, String userId, String accUserId, String password) {
        var res = senderImpl.getFile(filename, userId, accUserId, password);
        if (res.error() == ErrorCode.REDIRECT) {
            String location = res.errorValue();
            if (!location.contains(REST)) {
                String t = JavaDirectory.fileId(filename,userId) + Token.get();
                String hashToken = Hash.of(t);
                String tok = System.currentTimeMillis() + NEW_DELMITER + hashToken;
                res = FilesClients.get(location).getFile(JavaDirectory.fileId(filename, userId), tok);
            }
        }
        return super.resultOrThrow(res);
    }

    @Override
    public List<FileInfo> lsFile(Long version, String userId, String password) {
        long T0 = System.currentTimeMillis();
        try {

           // Log.info(String.format("REST lsFile: userId = %s, password = %s\n", userId, password));

            return super.resultOrThrow(senderImpl.lsFile(userId, password));
        } finally {
            System.err.println("TOOK:" + (System.currentTimeMillis() - T0));
        }
    }

    @Override
    public void deleteUserFiles(Long version, String userId, String password, String token) {
        super.resultOrThrow(senderImpl.deleteUserFiles(userId,password,token));
    }

    static class DirectoryRecordProcessor implements RecordProcessor{

        final Directory receiverImpl;
        final ReplicationManager replicationManager;
        final Gson json;
        final SyncPoint<Object> sync = SyncPoint.getInstance();

        public DirectoryRecordProcessor(Directory sub, ReplicationManager rep){
            json = new Gson();
            receiverImpl = sub;
            replicationManager = rep;
        }
        @Override
        public synchronized void onReceive(ConsumerRecord<String, String> r) {
            String key = r.key();
            String val = r.value();
            String filename;
            String userId;
            String pass;
            String accUserId;
            String userIdShare;
            replicationManager.setVersion(r.offset());
            switch (key) {
                case WRITE -> {
                    RepDirectory.WriteArgs op = json.fromJson(val, RepDirectory.WriteArgs.class);
                    filename = op.getFilename();
                    userId = op.getUserId();
                    byte[] data = op.getData();
                    pass = op.getPass();
                    Result<FileInfo> write = receiverImpl.writeFile(filename,data, userId,pass);
                    sync.setResult(r.offset(), write);
                }
                case DELETE -> {
                    String[] op = json.fromJson(val,String[].class);
                    filename = op[0];
                    userId = op[1];
                    pass = op[2];
                    Result<Void> delete = receiverImpl.deleteFile(filename, userId, pass);
                    sync.setResult(r.offset(), delete);
                }
                case GET -> {
                    String[] op = json.fromJson(val,String[].class);
                    filename = op[0];
                    userId = op[1];
                    accUserId = op[2];
                    pass = op[3];
                    Result<byte[]> get = receiverImpl.getFile(filename, userId, accUserId, pass);
                    sync.setResult(r.offset(), get);
                }
                case SHARE -> {
                    String[] op = json.fromJson(val,String[].class);
                    filename = op[0];
                    userId = op[1];
                    userIdShare = op[2];
                    pass = op[3];
                    Result<Void> share = receiverImpl.shareFile(filename, userId, userIdShare, pass);
                    sync.setResult(r.offset(), share);
                }
                case UNSHARE -> {
                    String[] op = json.fromJson(val,String[].class);
                    filename = op[0];
                    userId = op[1];
                    userIdShare = op[2];
                    pass = op[3];
                    Result<Void> unshare = receiverImpl.unshareFile(filename, userId, userIdShare, pass);
                    sync.setResult(r.offset(), unshare);
                }
                case LIST -> {
                    String[] op = json.fromJson(val,String[].class);
                    userId = op[0];
                    pass = op[1];
                    Result<List<FileInfo>> list = receiverImpl.lsFile(userId, pass);
                    sync.setResult(r.offset(), list);
                }
                case DELETES -> {
                    String[] op = json.fromJson(val,String[].class);
                    userId = op[0];
                    pass = op[1];
                    String token = op[2];
                    Result<Void> deletes = receiverImpl.deleteUserFiles(userId, pass, token);
                    sync.setResult(r.offset(), deletes);
                }
                default -> {
                }
            }

        }
    }
}
