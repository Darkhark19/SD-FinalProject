package tp1.impl.servers.rest;

import com.google.gson.Gson;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import tp1.api.FileInfo;
import tp1.api.service.java.Directory;
import tp1.api.service.java.Result;
import tp1.api.service.rest.RestDirectory;
import tp1.api.service.rest.RestRepDirectory;
import tp1.impl.servers.common.JavaDirectory;
import tp1.impl.servers.common.kafka.KafkaSubscriber;
import tp1.impl.servers.common.kafka.RecordProcessor;
import tp1.impl.servers.common.kafka.RepDirectory;
import tp1.api.service.java.Result.ErrorCode;
import tp1.impl.servers.common.kafka.sync.SyncPoint;

import java.util.LinkedList;
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

    private static final String REST = "/rest/";

    static final String FROM_BEGINNING = "earliest";
    static final String TOPIC = "single_partition_topic";
    static final String KAFKA_BROKERS = "localhost:9092";

    final Directory senderImpl;
    final Directory receiverImpl;
    final KafkaSubscriber receiver;
    final SyncPoint sync = SyncPoint.getInstance();
    final Gson json;

    @SuppressWarnings("unchecked")
    public RepDirectoryResource() {
        json = new Gson();
        senderImpl = new RepDirectory();
        receiverImpl = new JavaDirectory();
        List<String> topic = new LinkedList<>();
        topic.add("");
        this.receiver = KafkaSubscriber.createSubscriber(KAFKA_BROKERS, topic, FROM_BEGINNING);

        this.receiver.start(false, new RecordProcessor() {
            @Override
            public void onReceive(ConsumerRecord<String, String> r) {

                String key = r.key();
                String val = r.value();
                Object[] op = json.fromJson(val,Object[].class);
                String filename;
                byte[] data;
                String userId;
                String pass;
                String accUserId;
                String userIdShare;
                switch (key){
                    case WRITE :
                        filename = op[0].toString();
                        data = op[1].toString().getBytes();
                        userId = op[2].toString();
                        pass = op[3].toString();
                        Result<FileInfo> write = senderImpl.writeFile(filename,data,userId,pass);
                        sync.setResult(r.offset(),write);
                        break;
                    case DELETE:
                        filename = op[0].toString();
                        userId = op[1].toString();
                        pass = op[2].toString();
                        Result<Void> delete = senderImpl.deleteFile(filename,userId,pass);
                        sync.setResult(r.offset(),delete);
                        break;
                    case GET:
                        filename = op[0].toString();
                        userId = op[1].toString();
                        accUserId = op[2].toString();
                        pass = op[3].toString();
                        Result<byte[]> get = senderImpl.getFile(filename,userId,accUserId,pass);
                        sync.setResult(r.offset(),get);
                        break;
                    case SHARE:
                        filename = op[0].toString();
                        userId = op[1].toString();
                        userIdShare = op[2].toString();
                        pass = op[3].toString();
                        Result<Void> share = senderImpl.shareFile(filename,userId,userIdShare,pass);
                        sync.setResult(r.offset(),share);
                        break;
                    case UNSHARE:
                        filename = op[0].toString();
                        userId = op[1].toString();
                        userIdShare = op[2].toString();
                        pass = op[3].toString();
                        Result<Void> unshare = senderImpl.unshareFile(filename,userId,userIdShare,pass);
                        sync.setResult(r.offset(),unshare);
                        break;
                    case LIST:
                        userId = op[0].toString();
                        pass = op[1].toString();
                        Result<List<FileInfo>> list = senderImpl.lsFile(userId,pass);
                        sync.setResult(r.offset(),list);
                        break;
                    case DELETES:
                        userId = op[0].toString();
                        pass = op[1].toString();
                        String token = op[2].toString();
                        Result<Void> deletes = senderImpl.deleteUserFiles(userId,pass,token);
                        sync.setResult(r.offset(),deletes);
                        break;
                    default:
                        break;
                }
            }
        });
    }

    @Override
    public FileInfo writeFile(Long version, String filename, byte[] data, String userId, String password) {
        return super.repThrow(senderImpl.writeFile(filename,data,userId,password), version);
    }

    @Override
    public void deleteFile(Long version, String filename, String userId, String password) {
        super.repThrow(senderImpl.deleteFile(filename, userId, password),version);
    }

    @Override
    public void shareFile(Long version, String filename, String userId, String userIdShare, String password) {
        super.repThrow(senderImpl.shareFile(filename,userId,userIdShare,password),version);
    }

    @Override
    public void unshareFile(Long version, String filename, String userId, String userIdShare, String password) {
        super.repThrow(senderImpl.unshareFile(filename, userId, userIdShare, password),version);
    }

    @Override
    public byte[] getFile(Long version, String filename, String userId, String accUserId, String password) {
        var res = senderImpl.getFile(filename, userId, accUserId, password);
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

            return super.repThrow(senderImpl.lsFile(userId, password),version);
        } finally {
            System.err.println("TOOK:" + (System.currentTimeMillis() - T0));
        }
    }

    @Override
    public void deleteUserFiles(Long version, String userId, String password, String token) {
        super.repThrow(senderImpl.deleteUserFiles(userId,password,token),version);
    }
}
