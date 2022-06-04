package tp1.impl.servers.common.kafka;

import com.google.gson.Gson;
import tp1.api.FileInfo;
import tp1.api.service.java.Files;
import tp1.api.service.java.Result;
import tp1.impl.servers.common.kafka.sync.SyncPoint;
import tp1.impl.servers.rest.FilesResources;
import util.Version;

public class RepFiles implements Files {

    private static final String WRITE = "write";
    private static final String DELETE = "delete";
    private static final String GET = "get";
    private static final String DELETES = "deletes";
    static final String KAFKA_BROKERS = "localhost:9092";
    final KafkaPublisher sender;
    final SyncPoint<String> sync;
    final Gson json;
    final String topic = FilesResources.TOPIC;
    public RepFiles(){
        sender = KafkaPublisher.createPublisher(KAFKA_BROKERS);
        sync = SyncPoint.getInstance();
        Version.set(-1L);
        json = new Gson();
    }

    @Override
    public Result<byte[]> getFile(String fileId, String token) {
        Object[] get = {fileId,token};
        String result = sync.waitForResult(sender.publish(topic,GET, json.toJson(get)));
        return Result.ok(json.fromJson(result, byte[].class));
    }

    @Override
    public Result<Void> deleteFile(String fileId, String token) {
        Object[] delete = {fileId,token};
        sync.waitForResult(sender.publish(topic,DELETE, json.toJson(delete)));
        return Result.ok();
    }

    @Override
    public Result<Void> writeFile(String fileId, byte[] data, String token) {
        Object[] create = {fileId,data,token};
        sync.waitForResult(sender.publish(topic,WRITE, json.toJson(create)));
        return Result.ok();
    }

    @Override
    public Result<Void> deleteUserFiles(String userId, String token) {
        Object[] deleteFiles = {userId,token};
        sync.waitForResult(sender.publish(topic,DELETES, json.toJson(deleteFiles)));
        return Result.ok();
    }
}
