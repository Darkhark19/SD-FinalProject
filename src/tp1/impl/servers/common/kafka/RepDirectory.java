package tp1.impl.servers.common.kafka;

import com.google.gson.Gson;
import tp1.api.FileInfo;

import tp1.api.service.java.Directory;
import tp1.api.service.java.Result;


import java.util.Arrays;
import java.util.List;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

import tp1.impl.servers.common.kafka.sync.SyncPoint;
import tp1.impl.servers.rest.DirectoryResources;
import tp1.impl.servers.rest.RepDirectoryResource;

@SuppressWarnings("unchecked")
public class RepDirectory implements Directory {

    private static final String WRITE = "write";
    private static final String DELETE = "delete";
    private static final String GET = "get";
    private static final String SHARE = "share";
    private static final String UNSHARE = "unshare";
    private static final String LIST = "list";
    private static final String DELETES = "deletes";
    final static Logger Log = Logger.getLogger(RepDirectory.class.getName());
    final ExecutorService executor = Executors.newCachedThreadPool();

    static final String KAFKA_BROKERS = "kafka:9092"; // When running in docker container...
    public static final String TOPIC = "directory";

    final KafkaPublisher sender;
    //final KafkaSubscriber receiver;
    final SyncPoint<Result<?>> sync = SyncPoint.getInstance();
    final Gson json;

    final String topic = RepDirectoryResource.TOPIC;

    public RepDirectory(){
        sender = KafkaPublisher.createPublisher(KAFKA_BROKERS);
        json = new Gson();
    }

    @Override
    public Result<FileInfo> writeFile(String filename, byte[] data, String userId, String password) {
        WriteArgs create = new WriteArgs(filename,data,userId,password);
        long value =  sender.publish(topic,WRITE, json.toJson(create,WriteArgs.class));
        return (Result<FileInfo>) sync.waitForResult(value);
    }

    @Override
    public Result<Void> deleteFile(String filename, String userId, String password) {
        String[] delete = {filename,userId,password};

        return (Result<Void>) sync.waitForResult(sender.publish(topic,DELETE, json.toJson(delete)));
    }

    @Override
    public Result<Void> shareFile(String filename, String userId, String userIdShare, String password) {
        String[] share = {filename,userId,userIdShare,password};
        return (Result<Void>)sync.waitForResult(sender.publish(topic,SHARE, json.toJson(share)));
    }

    @Override
    public Result<Void> unshareFile(String filename, String userId, String userIdShare, String password) {
        String[] unshare = {filename,userId,userIdShare,password};
        return (Result<Void>) sync.waitForResult(sender.publish(topic,UNSHARE, json.toJson(unshare)));
    }

    @Override
    public Result<byte[]> getFile(String filename, String userId, String accUserId, String password) {
        String[] get = {filename,userId,accUserId,password};
        return (Result<byte[]>) sync.waitForResult(sender.publish(topic,GET, json.toJson(get)));
    }

    @Override
    public Result<List<FileInfo>> lsFile(String userId, String password) {
        String[] list = {userId,password};
        return (Result<List<FileInfo>>) sync.waitForResult(sender.publish(topic,LIST, json.toJson(list)));
    }

    @Override
    public Result<Void> deleteUserFiles(String userId, String password, String token) {
        String[] deleteFiles = {userId,password,token};

        return  (Result<Void>) sync.waitForResult(sender.publish(topic,DELETES, json.toJson(deleteFiles)));
    }
    public class WriteArgs{
        String filename, userid,pass;
        byte[] data;
        public WriteArgs(String filename, byte[] data, String userid, String pass){
            this.data = data;
            this.filename = filename;
            this.pass = pass;
            this.userid = userid;
        }

        public String getPass() {
            return pass;
        }

        public String getUserId() {
            return userid;
        }

        public String getFilename() {
            return filename;
        }

        public byte[] getData() {
            return data;
        }
    }
}
