package tp1.impl.servers.common.kafka;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.gson.Gson;
import tp1.api.FileInfo;
import tp1.api.User;
import tp1.api.service.java.Directory;
import tp1.api.service.java.Result;

import java.io.File;
import java.lang.reflect.GenericArrayType;
import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import static tp1.api.service.java.Result.ErrorCode.BAD_REQUEST;
import static tp1.api.service.java.Result.error;
import static tp1.impl.clients.Clients.UsersClients;
import tp1.api.service.java.Result.ErrorCode;
import tp1.impl.servers.common.kafka.sync.SyncPoint;
import tp1.impl.servers.rest.DirectoryResources;
import tp1.impl.servers.rest.RepDirectoryResource;
import util.Token;
import util.Version;

public class RepDirectory implements Directory {
    static final long USER_CACHE_EXPIRATION = 3000;

    private static final String WRITE = "write";
    private static final String DELETE = "delete";
    private static final String GET = "get";
    private static final String SHARE = "share";
    private static final String UNSHARE = "unshare";
    private static final String LIST = "list";
    private static final String DELETES = "deletes";

    final LoadingCache<UserInfo, Result<User>> users = CacheBuilder.newBuilder()
            .expireAfterWrite( Duration.ofMillis(USER_CACHE_EXPIRATION))
            .build(     new CacheLoader<>() {
        @Override
        public Result<User> load(UserInfo info) throws Exception {
            var res = UsersClients.get().getUser( info.userId(), info.password());
            if( res.error() == ErrorCode.TIMEOUT)
                return error(BAD_REQUEST);
            else
                return res;
        }
    });

    final static Logger Log = Logger.getLogger(RepDirectory.class.getName());
    final ExecutorService executor = Executors.newCachedThreadPool();

    final Map<String, ExtendedFileInfo> files = new ConcurrentHashMap<>();
    final Map<String, UserFiles> userFiles = new ConcurrentHashMap<>();
    final Map<URI, FileCounts> fileCounts = new ConcurrentHashMap<>();

    static final String KAFKA_BROKERS = "localhost:9092"; // When running in docker container...


    final KafkaPublisher sender;
    //final KafkaSubscriber receiver;
    final SyncPoint<String> sync;
    final Gson json;

    final String topic = RepDirectoryResource.TOPIC;

    public RepDirectory(){
        sender = KafkaPublisher.createPublisher(KAFKA_BROKERS);
        sync = SyncPoint.getInstance();
        //Version.set(-1L);
        json = new Gson();
        /*topicsList.add(Token.get());
        this.receiver = KafkaSubscriber.createSubscriber(KAFKA_BROKERS, topicsList,FROM_BEGINNING);
        this.receiver.start(false, );*/
    }
    @Override
    public Result<FileInfo> writeFile(String filename, byte[] data, String userId, String password) {
        Object[] create = {filename,data,userId,password};
        String result = sync.waitForResult(sender.publish(topic,WRITE, json.toJson(create)));
        return Result.ok(json.fromJson(result,FileInfo.class));
    }

    @Override
    public Result<Void> deleteFile(String filename, String userId, String password) {
        Object[] delete = {filename,userId,password};
        sync.waitForResult(sender.publish(topic,DELETE, json.toJson(delete)));
        return Result.ok();
    }

    @Override
    public Result<Void> shareFile(String filename, String userId, String userIdShare, String password) {
        Object[] share = {filename,userId,userIdShare,password};
        sync.waitForResult(sender.publish(topic,SHARE, json.toJson(share)));
        return Result.ok();
    }

    @Override
    public Result<Void> unshareFile(String filename, String userId, String userIdShare, String password) {
        Object[] unshare = {filename,userId,userIdShare,password};
        sync.waitForResult(sender.publish(topic,UNSHARE, json.toJson(unshare)));
        return Result.ok();
    }

    @Override
    public Result<byte[]> getFile(String filename, String userId, String accUserId, String password) {
        Object[] get = {filename,userId,accUserId,password};
        String result = sync.waitForResult(sender.publish(topic,GET, json.toJson(get)));
        return Result.ok(json.fromJson(result,byte[].class));
    }

    @Override
    public Result<List<FileInfo>> lsFile(String userId, String password) {
        Object[] list = {userId,password};
        String result = sync.waitForResult(sender.publish(topic,LIST, json.toJson(list)));
        FileInfo[] files = json.fromJson(result,FileInfo[].class);
        return Result.ok(Arrays.stream(files).toList());
    }

    @Override
    public Result<Void> deleteUserFiles(String userId, String password, String token) {
        Object[] deleteFiles = {userId,password,token};
        sync.waitForResult(sender.publish(topic,DELETES, json.toJson(deleteFiles)));
        return Result.ok();
    }

    static record ExtendedFileInfo(URI uri, String fileId, FileInfo info) {
    }

    static record UserFiles(Set<String> owned, Set<String> shared) {

        UserFiles() {
            this(ConcurrentHashMap.newKeySet(), ConcurrentHashMap.newKeySet());
        }
    }

    static record FileCounts(URI uri, AtomicLong numFiles) {
        FileCounts( URI uri) {
            this(uri, new AtomicLong(0L) );
        }

        static int ascending(FileCounts a, FileCounts b) {
            return Long.compare( a.numFiles().get(), b.numFiles().get());
        }
    }
    static record UserInfo(String userId, String password) {
    }


}
