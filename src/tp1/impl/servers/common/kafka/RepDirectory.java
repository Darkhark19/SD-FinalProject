package tp1.impl.servers.common.kafka;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import tp1.api.FileInfo;
import tp1.api.User;
import tp1.api.service.java.Directory;
import tp1.api.service.java.Result;
import tp1.impl.servers.common.JavaDirectory;

import java.net.URI;
import java.time.Duration;
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

public class RepDirectory implements Directory {
    static final long USER_CACHE_EXPIRATION = 3000;

    final LoadingCache<UserInfo, Result<User>> users = CacheBuilder.newBuilder()
            .expireAfterWrite( Duration.ofMillis(USER_CACHE_EXPIRATION))
            .build(new CacheLoader<>() {
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
    @Override
    public Result<FileInfo> writeFile(String filename, byte[] data, String userId, String password) {
        return null;
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
