package tp1.impl.servers.common;

import static tp1.api.service.java.Result.ErrorCode.*;
import static tp1.api.service.java.Result.error;
import static tp1.api.service.java.Result.ok;
import static tp1.api.service.java.Result.redirect;
import static tp1.impl.clients.Clients.FilesClients;
import static tp1.impl.clients.Clients.UsersClients;

import java.net.URI;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import com.google.gson.Gson;
import tp1.api.FileInfo;
import tp1.api.User;
import tp1.api.service.java.Directory;

import tp1.api.service.java.Result;
import tp1.api.service.java.Result.ErrorCode;

import tp1.impl.servers.common.kafka.KafkaPublisher;
import tp1.impl.servers.rest.FilesResources;
import util.Hash;
import util.Token;

public class JavaDirectory implements Directory {

    static final long TOKEN_TIME = 10000;
    static final long USER_CACHE_EXPIRATION = 3000;
    static final String KAFKA_BROKERS = "kafka:9092";
    final LoadingCache<UserInfo, Result<User>> users = CacheBuilder.newBuilder()
            .expireAfterWrite(Duration.ofMillis(USER_CACHE_EXPIRATION))
            .build(new CacheLoader<>() {
                @Override
                public Result<User> load(UserInfo info) throws Exception {
                    var res = UsersClients.get().getUser(info.userId(), info.password());
                    if (res.error() == ErrorCode.TIMEOUT)
                        return error(BAD_REQUEST);
                    else
                        return res;
                }
            });

    final static Logger Log = Logger.getLogger(JavaDirectory.class.getName());
    final ExecutorService executor = Executors.newCachedThreadPool();
    final String token = Token.get();
    final Map<String, Map<URI, ExtendedFileInfo>> files = new ConcurrentHashMap<>();
    final Map<String, UserFiles> userFiles = new ConcurrentHashMap<>();
    final Map<URI, FileCounts> fileCounts = new ConcurrentHashMap<>();
    final KafkaPublisher filesPub = KafkaPublisher.createPublisher(KAFKA_BROKERS);
    //final Map<String, Map<URI, Integer>> serverFilesOpen = new ConcurrentHashMap<>();

    @Override
    public Result<FileInfo> writeFile(String filename, byte[] data, String userId, String password) {

        if (badParam(filename) || badParam(userId))
            return error(BAD_REQUEST);

        var user = getUser(userId, password);
        if (!user.isOK())
            return error(user.error());

        var uf = userFiles.computeIfAbsent(userId, (k) -> new UserFiles());
        synchronized (uf) {
            String fileId = fileId(filename, userId);
            Map<URI, ExtendedFileInfo> filesServers = files.get(fileId);
            if (filesServers == null)
                filesServers = new ConcurrentHashMap<>();
            for (var uri : orderCandidateFileServers(filesServers.keySet())) {
                boolean hasSpace = filesServers.size() < 2;
                boolean hasUri = filesServers.get(uri) != null;
                if (hasSpace || hasUri) {
                    //apenas escrever em 2 servdores
                    //usar filesServers.size(); se for 2 e get(uri) == null nao executa
                    // se sizez() < 2 executa smp
                    // se get(uri) != null executa smp
                    String t = fileId + token;
                    String hashToken = Hash.of(t);
                    String tok = System.currentTimeMillis() + JavaFiles.NEW_DELMITER + hashToken;
                    var result = FilesClients.get(uri).writeFile(fileId, data, tok);
                    ExtendedFileInfo file = filesServers.get(uri);
                    var info = file != null ? file.info() : new FileInfo();
                    if (result.isOK()) {
                        info.setOwner(userId);
                        info.setFilename(filename);
                        info.setFileURL(String.format("%s/files/%s", uri, fileId));
                        int counter = file != null ? file.writes + 1 : 1;
                        filesServers.put(uri, file = new ExtendedFileInfo(uri, fileId, counter, info));
                        if (uf.owned().add(fileId))
                            getFileCounts(file.uri(), true).numFiles().incrementAndGet();
                        files.put(fileId, filesServers);
                    } else {
                        //Log.info(String.format("Files.writeFile(...) to %s failed with: %s \n", uri, result));
                    }
                }
            }
            if (!filesServers.isEmpty()) {
                Set<Map.Entry<URI, ExtendedFileInfo>> p = filesServers.entrySet();
                Map.Entry<URI, ExtendedFileInfo> fileInfo = p.stream().toList().get(0);

                return ok(fileInfo.getValue().info());
            }

            return error(BAD_REQUEST);
        }

    }


    @Override
    public Result<Void> deleteFile(String filename, String userId, String password) {
        if (badParam(filename) || badParam(userId))
            return error(BAD_REQUEST);

        var fileId = fileId(filename, userId);

        var file = files.get(fileId);
        if (file == null)
            return error(NOT_FOUND);

        var user = getUser(userId, password);
        if (!user.isOK())
            return error(user.error());

        var uf = userFiles.getOrDefault(userId, new UserFiles());
        synchronized (uf) {
            var info = files.remove(fileId);
            uf.owned().remove(fileId);
            for (Map.Entry<URI, ExtendedFileInfo> entry : info.entrySet()) {
                executor.execute(() -> {
                    this.removeSharesOfFile(entry.getValue());
                    String t = fileId + token;
                    String hashToken = Hash.of(t);
                    String tok = System.currentTimeMillis() + JavaFiles.NEW_DELMITER + hashToken;
                    String[] value = {entry.getValue().fileId(), tok};
                    //FilesClients.get(entry.getKey()).deleteFile(fileId, tok);
                    filesPub.publish(FilesResources.TOPIC, "delete", (new Gson()).toJson(value));
                });

                getFileCounts(entry.getKey(), false).numFiles().decrementAndGet();
            }
        }
        return ok();
    }

    @Override
    public Result<Void> shareFile(String filename, String userId, String userIdShare, String password) {
        if (badParam(filename) || badParam(userId) || badParam(userIdShare))
            return error(BAD_REQUEST);

        var fileId = fileId(filename, userId);

        var file = files.get(fileId);
        if (file == null || getUser(userIdShare, "").error() == NOT_FOUND || file.isEmpty())
            return error(NOT_FOUND);

        var user = getUser(userId, password);
        if (!user.isOK())
            return error(user.error());

        var uf = userFiles.computeIfAbsent(userIdShare, (k) -> new UserFiles());
        synchronized (uf) {
            uf.shared().add(fileId);
            for (ExtendedFileInfo f : file.values())
                f.info().getSharedWith().add(userIdShare);
        }

        return ok();
    }

    @Override
    public Result<Void> unshareFile(String filename, String userId, String userIdShare, String password) {
        if (badParam(filename) || badParam(userId) || badParam(userIdShare))
            return error(BAD_REQUEST);

        var fileId = fileId(filename, userId);

        var file = files.get(fileId);
        if (file == null || getUser(userIdShare, "").error() == NOT_FOUND)
            return error(NOT_FOUND);

        var user = getUser(userId, password);
        if (!user.isOK())
            return error(user.error());

        var uf = userFiles.computeIfAbsent(userIdShare, (k) -> new UserFiles());
        synchronized (uf) {
            uf.shared().remove(fileId);
            for (ExtendedFileInfo f : file.values())
                f.info().getSharedWith().remove(userIdShare);
        }

        return ok();
    }

    @Override
    public Result<byte[]> getFile(String filename, String userId, String accUserId, String password) {
        if (badParam(filename))
            return error(BAD_REQUEST);

        var fileId = fileId(filename, userId);
        var file = files.get(fileId);
        if (file == null || file.isEmpty())
            return error(NOT_FOUND);

        var user = getUser(accUserId, password);
        if (!user.isOK())
            return error(user.error());

        var f = getAndRemoveFile(fileId);
        if (!f.info().hasAccess(accUserId))
            return error(FORBIDDEN);

        String t = fileId + token;
        String hashToken = Hash.of(t);
        String tok = System.currentTimeMillis() + JavaFiles.NEW_DELMITER + hashToken;
        String result = f.info().getFileURL() + "?token=" + tok;
        return redirect(result);
    }

    @Override
    public Result<List<FileInfo>> lsFile(String userId, String password) {
        if (badParam(userId))
            return error(BAD_REQUEST);

        var user = getUser(userId, password);
        if (!user.isOK())
            return error(user.error());

        var uf = userFiles.getOrDefault(userId, new UserFiles());
        synchronized (uf) {
            //for(var uri : orderCandidateFileServers(new HashSet<URI>())) {

            var infos = Stream.concat(uf.owned().stream(), uf.shared().stream())
                    .map(f -> files.get(f).values().stream().toList().get(0).info())
                    .collect(Collectors.toSet());
            return ok(new ArrayList<>(infos));
            //	}
        }
        //return ok(new ArrayList<>());
    }

    public static String fileId(String filename, String userId) {
        return userId + JavaFiles.DELIMITER + filename;
    }

    private static boolean badParam(String str) {
        return str == null || str.length() == 0;
    }

    private Result<User> getUser(String userId, String password) {
        try {
            return users.get(new UserInfo(userId, password));
        } catch (Exception x) {
            x.printStackTrace();
            return error(ErrorCode.INTERNAL_ERROR);
        }
    }

    @Override
    public Result<Void> deleteUserFiles(String userId, String password, String token) {
        users.invalidate(new UserInfo(userId, password));
        String[] t = token.split(JavaFiles.NEW_DELMITER); //t[0] == time t[1] == h(m+k)
        String tok = userId + this.token;
        String hashToken = Hash.of(tok);
        if (System.currentTimeMillis() - Long.parseLong(t[0]) >= TOKEN_TIME) {
            return error(FORBIDDEN);
        } else if (!hashToken.equals(t[1])) {
            return error(FORBIDDEN);
        } else {
            var fileIds = userFiles.remove(userId);
            if (fileIds != null)
                for (var id : fileIds.owned()) {
                    var files = this.files.remove(id);
                    for (ExtendedFileInfo file : files.values()) {
                        removeSharesOfFile(file);
                        getFileCounts(file.uri(), false).numFiles().decrementAndGet();
                    }
                }
            return ok();
        }

    }

    private void removeSharesOfFile(ExtendedFileInfo file) {
        for (var userId : file.info().getSharedWith())
            userFiles.getOrDefault(userId, new UserFiles()).shared().remove(file.fileId());
    }

    private ExtendedFileInfo getAndRemoveFile(String fileId) {
        var serversOpen = FilesClients.alive();
        ExtendedFileInfo result = null;
        var deleteEntries = new LinkedList<Map.Entry<URI, ExtendedFileInfo>>();
        var servers = orderFilesServer(files.get(fileId));
        while (!servers.isEmpty()) {
            Map.Entry<URI, ExtendedFileInfo> e = servers.pop();
            if (serversOpen.contains(e.getKey())) {
                if (result == null)
                    result = e.getValue();
                else if (result.writes() > e.getValue().writes()) {
                    deleteEntries.add(e);
                }
            }
        }
        for (Map.Entry<URI, ExtendedFileInfo> entry : deleteEntries) {
            executor.execute(() -> {
                this.removeSharesOfFile(entry.getValue());
                String t = fileId + token;
                String hashToken = Hash.of(t);
                String tok = System.currentTimeMillis() + JavaFiles.NEW_DELMITER + hashToken;
                String[] value = {entry.getKey().toString(), entry.getValue().fileId(), tok};
                filesPub.publish(FilesResources.TOPIC, "delete", Arrays.toString(value));
            });

            getFileCounts(entry.getKey(), false).numFiles().decrementAndGet();
        }
        return result;
    }

    private Queue<URI> orderCandidateFileServers(Set<URI> files) {
        int MAX_SIZE = 3;
        Queue<URI> result = new ArrayDeque<>();

        if (files != null)
            result.addAll(files);

        FilesClients.all()
                .stream()
                .filter(u -> !result.contains(u))
                .map(u -> getFileCounts(u, false))
                .sorted(FileCounts::ascending)
                .map(FileCounts::uri)
                .limit(MAX_SIZE)
                .forEach(result::add);

        while (result.size() < MAX_SIZE)
            result.add(result.peek());

        //Log.info("Candidate files servers: " + result + "\n");
        return result;
    }

    private Stack<Map.Entry<URI, ExtendedFileInfo>> orderFilesServer(Map<URI, ExtendedFileInfo> files) {
        Stack<Map.Entry<URI, ExtendedFileInfo>> result = new Stack<>();
        var stream = files.entrySet().stream();
        var list = stream.sorted((o1, o2) -> o1.getValue().writes() < o2.getValue().writes() ? -1 : 1).toList();
        result.addAll(list);
        return result;
    }

    private FileCounts getFileCounts(URI uri, boolean create) {
        if (create)
            return fileCounts.computeIfAbsent(uri, FileCounts::new);
        else
            return fileCounts.getOrDefault(uri, new FileCounts(uri));
    }

    private URI getServerAvailable(String fileId) {
        var serversOpen = FilesClients.alive();
        for (URI u : serversOpen) {
            var servers = files.get(fileId);
            var file = servers.get(u);
            if (file != null)
                return u;
        }
        return null;
    }


    static record ExtendedFileInfo(URI uri, String fileId, int writes, FileInfo info) {

        static int ascending(ExtendedFileInfo a, ExtendedFileInfo b) {
            return a.writes() < b.writes() ? -1 : 1;
        }
    }


    static record UserFiles(Set<String> owned, Set<String> shared) {

        UserFiles() {
            this(ConcurrentHashMap.newKeySet(), ConcurrentHashMap.newKeySet());
        }
    }

    static record FileCounts(URI uri, AtomicLong numFiles) {
        FileCounts(URI uri) {
            this(uri, new AtomicLong(0L));
        }

        static int ascending(FileCounts a, FileCounts b) {
            return Long.compare(a.numFiles().get(), b.numFiles().get());
        }
    }

    static record UserInfo(String userId, String password) {
    }
}