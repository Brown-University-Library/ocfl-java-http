package edu.brown.library.repository;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.regex.Pattern;
import javax.json.Json;
import javax.json.JsonObject;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import edu.wisc.library.ocfl.api.model.OcflObjectVersion;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import edu.wisc.library.ocfl.api.exception.NotFoundException;
import edu.wisc.library.ocfl.api.OcflRepository;
import edu.wisc.library.ocfl.api.model.FileDetails;
import edu.wisc.library.ocfl.api.model.VersionInfo;
import edu.wisc.library.ocfl.api.model.ObjectVersionId;
import edu.wisc.library.ocfl.api.exception.OverwriteException;
import edu.wisc.library.ocfl.core.OcflRepositoryBuilder;
import edu.wisc.library.ocfl.core.extension.storage.layout.config.HashedTruncatedNTupleIdConfig;
import edu.wisc.library.ocfl.core.storage.filesystem.FileSystemOcflStorage;
import org.apache.tika.Tika;

import static edu.wisc.library.ocfl.api.OcflOption.OVERWRITE;

public class OcflHttp extends AbstractHandler {

    final Pattern ObjectIdFilesPattern = Pattern.compile("^/([a-zA-Z0-9:]+)/files$");
    final Pattern ObjectIdPathContentPattern = Pattern.compile("^/([a-zA-Z0-9:]+)/([a-zA-Z0-9:]+)/content$");
    final Pattern ObjectIdPathPattern = Pattern.compile("^/([a-zA-Z0-9:]+)/([a-zA-Z0-9:]+)$");
    final long ChunkSize = 1000L;

    private Path repoRoot;
    OcflRepository repo;

    public OcflHttp(Path root, Path workDir) throws Exception {
        repoRoot = root;
        repo = new OcflRepositoryBuilder()
                .layoutConfig(new HashedTruncatedNTupleIdConfig())
                .storage(FileSystemOcflStorage.builder().repositoryRoot(repoRoot).build())
                .workDir(workDir)
                .build();
    }

    void writeFileToObject(String objectId, InputStream content, String path, VersionInfo versionInfo, boolean overwrite)
    {
        repo.updateObject(ObjectVersionId.head(objectId), versionInfo, updater -> {
                    if(overwrite) {
                        updater.writeFile(content, path, OVERWRITE);
                    }
                    else {
                        updater.writeFile(content, path);
                    }
                });
    }

    JsonObject getRootOutput() {
        return Json.createObjectBuilder().add("OCFL ROOT", repoRoot.toString()).build();
    }

    void handleRoot(HttpServletResponse response) throws IOException {
        response.setContentType("application/json");
        response.setStatus(HttpServletResponse.SC_OK);
        var output = getRootOutput();
        var writer = Json.createWriter(response.getWriter());
        writer.writeObject(output);
    }

    void handleObjectPathPost(HttpServletRequest request,
                              HttpServletResponse response,
                              String objectId,
                              String path)
            throws IOException
    {
        try {
            var versionInfo = new VersionInfo();
            var params = request.getParameterMap();
            var messageParam = params.get("message");
            if(messageParam.length > 0) {
                versionInfo.setMessage(messageParam[0]);
            }
            var userNameParam = params.get("username");
            if(userNameParam.length > 0) {
                var userName = userNameParam[0];
                var userAddressParam = params.get("useraddress");
                var userAddress = "";
                if(userAddressParam.length > 0) {
                    userAddress = userAddressParam[0];
                }
                versionInfo.setUser(userName, userAddress);
            }
            writeFileToObject(objectId, request.getInputStream(), path, versionInfo, false);
            response.setStatus(HttpServletResponse.SC_CREATED);
        } catch(OverwriteException e) {
            response.setStatus(HttpServletResponse.SC_CONFLICT);
            response.getWriter().print(objectId + "/" + path + " already exists. Use PUT to overwrite it.");
        }
    }

    void handleObjectPathGetHead(HttpServletRequest request,
                                 HttpServletResponse response,
                                 String objectId,
                                 OcflObjectVersion object,
                                 String path)
            throws IOException
    {
        if(object.containsFile(path)) {
            response.setStatus(HttpServletResponse.SC_OK);
            response.addHeader("Accept-Ranges", "bytes");
            var file = object.getFile(path);
            try (var stream = file.getStream().enableFixityCheck(false)) {
                var contentType = OcflHttp.getContentType(stream, path);
                response.addHeader("Content-Type", contentType);
            }
            var filePath = repoRoot.resolve(file.getStorageRelativePath());
            var fileSize = Files.size(filePath);
            if(request.getMethod().equals("GET")) {
                var rangeHeader = request.getHeader("Range");
                var start = 0L;
                var end = fileSize - 1L; //end value is included in the range
                if(rangeHeader != null && !rangeHeader.isEmpty()) {
                    var range = OcflHttp.parseRangeHeader(rangeHeader, fileSize);
                    if(range != null) {
                        start = range.getOrDefault("start", start);
                        end = range.getOrDefault("end", end);
                        response.setStatus(HttpServletResponse.SC_PARTIAL_CONTENT);
                        var contentRange = "bytes " + start + "-" + end + "/" + fileSize;
                        response.addHeader("Content-Range", contentRange);
                    }
                    else {
                        response.setStatus(HttpServletResponse.SC_REQUESTED_RANGE_NOT_SATISFIABLE);
                        var contentRange = "bytes */" + fileSize;
                        response.addHeader("Content-Range", contentRange);
                        return;
                    }
                }
                try (var stream = file.getStream().enableFixityCheck(false)) {
                    try (var outputStream = response.getOutputStream()) {
                        byte[] bytesRead;
                        stream.skip(start);
                        var currentPosition = start;
                        while (true) {
                            if(currentPosition + ChunkSize > end) {
                                bytesRead = stream.readNBytes((int)(end + 1 - currentPosition)); //safe - this is less than ChunkSize
                            }
                            else {
                                bytesRead = stream.readNBytes((int)ChunkSize); //safe - ChunkSize isn't huge
                            }
                            if (bytesRead.length > 0) {
                                outputStream.write(bytesRead);
                                currentPosition += bytesRead.length;
                            } else {
                                break;
                            }
                        }
                    }
                }
            }
            else {
                response.addHeader("Content-Length", String.valueOf(fileSize));
            }
        }
        else {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            response.getWriter().print(objectId + "/" + path + " not found");
        }
    }

    void handleObjectPathPut(HttpServletRequest request,
                             HttpServletResponse response,
                             String objectId,
                             OcflObjectVersion object,
                             String path)
            throws IOException
    {
        if(object.containsFile(path)) {
            writeFileToObject(objectId, request.getInputStream(), path, new VersionInfo(), true);
            response.setStatus(HttpServletResponse.SC_CREATED);
        }
        else {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            response.getWriter().print(objectId + "/" + path + " doesn't exist. Use POST to create it.");
        }
    }

    void handleObjectPathContent(HttpServletRequest request,
                                 HttpServletResponse response,
                                 String objectId,
                                 String path)
            throws IOException
    {
        var method = request.getMethod();
        if(method.equals("GET") || method.equals("HEAD")) {
            try {
                var object = repo.getObject(ObjectVersionId.head(objectId));
                handleObjectPathGetHead(request, response, objectId, object, path);
            } catch(NotFoundException e) {
                response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                response.getWriter().print(objectId + " not found");
            }
        }
    }

    void handleObjectPath(HttpServletRequest request,
                          HttpServletResponse response,
                          String objectId,
                          String path)
            throws IOException
    {
        var method = request.getMethod();
        if(method.equals("POST")) {
            handleObjectPathPost(request, response, objectId, path);
        }
        else {
            try {
                var object = repo.getObject(ObjectVersionId.head(objectId));
                if(method.equals("PUT")) {
                    handleObjectPathPut(request, response, objectId, object, path);
                }
            } catch(NotFoundException e) {
                response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                response.getWriter().print(objectId + " not found");
            }
        }
    }

    void handleObjectFiles(HttpServletResponse response,
                      String objectId)
        throws IOException
    {
        if(repo.containsObject(objectId)) {
            var version = repo.describeVersion(ObjectVersionId.head(objectId));
            var files = version.getFiles();
            var emptyOutput = Json.createObjectBuilder();
            var filesOutput = Json.createObjectBuilder();
            for (FileDetails f : files) {
                filesOutput.add(f.getPath(), emptyOutput);
            }
            var output = Json.createObjectBuilder().add("files", filesOutput).build();
            response.setStatus(HttpServletResponse.SC_OK);
            response.addHeader("Accept-Ranges", "bytes");
            var writer = Json.createWriter(response.getWriter());
            writer.writeObject(output);
        }
        else {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            response.getWriter().print("object " + objectId + " not found");
        }
    }

    public void handle(String target,
                       Request baseRequest,
                       HttpServletRequest request,
                       HttpServletResponse response)
            throws IOException
    {
        var pathInfo = request.getPathInfo();
        if (pathInfo.equals("/")) {
            handleRoot(response);
        }
        else {
            var matcher = ObjectIdFilesPattern.matcher(pathInfo);
            if(matcher.matches()) {
                var objectId = matcher.group(1);
                handleObjectFiles(response, objectId);
            }
            else {
                var matcher2 = ObjectIdPathContentPattern.matcher(pathInfo);
                if (matcher2.matches()) {
                    var objectId = matcher2.group(1);
                    var path = matcher2.group(2);
                    handleObjectPathContent(request, response, objectId, path);
                }
                else {
                    var matcher3 = ObjectIdPathPattern.matcher(pathInfo);
                    if (matcher3.matches()) {
                        var objectId = matcher3.group(1);
                        var path = matcher3.group(2);
                        handleObjectPath(request, response, objectId, path);
                    }
                    else {
                        response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                    }
                }
            }
        }
        baseRequest.setHandled(true);
    }

    public static String getContentType(InputStream is, String name) throws IOException {
        /*
        There could be a couple issues with this tika-detection:
        1. The content-type might not be what we expect:
            - we could do the full tika detection, where we read/parse the whole file
            - we could explicitly store the mimetype in a location that OcflHttp knows how to process
                (eg. in a mimetypes.json file for each object, with the filename coming from a config file setting)
                We might want to generalize the file to include more technical metadata.
                Or, could mimetypes be added (through an extension) to inventory.json?
        2. The detection might not be as fast as we want: we could use some kind of caching (with pre-warming).
         */
        var tika = new Tika();
        return tika.detect(is, name);
    }

    public static HashMap<String, Long> parseRangeHeader(String rangeHeader, Long fileSize) {
        try {
            var parts = rangeHeader.split("=");
            if (parts[0].equals("bytes")) {
                Long start;
                Long end;
                var range = new HashMap<String, Long>();
                if (parts[1].startsWith("-")) {
                    var suffixLength = Long.parseLong(parts[1]);
                    start = fileSize + suffixLength; //suffixLength is negative
                    range.put("start", start);
                } else {
                    var numbers = parts[1].split("-");
                    start = Long.parseLong(numbers[0]);
                    range.put("start", start);
                    if (numbers.length > 1) {
                        end = Long.parseLong(numbers[1]);
                        range.put("end", end);
                    }
                }
                if(range.get("start") < 0 || range.get("start") >= fileSize) {
                    return null;
                }
                if(range.getOrDefault("end", 0L) >= fileSize) {
                    return null;
                }
                return range;
            }
        }
        catch(Exception e) {
            //if the parsing fails, we'll return null from this method and that will be handled above
        }
        return null;
    }

    public static Server getServer(int port, int minThreads, int maxThreads) {
        Server server = new Server(port);
        var threadPool = (QueuedThreadPool) server.getThreadPool();
        if(minThreads != -1) {
            threadPool.setMinThreads(minThreads);
        }
        if(maxThreads != -1) {
            threadPool.setMaxThreads(maxThreads);
        }
        return server;
    }

    public static void main(String[] args) throws Exception {
        var port = 8000;
        var minThreads = -1;
        var maxThreads = -1;
        var tmp = System.getProperty("java.io.tmpdir");
        var repoRootDir = Path.of(tmp).resolve("ocfl-java-http");
        if(args.length == 1) {
            var pathToConfigFile = args[0];
            try(InputStream is = Files.newInputStream(Path.of(pathToConfigFile))) {
                var reader = Json.createReader(is);
                var object = reader.readObject();
                repoRootDir = Path.of(object.getString("OCFL-ROOT"));
                port = object.getInt("PORT");
                minThreads = object.getInt("JETTY_MIN_THREADS", -1);
                maxThreads = object.getInt("JETTY_MAX_THREADS", -1);
            }
        }
        var server = getServer(port, minThreads, maxThreads);
        var ocflHttp = new OcflHttp(
                repoRootDir,
                Files.createTempDirectory("ocfl-work")
        );
        server.setHandler(ocflHttp);
        server.start();
        server.join();
    }
}
