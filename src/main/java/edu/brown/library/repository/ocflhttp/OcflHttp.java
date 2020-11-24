package edu.brown.library.repository.ocflhttp;

import java.io.InputStream;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.regex.Pattern;
import java.util.logging.Logger;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.servlet.MultipartConfigElement;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.Part;

import edu.wisc.library.ocfl.api.exception.FixityCheckException;
import edu.wisc.library.ocfl.api.exception.ObjectOutOfSyncException;
import edu.wisc.library.ocfl.api.io.FixityCheckInputStream;
import edu.wisc.library.ocfl.api.model.FileDetails;
import edu.wisc.library.ocfl.api.model.ObjectVersionId;
import edu.wisc.library.ocfl.api.model.OcflObjectVersion;
import edu.wisc.library.ocfl.api.model.VersionDetails;
import edu.wisc.library.ocfl.api.model.VersionInfo;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import edu.wisc.library.ocfl.api.exception.NotFoundException;
import edu.wisc.library.ocfl.api.OcflRepository;
import edu.wisc.library.ocfl.core.OcflRepositoryBuilder;
import edu.wisc.library.ocfl.core.extension.storage.layout.config.HashedTruncatedNTupleIdConfig;
import edu.wisc.library.ocfl.core.storage.filesystem.FileSystemOcflStorage;
import org.apache.tika.Tika;

import static edu.wisc.library.ocfl.api.OcflOption.OVERWRITE;

class InvalidLocationException extends Exception {
    public InvalidLocationException(String errMessage) {
        super(errMessage);
    }
}

public class OcflHttp extends AbstractHandler {

    final String objectIdRegex = "[-:_. %a-zA-Z0-9]+";
    final String fileNameRegex = "[-:_. %a-zA-Z0-9]+";
    final Pattern ObjectIdFilesPattern = Pattern.compile("^/(" + objectIdRegex + ")/files$");
    final Pattern ObjectIdPathContentPattern = Pattern.compile("^/(" + objectIdRegex + ")/files/(" + fileNameRegex + ")/content$");
    final long ChunkSize = 1000L;
    public static String IfNoneMatchHeader = "If-None-Match";
    public static String IfModifiedSinceHeader = "If-Modified-Since";
    public static String IncludeDeletedParameter = "includeDeleted";
    public static DateTimeFormatter IfModifiedFormatter = DateTimeFormatter.ofPattern("E, dd LLL uuuu kk:mm:ss O");
    private static final MultipartConfigElement MULTI_PART_CONFIG = new MultipartConfigElement(System.getProperty("java.io.tmpdir"));
    private static Logger logger = Logger.getLogger("edu.brown.library.repository.ocflhttp");

    private Path repoRoot;
    OcflRepository repo;

    public OcflHttp(Path root, Path workDir) throws Exception {
        repoRoot = root;
        var repoBuilder = new OcflRepositoryBuilder();
        if (!Files.list(repoRoot).findAny().isPresent()) {
            //if repoRoot is empty, we'll initialize it with our default config
            repoBuilder.layoutConfig(new HashedTruncatedNTupleIdConfig());
        }
        repo = repoBuilder.storage(FileSystemOcflStorage.builder().repositoryRoot(repoRoot).build())
                .workDir(workDir)
                .build();
    }

    void writeFileToObject(String objectId, InputStream content, String path, VersionInfo versionInfo, boolean overwrite)
    {
        repo.updateObject(ObjectVersionId.head(objectId), versionInfo, updater -> {
                    if(overwrite) {
                        updater.writeFile(content, path, OVERWRITE);
                    } else {
                        updater.writeFile(content, path);
                    }
                });
    }

    void writeFilesToObject(ObjectVersionId objectVersionId, HashMap<String, InputStream> files, VersionInfo versionInfo, boolean overwrite)
    {
        repo.updateObject(objectVersionId, versionInfo, updater -> {
                files.forEach((fileName, inputStream) -> {
                    if(overwrite) {
                        updater.writeFile(inputStream, fileName, OVERWRITE);
                    } else {
                        updater.writeFile(inputStream, fileName);
                    }
                });
        });
    }

    void setResponseError(HttpServletResponse response, int statusCode, String msg) throws IOException
    {
        response.setStatus(statusCode);
        response.getOutputStream().write(msg.getBytes(StandardCharsets.UTF_8.toString()));
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

    VersionInfo getVersionInfo(HttpServletRequest request) {
        var versionInfo = new VersionInfo();
        var params = request.getParameterMap();
        var messageParam = params.get("message");
        if(messageParam != null && messageParam.length > 0) {
            versionInfo.setMessage(messageParam[0]);
        }
        var userNameParam = params.get("userName");
        if(userNameParam != null && userNameParam.length > 0) {
            var userName = userNameParam[0];
            var userAddressParam = params.get("userAddress");
            var userAddress = "";
            if(userAddressParam != null && userAddressParam.length > 0) {
                userAddress = userAddressParam[0];
            }
            versionInfo.setUser(userName, userAddress);
        }
        return versionInfo;
    }

    Path pathFromEncodedURI(String encodedURI) throws URISyntaxException {
        var fileURI = new URI(URLDecoder.decode(encodedURI, StandardCharsets.UTF_8));
        return Path.of(fileURI);
    }

    void closeFilesInputStreams(HashMap<String, InputStream> files) {
        files.forEach((fileName, inputStream) -> {
            try {
                inputStream.close();
            } catch (Exception e) {
                logger.severe(e.getMessage());
            }
        });
    }

    HashMap<String, InputStream> getFiles(HttpServletRequest request) throws IOException, ServletException, InvalidLocationException {
        var files = new HashMap<String, InputStream>();
        JsonObject params = null;
        for(Part p: request.getParts()) {
            if(p.getName().equals("params")) {
                JsonReader reader = Json.createReader(p.getInputStream());
                params = reader.readObject();
            }
            else {
                var fileName = p.getSubmittedFileName();
                files.put(fileName, p.getInputStream());
            }
        }
        var entries = params.entrySet().iterator();
        try {
            while (entries.hasNext()) {
                var entry = entries.next();
                var fileName = entry.getKey();
                var fileInfo = entry.getValue().asJsonObject();
                if (fileInfo != null) {
                    if (fileInfo.containsKey("location")) {
                        var encodedFileURI = fileInfo.getString("location");
                        if (encodedFileURI != null && !encodedFileURI.isEmpty()) {
                            try {
                                var path = pathFromEncodedURI(encodedFileURI);
                                var inputStream = Files.newInputStream(path);
                                files.put(fileName, inputStream);
                            } catch (URISyntaxException | IllegalArgumentException e) {
                                logger.warning(e.getMessage());
                                throw new InvalidLocationException("invalid location: " + encodedFileURI);
                            } catch (NoSuchFileException e) {
                                logger.warning(e.getMessage());
                                throw new InvalidLocationException("invalid location - no such file: " + encodedFileURI);
                            }
                        }
                    }
                    if (fileInfo.containsKey("checksum")) {
                        var checksum = fileInfo.getString("checksum");
                        if (checksum != null && !checksum.isEmpty()) {
                            var checksumType = "MD5";
                            if (fileInfo.containsKey("checksumType")) {
                                checksumType = fileInfo.getString("checksumType");
                                if (checksumType == null || checksumType.isEmpty()) {
                                    checksumType = "MD5";
                                }
                            }
                            files.put(fileName, new FixityCheckInputStream(files.get(fileName), checksumType, checksum));
                        }
                    }
                }
            }
        }
        catch (Exception e) {
            closeFilesInputStreams(files);
            throw e;
        }
        return files;
    }

    void handleObjectFilesPost(HttpServletRequest request,
                               HttpServletResponse response,
                               String objectId)
            throws IOException, ServletException
    {
        var versionInfo = getVersionInfo(request);
        try {
            var files = getFiles(request);
            try {
                try {
                    //version 0 is the way to tell ocfl-java you want to write version 1 of a new object
                    writeFilesToObject(ObjectVersionId.version(objectId, 0), files, versionInfo, false);
                    response.setStatus(HttpServletResponse.SC_CREATED);
                } catch (ObjectOutOfSyncException e) {
                    setResponseError(response, HttpServletResponse.SC_CONFLICT, "object " + objectId + " already exists. Use PUT to update it.");
                } catch (FixityCheckException e) {
                    setResponseError(response, HttpServletResponse.SC_CONFLICT, e.getMessage());
                }
            } finally {
                closeFilesInputStreams(files);
            }
        }
        catch(InvalidLocationException e) {
            logger.warning(e.getMessage());
            setResponseError(response, HttpServletResponse.SC_BAD_REQUEST, e.getMessage());
        }
    }

    void handleObjectFilesPut(HttpServletRequest request,
                               HttpServletResponse response,
                               String objectId)
            throws IOException, ServletException
    {
        var versionInfo = getVersionInfo(request);
        try {
            var files = getFiles(request);
            try {
                if (repo.containsObject(objectId)) {
                    var object = repo.getObject(ObjectVersionId.head(objectId));
                    //check that all files exist
                    var existingFiles = new ArrayList<String>();
                    files.forEach((fileName, inputStream) -> {
                        if (object.containsFile(fileName)) {
                            existingFiles.add(fileName);
                        }
                    });
                    if (!existingFiles.isEmpty()) {
                        var updateExisting = request.getParameter("updateExisting");
                        if (updateExisting == null || !updateExisting.equals("yes")) {
                            var msg = "files " + existingFiles + " already exist. Add updateExisting=yes parameter to the URL to update them.";
                            setResponseError(response, HttpServletResponse.SC_CONFLICT, msg);
                            return;
                        }
                    }
                    try {
                        writeFilesToObject(ObjectVersionId.head(objectId), files, versionInfo, true);
                        response.setStatus(HttpServletResponse.SC_CREATED);
                    } catch (FixityCheckException e) {
                        setResponseError(response, HttpServletResponse.SC_CONFLICT, e.getMessage());
                    }
                } else {
                    var msg = objectId + " doesn't exist. Use POST to create it.";
                    setResponseError(response, HttpServletResponse.SC_NOT_FOUND, msg);
                }
            } finally {
                closeFilesInputStreams(files);
            }
        }
        catch(InvalidLocationException e) {
            logger.warning(e.getMessage());
            setResponseError(response, HttpServletResponse.SC_BAD_REQUEST, e.getMessage());
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
                var fileChangeHistory = repo.fileChangeHistory(objectId, path);
                //make sure lastModified is converted to UTC if needed
                var fileLastModifiedUTC = fileChangeHistory.getMostRecent().getTimestamp().withOffsetSameInstant(ZoneOffset.UTC);
                var digestAlgorithm = repo.describeObject(objectId).getDigestAlgorithm();
                var digestValue = file.getFixity().get(digestAlgorithm);
                var ifNoneMatchHeader = request.getHeader(OcflHttp.IfNoneMatchHeader);
                if(ifNoneMatchHeader != null && !ifNoneMatchHeader.isEmpty()) {
                    if(ifNoneMatchHeader.replace("\"", "").equals(digestValue)) {
                        response.setStatus(HttpServletResponse.SC_NOT_MODIFIED);
                        return;
                    }
                }
                var ifModifiedSinceHeader = request.getHeader(OcflHttp.IfModifiedSinceHeader);
                if(ifModifiedSinceHeader != null && !ifModifiedSinceHeader.isEmpty()) {
                    var headerLastModifiedUTC = OffsetDateTime.parse(ifModifiedSinceHeader, OcflHttp.IfModifiedFormatter);
                    if(!fileLastModifiedUTC.truncatedTo(ChronoUnit.SECONDS).isAfter(headerLastModifiedUTC)) {
                        response.setStatus(HttpServletResponse.SC_NOT_MODIFIED);
                        return;
                    }
                }
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
                else {
                    var lastModifiedHeader = fileLastModifiedUTC.format(OcflHttp.IfModifiedFormatter);
                    response.addHeader("Last-Modified", lastModifiedHeader);
                    response.addHeader("ETag", "\"" + digestValue + "\"");
                    response.addHeader("Content-Disposition", "attachment; filename*=UTF-8''" + URLEncoder.encode(path, StandardCharsets.UTF_8));
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
            var msg = objectId + "/" + path + " not found";
            setResponseError(response, HttpServletResponse.SC_NOT_FOUND, msg);
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
                var msg = objectId + " not found";
                setResponseError(response, HttpServletResponse.SC_NOT_FOUND, msg);
            }
        }
    }

    void handleObjectFiles(HttpServletRequest request,
                           HttpServletResponse response,
                           String objectId)
        throws IOException, ServletException
    {
        var method = request.getMethod();
        if(method.equals("GET")) {
            if (repo.containsObject(objectId)) {
                var filesSet = new HashSet<String>();
                var includeDeletedParam = request.getParameter(IncludeDeletedParameter);
                if(includeDeletedParam != null && includeDeletedParam.equals("1")) {
                    var versionDetails = repo.describeObject(objectId).getVersionMap().values();
                    for(VersionDetails v: versionDetails) {
                        for(FileDetails f: v.getFiles()) {
                            filesSet.add(f.getPath());
                        }
                    }
                }
                else {
                    var version = repo.describeVersion(ObjectVersionId.head(objectId));
                    var files = version.getFiles();
                    for (FileDetails f : files) {
                        filesSet.add(f.getPath());
                    }
                }
                var emptyOutput = Json.createObjectBuilder();
                var filesOutput = Json.createObjectBuilder();
                for(String path: filesSet) {
                    filesOutput.add(path, emptyOutput);
                }
                var output = Json.createObjectBuilder().add("files", filesOutput).build();
                response.setStatus(HttpServletResponse.SC_OK);
                response.addHeader("Accept-Ranges", "bytes");
                var writer = Json.createWriter(response.getWriter());
                writer.writeObject(output);
            } else {
                setResponseError(response, HttpServletResponse.SC_NOT_FOUND, objectId + " not found");
            }
        }
        else {
            if(method.equals("POST")) {
                try {
                    handleObjectFilesPost(request, response, objectId);
                } catch(Exception e) {logger.severe(e.getMessage()); throw e;}
            }
            else {
                if(method.equals("PUT")) {
                    try {
                        handleObjectFilesPut(request, response, objectId);
                    } catch(Exception e) {logger.severe(e.getMessage()); throw e;}
                }
            }
        }
    }

    public void handle(String target,
                       Request baseRequest,
                       HttpServletRequest request,
                       HttpServletResponse response)
            throws IOException, ServletException
    {
        if (request.getContentType() != null && request.getContentType().startsWith("multipart/form-data")) {
            request.setAttribute("org.eclipse.jetty.multipartConfig", MULTI_PART_CONFIG); //should be Request.__MULTIPART_CONFIG_ELEMENT, but that didn't compile
        }
        var requestURI = request.getRequestURI();
        var updatedRequestURI = requestURI.replace("+", "%20");
        if (updatedRequestURI.equals("/")) {
            handleRoot(response);
        }
        else {
            var matcher = ObjectIdFilesPattern.matcher(updatedRequestURI);
            if (matcher.matches()) {
                var objectId = URLDecoder.decode(matcher.group(1), StandardCharsets.UTF_8.toString());
                handleObjectFiles(request, response, objectId);
            } else {
                var matcher2 = ObjectIdPathContentPattern.matcher(updatedRequestURI);
                if (matcher2.matches()) {
                    var objectId = URLDecoder.decode(matcher2.group(1), StandardCharsets.UTF_8.toString());
                    var path = URLDecoder.decode(matcher2.group(2), StandardCharsets.UTF_8.toString());
                    handleObjectPathContent(request, response, objectId, path);
                } else {
                    response.setStatus(HttpServletResponse.SC_NOT_FOUND);
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
        if(!is.markSupported()) {
            is = new BufferedInputStream(is);
        }
        is.mark(100);
        var bytes = is.readNBytes(5);
        //use single byte char set, so it shouldn't error on bytes that could be invalid for UTF-8
        if(new String(bytes, StandardCharsets.ISO_8859_1).equals("<?xml")) {
            return "application/xml";
        }
        is.reset();
        var tika = new Tika();
        var tikaMimetype = tika.detect(is, name);
        if(tikaMimetype.equals("image/x-raw-adobe")) {
            return "image/x-adobe-dng";
        }
        return tikaMimetype;
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
