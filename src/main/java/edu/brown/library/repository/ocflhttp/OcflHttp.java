package edu.brown.library.repository.ocflhttp;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.text.Normalizer;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.regex.Pattern;
import java.util.logging.Logger;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;

import edu.wisc.library.ocfl.api.exception.*;
import jakarta.servlet.MultipartConfigElement;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.Part;

import edu.wisc.library.ocfl.api.io.FixityCheckInputStream;
import edu.wisc.library.ocfl.api.model.*;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import edu.wisc.library.ocfl.api.OcflRepository;
import edu.wisc.library.ocfl.core.OcflRepositoryBuilder;
import edu.wisc.library.ocfl.core.extension.storage.layout.config.HashedNTupleIdEncapsulationLayoutConfig;
import edu.wisc.library.ocfl.core.storage.filesystem.FileSystemOcflStorage;
import org.apache.tika.Tika;

import static edu.wisc.library.ocfl.api.OcflOption.OVERWRITE;

class InvalidRequestException extends Exception {
    public InvalidRequestException(String errMessage) {
        super(errMessage);
    }
}

public class OcflHttp extends AbstractHandler {

    final String objectIdRegex = "[-:_. %a-zA-Z0-9]+";
    final String fileNameRegex = "[-:_. %a-zA-Z0-9]+";
    final Pattern ObjectIdVersionsPattern = Pattern.compile("^/(" + objectIdRegex + ")/versions$");
    final Pattern ObjectIdFilesPattern = Pattern.compile("^/(" + objectIdRegex + ")/files$");
    final Pattern ObjectIdVersionFilesPattern = Pattern.compile("^/(" + objectIdRegex + ")/v([0-9]+)/files$");
    final Pattern ObjectIdPathPattern = Pattern.compile("^/(" + objectIdRegex + ")/files/(" + fileNameRegex + ")$");
    final Pattern ObjectIdPathContentPattern = Pattern.compile("^/(" + objectIdRegex + ")/files/(" + fileNameRegex + ")/content$");
    final Pattern ObjectIdVersionPathContentPattern = Pattern.compile("^/(" + objectIdRegex + ")/v([0-9]+)/files/(" + fileNameRegex + ")/content$");
    final long ChunkSize = 1000L;
    public static String IfNoneMatchHeader = "If-None-Match";
    public static String IfModifiedSinceHeader = "If-Modified-Since";
    public static String IncludeDeletedParameter = "includeDeleted";
    public static String ObjectTimestampsParameter = "objectTimestamps";
    public static String FieldsParameter = "fields";
    public static DateTimeFormatter IfModifiedFormatter = DateTimeFormatter.ofPattern("E, dd LLL uuuu kk:mm:ss O");
    private static Logger logger = Logger.getLogger("edu.brown.library.repository.ocflhttp");
    private static MultipartConfigElement MULTI_PART_CONFIG;

    private Path repoRoot;
    private List<Path> allowedUploadDirs;
    OcflRepository repo;

    public OcflHttp(Path root, Path workDir) throws Exception {
        this(root, workDir, OcflHttpConfig.DEFAULT_FILE_SIZE_THRESHOLD);
    }

    public OcflHttp(Path root, Path workDir, int fileSizeThreshold) throws Exception {
        this(root, workDir, fileSizeThreshold, List.of());
    }

    public OcflHttp(Path root, Path workDir, int fileSizeThreshold, List<Path> uploadDirs) throws Exception {
        repoRoot = root;
        allowedUploadDirs = uploadDirs;
        var repoBuilder = new OcflRepositoryBuilder();
        repoBuilder.defaultLayoutConfig(new HashedNTupleIdEncapsulationLayoutConfig());
        var ocflJavaWorkDir = workDir.resolve("ocfl-java");
        Files.createDirectories(ocflJavaWorkDir);
        repo = repoBuilder.storage(FileSystemOcflStorage.builder().repositoryRoot(repoRoot).build())
                .workDir(ocflJavaWorkDir)
                .build();
        var jettyWorkDir = workDir.resolve("jetty");
        MULTI_PART_CONFIG = new MultipartConfigElement(jettyWorkDir.toString(), -1L, -1L, fileSizeThreshold);
    }

    void writeFilesToObject(ObjectVersionId objectVersionId, HashMap<String, InputStream> files, VersionInfo versionInfo, boolean overwrite) {
        repo.updateObject(objectVersionId, versionInfo, updater -> {
            files.forEach((fileName, inputStream) -> {
                var fileNameNFC = Normalizer.normalize(fileName, Normalizer.Form.NFC);
                if (overwrite) {
                    updater.writeFile(inputStream, fileNameNFC, OVERWRITE);
                } else {
                    updater.writeFile(inputStream, fileNameNFC);
                }
            });
        });
    }

    void setResponseError(HttpServletResponse response, int statusCode, String msg) throws IOException {
        response.setStatus(statusCode);
        response.getOutputStream().write(msg.getBytes(StandardCharsets.UTF_8.toString()));
    }

    JsonObject getRootOutput() {
        var builder = Json.createObjectBuilder();
        builder.add("OCFL ROOT", repoRoot.toString());
        if (!allowedUploadDirs.isEmpty()) {
            var uploadDirsBuilder = Json.createArrayBuilder();
            allowedUploadDirs.forEach((uploadDir) -> {
                uploadDirsBuilder.add(uploadDir.toString());
            });
            builder.add("ALLOWED-UPLOAD-DIRS", uploadDirsBuilder);
        }
        return builder.build();
    }

    void handleRoot(HttpServletResponse response) throws IOException {
        response.setContentType("application/json");
        response.setStatus(HttpServletResponse.SC_OK);
        var output = getRootOutput();
        var writer = Json.createWriter(response.getWriter());
        writer.writeObject(output);
    }

    void handleObjectVersions(HttpServletRequest request, HttpServletResponse response, String objectId) throws IOException {
        var method = request.getMethod();
        if (method.equals("GET")) {
            if (repo.containsObject(objectId)) {
                var versions = repo.describeObject(objectId).getVersionMap();
                var output = Json.createObjectBuilder();
                versions.forEach((versionNum, versionDetails) -> {
                    var versionOutput = Json.createObjectBuilder();
                    var created = versionDetails.getCreated().withOffsetSameInstant(ZoneOffset.UTC);
                    var message = versionDetails.getVersionInfo().getMessage();
                    var user = versionDetails.getVersionInfo().getUser();
                    String userInfo = "";
                    if (message == null) {
                        message = "";
                    }
                    if (user != null) {
                        String userName = user.getName();
                        if (userName != null) {
                            userInfo = userName;
                        }
                        String userAddress = user.getAddress();
                        if (userAddress != null) {
                            userInfo = userInfo + " <" + userAddress + ">";
                        }
                    }
                    versionOutput.add("created", created.format(DateTimeFormatter.ISO_DATE_TIME));
                    versionOutput.add("user", userInfo);
                    versionOutput.add("message", message);
                    output.add(versionNum.toString(), versionOutput.build());
                });
                var writer = Json.createWriter(response.getWriter());
                writer.writeObject(output.build());
            } else {
                setResponseError(response, HttpServletResponse.SC_NOT_FOUND, objectId + " not found");
            }
        } else {
            setResponseError(response, HttpServletResponse.SC_METHOD_NOT_ALLOWED, "");
        }
    }

    HashMap<String, String> parseUrlParams(String queryString) {
        HashMap<String, String> params = new HashMap<>();
        var paramParts = queryString.split("&");
        for (String paramPart : paramParts) {
            var paramName = paramPart.split("=")[0];
            var paramValue = paramPart.split("=")[1];
            if (!params.containsKey(paramName)) {
                params.put(paramName, URLDecoder.decode(paramValue, StandardCharsets.UTF_8));
            }
        }
        return params;
    }

    VersionInfo getVersionInfo(HttpServletRequest request) throws InvalidRequestException {
        var versionInfo = new VersionInfo();
        var queryString = request.getQueryString();
        if (queryString != null) {
            try {
                var params = parseUrlParams(queryString);
                var messageParam = params.get("message");
                if (messageParam != null) {
                    versionInfo.setMessage(messageParam);
                }
                var userNameParam = params.get("userName");
                if (userNameParam != null) {
                    var userName = userNameParam;
                    var userAddressParam = params.get("userAddress");
                    var userAddress = "";
                    if (userAddressParam != null) {
                        userAddress = userAddressParam;
                    }
                    versionInfo.setUser(userName, userAddress);
                }
                var createdParam = params.get("created");
                if (createdParam != null) {
                    versionInfo.setCreated(OffsetDateTime.parse(createdParam, DateTimeFormatter.ISO_DATE_TIME));
                }
            } catch (Exception e) {
                throw new InvalidRequestException("invalid url params");
            }
        }
        return versionInfo;
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

    HashMap<String, String> getRenameInfo(HttpServletRequest request) throws IOException, ServletException {
        for (Part p : request.getParts()) {
            if (p.getName().equals("rename")) {
                HashMap<String, String> info = new HashMap<String, String>();
                JsonReader reader = Json.createReader(p.getInputStream());
                var renameJson = reader.readObject();
                var oldPath = renameJson.getString("old");
                var newPath = renameJson.getString("new");
                info.put("old", oldPath);
                info.put("new", newPath);
                return info;
            }
        }
        return null;
    }

    HashMap<String, InputStream> getFiles(HttpServletRequest request) throws IOException, ServletException, InvalidRequestException {
        var files = new HashMap<String, InputStream>();
        JsonObject params = null;
        for (Part p : request.getParts()) {
            if (p.getName().equals("params")) {
                JsonReader reader = Json.createReader(p.getInputStream());
                params = reader.readObject();
            } else {
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
                        var fileURI = fileInfo.getString("location");
                        if (fileURI != null && !fileURI.isEmpty()) {
                            try {
                                var path = Path.of(new URI(fileURI));
                                if (uploadDirectoryAllowed(path, allowedUploadDirs)) {
                                    var inputStream = Files.newInputStream(path);
                                    files.put(fileName, inputStream);
                                } else {
                                    throw new InvalidRequestException("invalid location - upload directory not allowed: " + fileURI);
                                }
                            } catch (URISyntaxException e) {
                                logger.warning("URISyntaxException: " + e.getMessage());
                                throw new InvalidRequestException("invalid location: " + fileURI);
                            } catch (IllegalArgumentException e) {
                                logger.warning("IllegalArgumentException: " + e.getMessage());
                                throw new InvalidRequestException("invalid location: " + fileURI);
                            } catch (NoSuchFileException e) {
                                logger.warning(e.getMessage());
                                throw new InvalidRequestException("invalid location - no such file: " + fileURI);
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
                            var inputStream = files.get(fileName);
                            files.put(fileName, new FixityCheckInputStream(inputStream, checksumType, checksum));
                        }
                    }
                }
            }
        } catch (Exception e) {
            closeFilesInputStreams(files);
            throw e;
        }
        return files;
    }

    void handleObjectFilesPost(HttpServletRequest request,
                               HttpServletResponse response,
                               String objectId)
            throws IOException, ServletException {
        try {
            var versionInfo = getVersionInfo(request);
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
        } catch (InvalidRequestException e) {
            logger.warning(e.getMessage());
            setResponseError(response, HttpServletResponse.SC_BAD_REQUEST, e.getMessage());
        }
    }

    void handleObjectFilesPut(HttpServletRequest request,
                              HttpServletResponse response,
                              String objectId)
            throws IOException, ServletException {
        try {
            var versionInfo = getVersionInfo(request);
            var renameInfo = getRenameInfo(request);
            if (renameInfo != null) {
                var oldPath = renameInfo.get("old");
                var newPath = renameInfo.get("new");
                if (repo.containsObject(objectId)) {
                    var object = repo.getObject(ObjectVersionId.head(objectId));
                    if (object.containsFile(oldPath)) {
                        try {
                            repo.updateObject(ObjectVersionId.head(objectId), versionInfo, updater -> {
                                updater.renameFile(oldPath, newPath);
                            });
                            response.setStatus(HttpServletResponse.SC_NO_CONTENT);
                        } catch (OverwriteException e) {
                            setResponseError(response, HttpServletResponse.SC_CONFLICT, newPath + " already exists");
                        }
                    } else {
                        setResponseError(response, HttpServletResponse.SC_NOT_FOUND, oldPath + " doesn't exist");
                    }
                } else {
                    setResponseError(response, HttpServletResponse.SC_NOT_FOUND, objectId + " doesn't exist");
                }
                return;
            }
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
                        if (updateExisting == null || !updateExisting.equals("true")) {
                            var msg = "files " + existingFiles + " already exist. Add updateExisting=true parameter to the URL to update them.";
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
        } catch (InvalidRequestException e) {
            logger.warning(e.getMessage());
            setResponseError(response, HttpServletResponse.SC_BAD_REQUEST, e.getMessage());
        }
    }

    OffsetDateTime getFileLastModifiedUTC(String objectId, String path) {
        var fileChangeHistory = repo.fileChangeHistory(objectId, path);
        //make sure lastModified is converted to UTC
        return fileChangeHistory.getMostRecent().getTimestamp().withOffsetSameInstant(ZoneOffset.UTC);
    }

    void handleObjectPathGetHead(HttpServletRequest request,
                                 HttpServletResponse response,
                                 String objectId,
                                 OcflObjectVersion object,
                                 String path)
            throws IOException {
        handleObjectPathGetHead(request, response, objectId, object, path, -1);
    }

    void handleObjectPathGetHead(HttpServletRequest request,
                                 HttpServletResponse response,
                                 String objectId,
                                 OcflObjectVersion object,
                                 String path,
                                 int versionNum)
            throws IOException {
        if (object.containsFile(path)) {
            response.setStatus(HttpServletResponse.SC_OK);
            response.addHeader("Accept-Ranges", "bytes");
            var file = object.getFile(path);
            try (var stream = file.getStream().enableFixityCheck(false)) {
                var contentType = OcflHttp.getContentType(stream, path);
                response.addHeader("Content-Type", contentType);
            }
            var filePath = repoRoot.resolve(file.getStorageRelativePath());
            var fileSize = Files.size(filePath);
            if (request.getMethod().equals("GET")) {
                var fileLastModifiedUTC = getFileLastModifiedUTC(objectId, path);
                var digestAlgorithm = repo.describeObject(objectId).getDigestAlgorithm();
                var digestValue = file.getFixity().get(digestAlgorithm);
                var ifNoneMatchHeader = request.getHeader(OcflHttp.IfNoneMatchHeader);
                if (ifNoneMatchHeader != null && !ifNoneMatchHeader.isEmpty()) {
                    if (ifNoneMatchHeader.replace("\"", "").equals(digestValue)) {
                        response.setStatus(HttpServletResponse.SC_NOT_MODIFIED);
                        return;
                    }
                }
                var ifModifiedSinceHeader = request.getHeader(OcflHttp.IfModifiedSinceHeader);
                if (ifModifiedSinceHeader != null && !ifModifiedSinceHeader.isEmpty()) {
                    var headerLastModifiedUTC = OffsetDateTime.parse(ifModifiedSinceHeader, OcflHttp.IfModifiedFormatter);
                    if (!fileLastModifiedUTC.truncatedTo(ChronoUnit.SECONDS).isAfter(headerLastModifiedUTC)) {
                        response.setStatus(HttpServletResponse.SC_NOT_MODIFIED);
                        return;
                    }
                }
                var rangeHeader = request.getHeader("Range");
                var start = 0L;
                var end = fileSize - 1L; //end value is included in the range
                if (rangeHeader != null && !rangeHeader.isEmpty()) {
                    var range = OcflHttp.parseRangeHeader(rangeHeader, fileSize);
                    if (range != null) {
                        start = range.getOrDefault("start", start);
                        end = range.getOrDefault("end", end);
                        response.setStatus(HttpServletResponse.SC_PARTIAL_CONTENT);
                        var contentRange = "bytes " + start + "-" + end + "/" + fileSize;
                        response.addHeader("Content-Range", contentRange);
                    } else {
                        response.setStatus(HttpServletResponse.SC_REQUESTED_RANGE_NOT_SATISFIABLE);
                        var contentRange = "bytes */" + fileSize;
                        response.addHeader("Content-Range", contentRange);
                        return;
                    }
                } else {
                    var lastModifiedHeader = fileLastModifiedUTC.format(OcflHttp.IfModifiedFormatter);
                    response.addHeader("Last-Modified", lastModifiedHeader);
                    response.addHeader("ETag", "\"" + digestValue + "\"");
                    response.addHeader("Content-Disposition", "attachment; filename*=UTF-8''" + URLEncoder.encode(path, StandardCharsets.UTF_8));
                    response.addHeader("Content-Length", String.valueOf(fileSize));
                }
                try (var stream = file.getStream().enableFixityCheck(false)) {
                    try (var outputStream = response.getOutputStream()) {
                        byte[] bytesRead;
                        stream.skip(start);
                        var currentPosition = start;
                        while (true) {
                            if (currentPosition + ChunkSize > end) {
                                bytesRead = stream.readNBytes((int) (end + 1 - currentPosition)); //safe - this is less than ChunkSize
                            } else {
                                bytesRead = stream.readNBytes((int) ChunkSize); //safe - ChunkSize isn't huge
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
            } else {
                response.addHeader("Content-Length", String.valueOf(fileSize));
            }
        } else {
            //check for deleted object
            var activeFiles = repo.describeVersion(ObjectVersionId.head(objectId)).getFiles();
            if (activeFiles.isEmpty()) {
                setResponseError(response, HttpServletResponse.SC_GONE, "object " + objectId + " deleted");
                return;
            }
            //check for deleted file
            var allObjectVersions = repo.describeObject(objectId).getVersionMap().values();
            for (VersionDetails v : allObjectVersions) {
                if (versionNum == -1) {
                    for (FileDetails f : v.getFiles()) {
                        if (f.getPath().equals(path)) {
                            setResponseError(response, HttpServletResponse.SC_GONE, "file " + path + " deleted");
                            return;
                        }
                    }
                }
            }
            var msg = objectId + "/" + path + " not found";
            setResponseError(response, HttpServletResponse.SC_NOT_FOUND, msg);
        }
    }

    void handleObjectPathContent(HttpServletRequest request,
                                 HttpServletResponse response,
                                 String objectId,
                                 String path)
            throws IOException {
        var method = request.getMethod();
        if (method.equals("GET") || method.equals("HEAD")) {
            try {
                var object = repo.getObject(ObjectVersionId.head(objectId));
                handleObjectPathGetHead(request, response, objectId, object, path);
            } catch (NotFoundException e) {
                var msg = objectId + " not found";
                setResponseError(response, HttpServletResponse.SC_NOT_FOUND, msg);
            }
        } else {
            setResponseError(response, HttpServletResponse.SC_METHOD_NOT_ALLOWED, "");
        }
    }

    void handleObjectVersionPathContent(HttpServletRequest request,
                                        HttpServletResponse response,
                                        String objectId,
                                        String path,
                                        int versionNum)
            throws IOException {
        var method = request.getMethod();
        if (method.equals("GET") || method.equals("HEAD")) {
            if (repo.containsObject(objectId)) {
                try {
                    var object = repo.getObject(ObjectVersionId.version(objectId, versionNum));
                    handleObjectPathGetHead(request, response, objectId, object, path, versionNum);
                } catch (NotFoundException e) {
                    var msg = "version v" + versionNum + " not found";
                    setResponseError(response, HttpServletResponse.SC_NOT_FOUND, msg);
                }
            } else {
                var msg = objectId + " not found";
                setResponseError(response, HttpServletResponse.SC_NOT_FOUND, msg);
            }
        } else {
            setResponseError(response, HttpServletResponse.SC_METHOD_NOT_ALLOWED, "");
        }
    }

    void handleObjectPath(HttpServletRequest request,
                          HttpServletResponse response,
                          String objectId,
                          String path)
            throws IOException {
        var method = request.getMethod();
        if (method.equals("DELETE")) {
            if (repo.containsObject(objectId)) {
                try {
                    var versionInfo = getVersionInfo(request);
                    var object = repo.getObject(ObjectVersionId.head(objectId));
                    if (object.containsFile(path)) {
                        repo.updateObject(ObjectVersionId.head(objectId), versionInfo, updater -> {
                            updater.removeFile(path);
                        });
                        response.setStatus(204);
                    } else {
                        //see if the file was ever in the object
                        var allObjectVersions = repo.describeObject(objectId).getVersionMap().values();
                        for (VersionDetails v : allObjectVersions) {
                            for (FileDetails f : v.getFiles()) {
                                if (f.getPath().equals(path)) {
                                    response.setStatus(204);
                                    return;
                                }
                            }
                        }
                        //file never existed, so return 404
                        setResponseError(response, HttpServletResponse.SC_NOT_FOUND, path + " not found");
                    }
                } catch (InvalidRequestException e) {
                    setResponseError(response, HttpServletResponse.SC_BAD_REQUEST, e.toString());
                }
            } else {
                setResponseError(response, HttpServletResponse.SC_NOT_FOUND, objectId + " not found");
            }
        } else {
            setResponseError(response, HttpServletResponse.SC_METHOD_NOT_ALLOWED, "");
        }
    }

    void handleObjectFiles(HttpServletRequest request,
                           HttpServletResponse response,
                           String objeectId)
            throws IOException, ServletException {
        handleObjectFiles(request, response, objeectId, -1);
    }

    void handleObjectFiles(HttpServletRequest request,
                           HttpServletResponse response,
                           String objectId,
                           int versionNum)
            throws IOException, ServletException {
        var method = request.getMethod();
        if (method.equals("GET")) {
            if (repo.containsObject(objectId)) {
                var fieldsParam = request.getParameter(FieldsParameter);
                if (fieldsParam == null) {
                    fieldsParam = "";
                }
                var fields = fieldsParam.split(",");
                var filesInfoMap = new HashMap<String, JsonObject>();
                //add all active files
                Collection<FileDetails> activeFiles = null;
                if (versionNum == -1) {
                    activeFiles = repo.describeVersion(ObjectVersionId.head(objectId)).getFiles();
                } else {
                    try {
                        activeFiles = repo.describeVersion(ObjectVersionId.version(objectId, versionNum)).getFiles();
                    } catch (NotFoundException e) {
                        setResponseError(response, HttpServletResponse.SC_NOT_FOUND, "");
                        return;
                    }
                }
                if (activeFiles.isEmpty()) {
                    setResponseError(response, HttpServletResponse.SC_GONE, "object " + objectId + " deleted");
                    return;
                }
                for (FileDetails f : activeFiles) {
                    var info = Json.createObjectBuilder();
                    for (String field : fields) {
                        switch (field) {
                            case "state":
                                info.add("state", "A");
                                break;
                            case "size":
                                var filePath = repoRoot.resolve(f.getStorageRelativePath());
                                var fileSize = Files.size(filePath);
                                info.add("size", fileSize);
                                break;
                            case "mimetype":
                                try (InputStream is = repo.getObject(ObjectVersionId.head(objectId)).getFile(f.getPath()).getStream()) {
                                    var mimetype = getContentType(is, f.getPath());
                                    info.add("mimetype", mimetype);
                                }
                                break;
                            case "checksum":
                                info.add("checksum", f.getFixity().get(DigestAlgorithm.sha512));
                                info.add("checksumType", "SHA-512");
                                break;
                            case "lastModified":
                                var lastModifiedUTC = getFileLastModifiedUTC(objectId, f.getPath());
                                info.add("lastModified", lastModifiedUTC.format(DateTimeFormatter.ISO_DATE_TIME));
                                break;
                        }
                    }
                    filesInfoMap.put(f.getPath(), info.build());
                }
                if (versionNum == -1) {
                    //now fill in deleted files if needed
                    var includeDeletedParam = request.getParameter(IncludeDeletedParameter);
                    if (includeDeletedParam != null && includeDeletedParam.equals("true")) {
                        var allObjectVersions = repo.describeObject(objectId).getVersionMap().values();
                        for (VersionDetails v : allObjectVersions) {
                            //for each version, add information for any file we don't have info for
                            // When we're adding the file, we look at the whole file change history as needed, so
                            // we don't have to worry about the order of the versions in allObjectVersions.
                            for (FileDetails f : v.getFiles()) {
                                if (!filesInfoMap.containsKey(f.getPath())) {
                                    var info = Json.createObjectBuilder();
                                    for (String field : fields) {
                                        switch (field) {
                                            case "state":
                                                info.add("state", "D"); //at this stage we're only adding deleted files
                                                break;
                                            case "size":
                                                var fileChanges = repo.fileChangeHistory(objectId, f.getPath());
                                                var it = fileChanges.getReverseChangeIterator();
                                                while (it.hasNext()) {
                                                    var change = it.next();
                                                    if (!change.getChangeType().equals(FileChangeType.REMOVE)) {
                                                        var filePath = repoRoot.resolve(change.getStorageRelativePath());
                                                        var fileSize = Files.size(filePath);
                                                        info.add("size", fileSize);
                                                        break;
                                                    }
                                                }
                                                break;
                                            case "mimetype":
                                                try (InputStream is = Files.newInputStream(repoRoot.resolve(f.getStorageRelativePath()))) {
                                                    var mimetype = getContentType(is, f.getPath());
                                                    info.add("mimetype", mimetype);
                                                }
                                                break;
                                            case "checksum":
                                                fileChanges = repo.fileChangeHistory(objectId, f.getPath());
                                                it = fileChanges.getReverseChangeIterator();
                                                while (it.hasNext()) {
                                                    var change = it.next();
                                                    if (!change.getChangeType().equals(FileChangeType.REMOVE)) {
                                                        info.add("checksum", change.getFixity().get(DigestAlgorithm.sha512));
                                                        info.add("checksumType", "SHA-512");
                                                        break;
                                                    }
                                                }
                                                break;
                                            case "lastModified":
                                                var lastModifiedUTC = getFileLastModifiedUTC(objectId, f.getPath());
                                                info.add("lastModified", lastModifiedUTC.format(DateTimeFormatter.ISO_DATE_TIME));
                                                break;
                                        }
                                    }
                                    filesInfoMap.put(f.getPath(), info.build());
                                }
                            }
                        }
                    }
                }
                var filesOutput = Json.createObjectBuilder();
                filesInfoMap.forEach((fileName, jsonInfo) -> {
                    filesOutput.add(fileName, jsonInfo);
                });
                var outputBuilder = Json.createObjectBuilder();
                if (versionNum == -1) {
                    outputBuilder.add("version", repo.describeObject(objectId).getHeadVersionNum().toString());
                } else {
                    outputBuilder.add("version", "v" + versionNum);
                }
                outputBuilder.add("files", filesOutput);
                var objectTimestampsParam = request.getParameter(ObjectTimestampsParameter);
                if (objectTimestampsParam != null && objectTimestampsParam.equals("true")) {
                    var objectOutput = Json.createObjectBuilder();
                    objectOutput.add("created", repo.getObject(ObjectVersionId.version(objectId, VersionNum.V1)).getCreated().withOffsetSameInstant(ZoneOffset.UTC).format(DateTimeFormatter.ISO_DATE_TIME));
                    objectOutput.add("lastModified", repo.getObject(ObjectVersionId.head(objectId)).getCreated().withOffsetSameInstant(ZoneOffset.UTC).format(DateTimeFormatter.ISO_DATE_TIME));
                    outputBuilder.add("object", objectOutput);
                }
                var output = outputBuilder.build();
                response.setStatus(HttpServletResponse.SC_OK);
                response.addHeader("Accept-Ranges", "bytes");
                var writer = Json.createWriter(response.getWriter());
                writer.writeObject(output);
            } else {
                setResponseError(response, HttpServletResponse.SC_NOT_FOUND, objectId + " not found");
            }
        } else {
            if (method.equals("DELETE")) {
                try {
                    if (repo.containsObject(objectId)) {
                        var versionInfo = getVersionInfo(request);
                        repo.updateObject(ObjectVersionId.head(objectId), versionInfo, updater -> {
                            repo.getObject(ObjectVersionId.head(objectId)).getFiles().forEach((fileDetails) -> {
                                updater.removeFile(fileDetails.getPath());
                            });
                        });
                        response.setStatus(HttpServletResponse.SC_NO_CONTENT);
                    } else {
                        setResponseError(response, HttpServletResponse.SC_NOT_FOUND, objectId + " not found");
                    }
                }  catch (InvalidRequestException e) {
                    setResponseError(response, HttpServletResponse.SC_BAD_REQUEST, e.toString());
                }
            } else {
                try {
                    if (method.equals("POST")) {
                        handleObjectFilesPost(request, response, objectId);
                    } else {
                        if (method.equals("PUT")) {
                            handleObjectFilesPut(request, response, objectId);
                        }
                    }
                } catch (IllegalStateException e) {
                    var exceptionMsg = e.toString();
                    if (exceptionMsg.contains("Illegal character")) {
                        logger.warning(exceptionMsg);
                        setResponseError(response, HttpServletResponse.SC_BAD_REQUEST, "invalid character in filename");
                    } else {
                        throw e;
                    }
                } catch (OcflJavaException e) {
                    var exceptionMsg = e.toString();
                    if (exceptionMsg.contains("MessageDigest not available")) {
                        setResponseError(response, HttpServletResponse.SC_BAD_REQUEST, exceptionMsg.split(": ")[1]);
                    } else {
                        throw e;
                    }
                }
            }
        }
    }

    private void handleRequest(String target,
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
                objectId = Normalizer.normalize(objectId, Normalizer.Form.NFC);
                handleObjectFiles(request, response, objectId);
            } else {
                var matcher2 = ObjectIdPathContentPattern.matcher(updatedRequestURI);
                if (matcher2.matches()) {
                    var objectId = URLDecoder.decode(matcher2.group(1), StandardCharsets.UTF_8.toString());
                    objectId = Normalizer.normalize(objectId, Normalizer.Form.NFC);
                    var path = URLDecoder.decode(matcher2.group(2), StandardCharsets.UTF_8.toString());
                    path = Normalizer.normalize(path, Normalizer.Form.NFC);
                    handleObjectPathContent(request, response, objectId, path);
                } else {
                    var matcher3 = ObjectIdPathPattern.matcher(updatedRequestURI);
                    if(matcher3.matches()) {
                        var objectId = URLDecoder.decode(matcher3.group(1), StandardCharsets.UTF_8.toString());
                        objectId = Normalizer.normalize(objectId, Normalizer.Form.NFC);
                        var path = URLDecoder.decode(matcher3.group(2), StandardCharsets.UTF_8.toString());
                        path = Normalizer.normalize(path, Normalizer.Form.NFC);
                        handleObjectPath(request, response, objectId, path);
                    } else {
                        var matcher4 = ObjectIdVersionPathContentPattern.matcher(updatedRequestURI);
                        if(matcher4.matches()) {
                            var objectId = URLDecoder.decode(matcher4.group(1), StandardCharsets.UTF_8.toString());
                            objectId = Normalizer.normalize(objectId, Normalizer.Form.NFC);
                            var versionNum = Integer.parseInt(matcher4.group(2));
                            var path = URLDecoder.decode(matcher4.group(3), StandardCharsets.UTF_8.toString());
                            path = Normalizer.normalize(path, Normalizer.Form.NFC);
                            handleObjectVersionPathContent(request, response, objectId, path, versionNum);
                        } else {
                            var matcher5 = ObjectIdVersionFilesPattern.matcher(updatedRequestURI);
                            if(matcher5.matches()) {
                                var objectId = URLDecoder.decode(matcher5.group(1), StandardCharsets.UTF_8.toString());
                                objectId = Normalizer.normalize(objectId, Normalizer.Form.NFC);
                                var versionNum = Integer.parseInt(matcher5.group(2));
                                handleObjectFiles(request, response, objectId, versionNum);
                            } else {
                                var versionsMatcher = ObjectIdVersionsPattern.matcher(updatedRequestURI);
                                if(versionsMatcher.matches()) {
                                    var objectId = URLDecoder.decode(versionsMatcher.group(1), StandardCharsets.UTF_8.toString());
                                    objectId = Normalizer.normalize(objectId, Normalizer.Form.NFC);
                                    handleObjectVersions(request, response, objectId);
                                } else {
                                    response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                                }
                            }
                        }
                    }
                }
            }
        }
        baseRequest.setHandled(true);
    }

    public void handle(String target,
                       Request baseRequest,
                       HttpServletRequest request,
                       HttpServletResponse response)
            throws IOException, ServletException
    {
        try {
            handleRequest(target, baseRequest, request, response);
        } catch(Exception e) {
            logger.log(Level.SEVERE, e.getMessage(), e);
            setResponseError(response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "internal server error");
            baseRequest.setHandled(true);
        }
    }

    public static boolean uploadDirectoryAllowed(Path path, List<Path> uploadDirs) {
        boolean allowed = false;
        if (uploadDirs.isEmpty()) {
            allowed = true;
        } else {
            for (Path dir : uploadDirs) {
                if (path.toAbsolutePath().startsWith(dir)) {
                    allowed = true;
                }
            }
        }
        return allowed;
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
        var config = new OcflHttpConfig(args);
        var server = getServer(config.port, config.minThreads, config.maxThreads);
        var ocflHttp = new OcflHttp(config.repoRootDir, config.workDir, config.fileSizeThreshold, config.allowedUploadDirs);
        server.setHandler(ocflHttp);
        server.start();
        server.join();
    }
}