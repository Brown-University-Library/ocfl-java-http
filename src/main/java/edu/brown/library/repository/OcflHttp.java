package edu.brown.library.repository;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Pattern;
import javax.json.Json;
import javax.json.JsonObject;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import edu.wisc.library.ocfl.api.exception.NotFoundException;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.Request;
import edu.wisc.library.ocfl.api.OcflRepository;
import edu.wisc.library.ocfl.api.model.FileDetails;
import edu.wisc.library.ocfl.api.model.VersionInfo;
import edu.wisc.library.ocfl.core.OcflRepositoryBuilder;
import edu.wisc.library.ocfl.core.extension.storage.layout.config.HashedTruncatedNTupleIdConfig;
import edu.wisc.library.ocfl.core.storage.filesystem.FileSystemOcflStorage;
import edu.wisc.library.ocfl.api.model.ObjectVersionId;
import edu.wisc.library.ocfl.api.exception.OverwriteException;

import static edu.wisc.library.ocfl.api.OcflOption.OVERWRITE;

public class OcflHttp extends AbstractHandler {

    final Pattern ObjectIdPathPattern = Pattern.compile("^/([a-zA-Z0-9:]+)/([a-zA-Z0-9:]+)$");
    final Pattern ObjectIdPattern = Pattern.compile("^/([a-zA-Z0-9:]+)$");
    final int ChunkSize = 1024;

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
        return Json.createObjectBuilder()
                .add("OCFL ROOT", repoRoot.toString())
                .build();
    }

    void handleRoot(HttpServletResponse response) throws IOException {
        response.setContentType("application/json");
        response.setStatus(HttpServletResponse.SC_OK);
        var output = getRootOutput();
        var writer = Json.createWriter(response.getWriter());
        writer.writeObject(output);
    }

    void handleObjectPath(HttpServletRequest request,
                          HttpServletResponse response,
                          String objectId,
                          String path)
            throws IOException
    {
        if(request.getMethod().equals("POST")) {
            try {
                writeFileToObject(objectId, request.getInputStream(), path, new VersionInfo(), false);
                response.setStatus(HttpServletResponse.SC_CREATED);
            } catch(OverwriteException e) {
                response.setStatus(HttpServletResponse.SC_CONFLICT);
                response.getWriter().print(objectId + "/" + path + " already exists. Use PUT to overwrite it.");
            }
        }
        else {
            try {
                var object = repo.getObject(ObjectVersionId.head(objectId));
                if(request.getMethod().equals("PUT")) {
                    if(object.containsFile(path)) {
                        writeFileToObject(objectId, request.getInputStream(), path, new VersionInfo(), true);
                        response.setStatus(HttpServletResponse.SC_CREATED);
                    }
                    else {
                        response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                        response.getWriter().print(objectId + "/" + path + " doesn't exist. Use POST to create it.");
                    }
                }
                else {
                    if(request.getMethod().equals("GET")) {
                        if(object.containsFile(path)) {
                            response.setStatus(HttpServletResponse.SC_OK);
                            var outputStream = response.getOutputStream();
                            try (var stream = object.getFile(path).getStream()) {
                                outputStream.write(stream.readAllBytes());
                            }
                        }
                        else {
                            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                            response.getWriter().print(objectId + "/" + path + " not found");
                        }
                    }
                }
            } catch(NotFoundException e) {
                response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                response.getWriter().print(objectId + " not found");
            }
        }
    }

    void handleObject(HttpServletResponse response,
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
            var output = Json.createObjectBuilder()
                    .add("files", filesOutput)
                    .build();
            response.setStatus(HttpServletResponse.SC_OK);
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
            var matcher = ObjectIdPathPattern.matcher(pathInfo);
            if (matcher.matches()) {
                var objectId = matcher.group(1);
                var path = matcher.group(2);
                handleObjectPath(request, response, objectId, path);
            }
            else {
                var objectIdMatcher = ObjectIdPattern.matcher(pathInfo);
                if (objectIdMatcher.matches()) {
                    var objectId = objectIdMatcher.group(1);
                    handleObject(response, objectId);
                }
                else {
                    response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                }
            }
        }
        baseRequest.setHandled(true);
    }

    public static void main(String[] args) throws Exception {
        Server server = new Server(8000);
        var ocflHttp = new OcflHttp(
                Path.of("/tmp/ocfl-java-http"),
                Files.createTempDirectory("ocfl-work")
        );
        server.setHandler(ocflHttp);
        server.start();
        server.join();
    }

}
