package edu.brown.library.repository.ocflhttp;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import edu.wisc.library.ocfl.api.OcflRepository;
import edu.wisc.library.ocfl.api.model.ObjectVersionId;
import edu.wisc.library.ocfl.api.model.VersionInfo;
import edu.wisc.library.ocfl.core.OcflRepositoryBuilder;
import edu.wisc.library.ocfl.core.extension.storage.layout.config.HashedNTupleLayoutConfig;
import edu.wisc.library.ocfl.core.storage.filesystem.FileSystemOcflStorage;
import org.eclipse.jetty.server.Server;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class RepoInitTest {

    Server server;
    OcflHttp ocflHttp;
    Path tmp;
    Path tmpRoot;
    Path workDir;
    HttpClient client;
    String boundary = "AaB03x";
    String contentTypeHeader = "multipart/form-data; boundary=" + boundary;
    String paramsContentDisposition = "Content-Disposition: form-data; name=\"params\"";

    @BeforeEach
    private void setup() throws Exception {
        tmp = Path.of(System.getProperty("java.io.tmpdir"));
        tmpRoot = Files.createTempDirectory("ocfl-java-http");
        workDir = Files.createTempDirectory("ocfl-work");
        client = HttpClient.newHttpClient();
    }

    @AfterEach
    private void teardown() throws Exception {
        TestUtils.deleteDirectory(tmpRoot);
        TestUtils.deleteDirectory(workDir);
        if(server != null && server.isRunning()) {
            server.stop();
        }
    }

    @Test
    public void testDifferentStorageLayout() throws Exception {
        //create a repo with a different storage layout
        var objectId = "testsuite:1";
        OcflRepository repo = new OcflRepositoryBuilder()
                .defaultLayoutConfig(new HashedNTupleLayoutConfig()) //instead of HashedNTupleIdEncapsulationLayoutConfig
                .storage(FileSystemOcflStorage.builder().repositoryRoot(tmpRoot).build())
                .workDir(workDir)
                .build();
        repo.updateObject(ObjectVersionId.head(objectId), new VersionInfo(), updater -> {
            updater.writeFile(new ByteArrayInputStream("data".getBytes(StandardCharsets.UTF_8)), "file1.txt");
        });
        var object = repo.getObject(ObjectVersionId.head(objectId));
        var file = object.getFile("file1.txt");
        //make sure repo is using a different storage layout than our default
        Assertions.assertTrue(file.getStorageRelativePath().startsWith("374/130/ee1/374130ee136fd3704ad495d757a5f6536712eebc81a33abcb3ecd102ec616588"));
        //start up OcflHttp & do a GET
        ocflHttp = new OcflHttp(tmpRoot, workDir);
        server = OcflHttp.getServer(8000, 8, 60);
        server.setHandler(ocflHttp);
        server.start();
        var uri = URI.create("http://localhost:8000/" + objectId + "/files/file1.txt/content");
        var request = HttpRequest.newBuilder(uri).GET().build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(200, response.statusCode());
        Assertions.assertEquals("data", response.body());
    }

    @Test
    public void testUploadDirectoryLimited() throws Exception {
        ocflHttp = new OcflHttp(tmpRoot, workDir, 2500000, List.of(workDir));
        server = OcflHttp.getServer(8000, 8, 60);
        server.setHandler(ocflHttp);
        server.start();
        var fileName = "test.txt";
        var filePath = Path.of(tmp.toString(), fileName);
        Files.write(filePath, "asdf".getBytes(StandardCharsets.UTF_8));
        var fileURI = filePath.toUri();
        var objectId = "testsuite:1";

        var uri = URI.create("http://localhost:8000/" + objectId + "/files?message=adding&userName=someone&userAddress=someone%40school.edu");
        var multipartData = "--" + boundary + "\r\n" +
                paramsContentDisposition + "\r\n" +
                "\r\n" +
                "{\"" + fileName + "\": {\"location\": \"" + fileURI + "\"}}" + "\r\n" +
                "--" + boundary + "--";

        //POST files - creates the object
        var request = HttpRequest.newBuilder(uri)
                .header("Content-Type", contentTypeHeader)
                .POST(HttpRequest.BodyPublishers.ofString(multipartData)).build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(400, response.statusCode());
        Assertions.assertTrue(response.body().contains("upload directory not allowed"));
    }

    @Test
    public void testConfig() throws Exception {
        //test no args - just defaults
        var config = new OcflHttpConfig();
        Assertions.assertEquals(OcflHttpConfig.DEFAULT_PORT, config.port);

        //test config file
        var filePath = Path.of(workDir.toString(), "config.json");
        Path userDir = Path.of(System.getProperty("user.dir"));
        var configJson = "{\"OCFL-ROOT\": \"" + tmp.toString().replace("\\", "\\\\") + "\", \"ALLOWED-UPLOAD-DIRS\": [\"" + tmp.toString().replace("\\", "\\\\") + "\",\"" + userDir.toString().replace("\\", "\\\\") + "\"]}";
        System.out.println("configJson: " + configJson);
        Files.write(filePath, configJson.getBytes(StandardCharsets.UTF_8));
        String[] args = {filePath.toString()};
        config = new OcflHttpConfig(args);
        config.allowedUploadDirs.forEach((path) -> {
            System.out.println("path: " + path);
        });
        Assertions.assertTrue(config.allowedUploadDirs.contains(tmp));
        Assertions.assertTrue(config.allowedUploadDirs.contains(userDir));
    }
}
