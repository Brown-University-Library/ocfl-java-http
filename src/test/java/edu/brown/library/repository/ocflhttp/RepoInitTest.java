package edu.brown.library.repository.ocflhttp;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

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
    Path tmpRoot;
    Path workDir;
    HttpClient client;

    @BeforeEach
    private void setup() throws Exception {
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
}
