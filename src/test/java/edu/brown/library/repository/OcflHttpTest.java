package edu.brown.library.repository;

import java.io.IOException;
import java.io.StringReader;
import java.io.ByteArrayInputStream;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.URI;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

import edu.wisc.library.ocfl.api.model.VersionInfo;
import org.eclipse.jetty.server.Server;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;

import javax.json.Json;
import javax.json.JsonObject;


public class OcflHttpTest {

    Server server;
    OcflHttp ocflHttp;
    Path tmpRoot;
    Path workDir;

    private void deleteDirectory(Path dir) throws IOException {
        //https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/nio/file/FileVisitor.html
        Files.walkFileTree(dir, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                    throws IOException
            {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }
            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException e)
                    throws IOException
            {
                if (e == null) {
                    Files.delete(dir);
                    return FileVisitResult.CONTINUE;
                } else {
                    //directory iteration failed
                    throw e;
                }
            }
        });
    }

    @BeforeEach
    private void setupServer() throws Exception {
        tmpRoot = Files.createTempDirectory("ocfl-java-http");
        workDir = Files.createTempDirectory("ocfl-work");
        ocflHttp = new OcflHttp(tmpRoot, workDir);
        server = new Server(8000);
        server.setHandler(ocflHttp);
        server.start();
    }

    @AfterEach
    private void stopServer() throws Exception {
        server.stop();
        deleteDirectory(tmpRoot);
        deleteDirectory(workDir);
    }

    @Test
    public void testRoot() throws Exception {
        var url = "http://localhost:8000/";
        var client = HttpClient.newHttpClient();
        var request = HttpRequest.newBuilder(URI.create(url)).build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(200, response.statusCode());
        var body = response.body();
        Assertions.assertEquals("{\"OCFL ROOT\":\"" + tmpRoot + "\"}", body);
    }

    @Test
    public void testNotFound() throws Exception {
        var url = "http://localhost:8000/not-found";
        var client = HttpClient.newHttpClient();
        var request = HttpRequest.newBuilder(URI.create(url)).build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(404, response.statusCode());
    }

    @Test
    public void testShowObject() throws Exception {
        ocflHttp.writeFileToObject("testsuite:1",
                new ByteArrayInputStream("data".getBytes(StandardCharsets.UTF_8)),
                "file1",
                new VersionInfo());
        var url = "http://localhost:8000/testsuite:1";
        var client = HttpClient.newHttpClient();
        var request = HttpRequest.newBuilder(URI.create(url)).build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(200, response.statusCode());
        var body = response.body();
        Assertions.assertEquals("{\"files\":{\"file1\":{}}}", body);
    }
}