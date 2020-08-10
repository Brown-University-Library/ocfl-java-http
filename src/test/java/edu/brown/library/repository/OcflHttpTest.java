package edu.brown.library.repository;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.URI;
import java.net.http.HttpResponse;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

import org.eclipse.jetty.server.Server;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;


public class OcflHttpTest {

    Server server;
    Path tmpRoot;

    private void deleteDirectory(Path dir) throws IOException {
        //https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/nio/file/FileVisitor.html
        Files.walkFileTree(tmpRoot, new SimpleFileVisitor<Path>() {
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
        server = new Server(8000);
        server.setHandler(new OcflHttp(tmpRoot));
        server.start();
    }

    @AfterEach
    private void stopServer() throws Exception {
        server.stop();
        deleteDirectory(tmpRoot);
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
}