package edu.brown.library.repository;

import java.io.IOException;
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
import java.util.List;
import java.util.concurrent.CountDownLatch;

import edu.wisc.library.ocfl.api.model.ObjectVersionId;
import edu.wisc.library.ocfl.api.model.VersionInfo;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;


public class OcflHttpTest {

    Server server;
    OcflHttp ocflHttp;
    Path tmpRoot;
    Path workDir;
    HttpClient client;

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
        server = OcflHttp.getServer(8000, 8, 60);
        server.setHandler(ocflHttp);
        server.start();
        client = HttpClient.newHttpClient();
    }

    @AfterEach
    private void stopServer() throws Exception {
        server.stop();
        deleteDirectory(tmpRoot);
        deleteDirectory(workDir);
    }

    @Test
    public void testGetServer() throws Exception {
        var s = OcflHttp.getServer(5000, -1, -1);
        var threadPool = (QueuedThreadPool)s.getThreadPool();
        Assertions.assertEquals(8, threadPool.getMinThreads());
        Assertions.assertEquals(200, threadPool.getMaxThreads());

        s = OcflHttp.getServer(5000, 4, 300);
        threadPool = (QueuedThreadPool)s.getThreadPool();
        Assertions.assertEquals(4, threadPool.getMinThreads());
        Assertions.assertEquals(300, threadPool.getMaxThreads());
    }

    @Test
    public void testBasicUrls() throws Exception {
        var url = "http://localhost:8000/";
        var request = HttpRequest.newBuilder(URI.create(url)).build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(200, response.statusCode());
        var body = response.body();
        Assertions.assertEquals("{\"OCFL ROOT\":\"" + tmpRoot + "\"}", body);

        //test unhandled/not found url
        url = "http://localhost:8000/not-found";
        request = HttpRequest.newBuilder(URI.create(url)).build();
        response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(404, response.statusCode());
    }

    @Test
    public void testObjectFiles() throws Exception {
        ocflHttp.writeFileToObject("testsuite:1",
                new ByteArrayInputStream("data".getBytes(StandardCharsets.UTF_8)),
                "file1", new VersionInfo(), false);
        var url = "http://localhost:8000/testsuite:1/files";
        var request = HttpRequest.newBuilder(URI.create(url)).build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(200, response.statusCode());
        Assertions.assertEquals("bytes", response.headers().firstValue("Accept-Ranges").get());
        var body = response.body();
        Assertions.assertEquals("{\"files\":{\"file1\":{}}}", body);

        //now test object that doesn't exist
        url = "http://localhost:8000/testsuite:notfound/files";
        request = HttpRequest.newBuilder(URI.create(url)).build();
        response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(404, response.statusCode());
        Assertions.assertEquals("object testsuite:notfound not found", response.body());
    }

    @Test
    public void testGetFileContent() throws Exception {
        var objectId = "testsuite:1";
        //test non-existent object
        var uri = URI.create("http://localhost:8000/" + objectId + "/files/file1/content");
        var request = HttpRequest.newBuilder(uri).GET().build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(404, response.statusCode());
        Assertions.assertEquals("testsuite:1 not found", response.body());
        //now test object exists, but missing file
        ocflHttp.writeFileToObject(objectId,
                new ByteArrayInputStream("data".getBytes(StandardCharsets.UTF_8)),
                "afile", new VersionInfo(), false);
        response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(404, response.statusCode());
        Assertions.assertEquals("testsuite:1/file1 not found", response.body());
        //now test success
        uri = URI.create("http://localhost:8000/" + objectId + "/files/afile/content");
        request = HttpRequest.newBuilder(uri).GET().build();
        response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(200, response.statusCode());
        Assertions.assertEquals("bytes", response.headers().firstValue("Accept-Ranges").get());
        Assertions.assertEquals("4", response.headers().firstValue("Content-Length").get());
        Assertions.assertEquals("text/plain", response.headers().firstValue("Content-Type").get());
        Assertions.assertEquals("data", response.body());
        //test HEAD request
        request = HttpRequest.newBuilder(uri).method("HEAD", HttpRequest.BodyPublishers.noBody()).build();
        response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(200, response.statusCode());
        Assertions.assertEquals("bytes", response.headers().firstValue("Accept-Ranges").get());
        Assertions.assertEquals("4", response.headers().firstValue("Content-Length").get());
        Assertions.assertEquals("text/plain", response.headers().firstValue("Content-Type").get());
        Assertions.assertEquals("", response.body());
        //test with a larger file
        var contents = "abcdefghij".repeat(4000);
        ocflHttp.writeFileToObject(objectId,
                new ByteArrayInputStream(contents.getBytes(StandardCharsets.UTF_8)),
                "biggerfile", new VersionInfo(), false);
        uri = URI.create("http://localhost:8000/" + objectId + "/files/biggerfile/content");
        request = HttpRequest.newBuilder(uri).GET().build();
        response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(contents, response.body());

        //test range requests
        request = HttpRequest.newBuilder(uri).header("Range", "bytes=0-2").GET().build();
        response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(206, response.statusCode());
        Assertions.assertEquals("bytes", response.headers().firstValue("Accept-Ranges").get());
        Assertions.assertEquals("3", response.headers().firstValue("Content-Length").get());
        Assertions.assertEquals("bytes 0-2/40000", response.headers().firstValue("Content-Range").get());
        Assertions.assertEquals("text/plain", response.headers().firstValue("Content-Type").get());
        Assertions.assertEquals("abc", response.body());
        request = HttpRequest.newBuilder(uri).header("Range", "bytes=39998-39999").GET().build();
        response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(206, response.statusCode());
        Assertions.assertEquals("ij", response.body());
        request = HttpRequest.newBuilder(uri).header("Range", "bytes=1-4").GET().build();
        response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(206, response.statusCode());
        Assertions.assertEquals("bcde", response.body());
        request = HttpRequest.newBuilder(uri).header("Range", "bytes=39995-").GET().build();
        response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(206, response.statusCode());
        Assertions.assertEquals("fghij", response.body());
        request = HttpRequest.newBuilder(uri).header("Range", "bytes=-4").GET().build();
        response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(206, response.statusCode());
        Assertions.assertEquals("ghij", response.body());
        //invalid range requests (for this implementation - we don't handle multiple ranges or non-bytes units)
        request = HttpRequest.newBuilder(uri).header("Range", "someunit=1-4").GET().build();
        response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(416, response.statusCode());
        Assertions.assertEquals("bytes */40000", response.headers().firstValue("Content-Range").get());
        Assertions.assertEquals("", response.body());
        request = HttpRequest.newBuilder(uri).header("Range", "bytes=0-499, -500").GET().build();
        response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(416, response.statusCode());
        request = HttpRequest.newBuilder(uri).header("Range", "bytes=40000-40004").GET().build();
        response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(416, response.statusCode());
        request = HttpRequest.newBuilder(uri).header("Range", "bytes=39999-40000").GET().build();
        response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(416, response.statusCode());
    }

    @Test
    public void testUploadFile() throws Exception {
        var objectId = "testsuite:1";
        var uri = URI.create("http://localhost:8000/" + objectId + "/files/file1?message=adding%20file1&username=someone&useraddress=someone%40school.edu");
        var request = HttpRequest.newBuilder(uri)
                .POST(HttpRequest.BodyPublishers.ofString("content")).build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(201, response.statusCode());
        var object = ocflHttp.repo.getObject(ObjectVersionId.head(objectId));
        try (var stream = object.getFile("file1").getStream()) {
            Assertions.assertEquals("content", new String(stream.readAllBytes()));
        }
        Assertions.assertEquals("adding file1", object.getVersionInfo().getMessage());
        var user = object.getVersionInfo().getUser();
        Assertions.assertEquals("someone", user.getName());
        Assertions.assertEquals("someone@school.edu", user.getAddress());

        //now verify that a POST to an existing file fails
        request = HttpRequest.newBuilder(uri)
                .POST(HttpRequest.BodyPublishers.ofString("content update")).build();
        response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(409, response.statusCode());
        Assertions.assertEquals("testsuite:1/file1 already exists. Use PUT to overwrite it.", response.body());

        //now test that a PUT to an existing file succeeds
        request = HttpRequest.newBuilder(uri)
                .PUT(HttpRequest.BodyPublishers.ofString("content update")).build();
        response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(201, response.statusCode());
        object = ocflHttp.repo.getObject(ObjectVersionId.head(objectId));
        try (var stream = object.getFile("file1").getStream()) {
            Assertions.assertEquals("content update", new String(stream.readAllBytes()));
        }

        //test PUT to a non-existent file
        uri = URI.create("http://localhost:8000/" + objectId + "/files/nonexistent");
        request = HttpRequest.newBuilder(uri)
                .PUT(HttpRequest.BodyPublishers.ofString("content")).build();
        response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(404, response.statusCode());
        Assertions.assertEquals("testsuite:1/nonexistent doesn't exist. Use POST to create it.", response.body());
    }

    @Test
    public void testUploadMultipleFiles() throws Exception {
        var objectId = "testsuite:1";
        var uri = URI.create("http://localhost:8000/" + objectId + "/files?message=adding%20multiple%20files&username=someone&useraddress=someone%40school.edu");
        var contentTypeHeader = "multipart/form-data; boundary=AaB03x";
        var boundary = "AaB03x";
        var file1ContentDisposition = "Content-Disposition: form-data; name=\"file1.txt\"; filename=\"file1.txt\"";
        var file1Contents = "... contents of file1.txt ...";
        var multipartData = "--" + boundary + "\r\n" +
                            file1ContentDisposition + "\r\n" +
                            "\r\n" +
                            file1Contents + "\r\n" +
                            "--" + boundary + "\r\n" +
                            "Content-Disposition: form-data; name=\"file2.txt\"; filename=\"file2.txt\"\r\n" +
                            "\r\n" +
                            "...contents of file2.txt...\r\n" +
                            "--" + boundary + "--\r\n";
        //try putting these files - fails because object doesn't exist
        var request = HttpRequest.newBuilder(uri)
                .header("Content-Type", contentTypeHeader)
                .PUT(HttpRequest.BodyPublishers.ofString(multipartData)).build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(404, response.statusCode());
        //create object with different file
        ocflHttp.writeFileToObject(objectId,
                new ByteArrayInputStream("asdf".getBytes(StandardCharsets.UTF_8)),
                "initial_file.txt", new VersionInfo(), false);
        //try putting files again - should fail now because those files don't exist
        request = HttpRequest.newBuilder(uri)
                .header("Content-Type", contentTypeHeader)
                .PUT(HttpRequest.BodyPublishers.ofString(multipartData)).build();
        response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(404, response.statusCode());
        //now post the files - success
        request = HttpRequest.newBuilder(uri)
                .header("Content-Type", contentTypeHeader)
                .POST(HttpRequest.BodyPublishers.ofString(multipartData)).build();
        response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(201, response.statusCode());
        var object = ocflHttp.repo.getObject(ObjectVersionId.head(objectId));
        var files = object.getFiles();
        try (var stream = object.getFile("file1.txt").getStream()) {
            Assertions.assertEquals(file1Contents, new String(stream.readAllBytes()));
        }
        //now put the files - should succeed
        var newMultipartData = "--" + boundary + "\r\n" +
                file1ContentDisposition + "\r\n" +
                "\r\n" +
                "new file1 contents\r\n" +
                "--" + boundary + "\r\n" +
                "Content-Disposition: form-data; name=\"file2.txt\"; filename=\"file2.txt\"\r\n" +
                "\r\n" +
                "... new contents of file2.txt...\r\n" +
                "--" + boundary + "--\r\n";
        request = HttpRequest.newBuilder(uri)
                .header("Content-Type", contentTypeHeader)
                .PUT(HttpRequest.BodyPublishers.ofString(newMultipartData)).build();
        response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(201, response.statusCode());
        object = ocflHttp.repo.getObject(ObjectVersionId.head(objectId));
        try (var stream = object.getFile("file1.txt").getStream()) {
            Assertions.assertEquals("new file1 contents", new String(stream.readAllBytes()));
        }
        //posting the files should fail now
        request = HttpRequest.newBuilder(uri)
                .header("Content-Type", contentTypeHeader)
                .POST(HttpRequest.BodyPublishers.ofString(newMultipartData)).build();
        response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(409, response.statusCode());
    }

    @Test
    public void testConcurrentWrites() throws Exception {
        //https://jodah.net/testing-multi-threaded-code
        var doneSignal = new CountDownLatch(2);
        var posters = List.of(
                new FilePoster(doneSignal),
                new FilePoster(doneSignal)
        );

        for (var poster: posters) {
            new Thread(poster).start();
        }
        doneSignal.await();

        var failCount = 0;
        for (var poster: posters) {
            if (!poster.failure.equals("")) {
                failCount++;
                Assertions.assertEquals("ObjectOutOfSyncException", poster.failure);
            }
        }
        Assertions.assertEquals(1, failCount);
    }
}

class FilePoster implements Runnable {

    private final CountDownLatch doneSignal;
    String failure;

    FilePoster(CountDownLatch doneSignal) {
        this.doneSignal = doneSignal;
        this.failure = "";
    }

    public void run() {
        var objectId = "testsuite:1";
        var uri = URI.create("http://localhost:8000/" + objectId + "/files/file1?message=adding%20file1&username=someone&useraddress=someone%40school.edu");
        var request = HttpRequest.newBuilder(uri)
                .POST(HttpRequest.BodyPublishers.ofString("content")).build();
        var client = HttpClient.newHttpClient();
        try {
            var response = client.send(request, HttpResponse.BodyHandlers.ofString());
            var statusCode = response.statusCode();
            var body = response.body();
            try {
                Assertions.assertEquals(201, statusCode);
            }
            catch(AssertionError e) {
                if(body.contains("ObjectOutOfSyncException")) {
                    failure = "ObjectOutOfSyncException";
                }
                else {
                    failure = "unexpected response: " + statusCode + " - " + body;
                }
            }
        }
        catch(IOException e) {
            failure = e.toString();
        }
        catch(InterruptedException e) {
            failure = e.toString();
        }
        finally {
            doneSignal.countDown();
        }
    }
}
