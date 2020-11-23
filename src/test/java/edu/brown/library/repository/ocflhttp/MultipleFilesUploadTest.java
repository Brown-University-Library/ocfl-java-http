package edu.brown.library.repository.ocflhttp;

import edu.wisc.library.ocfl.api.model.ObjectVersionId;
import edu.wisc.library.ocfl.api.model.VersionInfo;
import org.eclipse.jetty.server.Server;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class MultipleFilesUploadTest {

    Server server;
    OcflHttp ocflHttp;
    Path tmpRoot;
    Path workDir;
    HttpClient client;
    String objectId = "testsuite:1";
    String boundary = "AaB03x";
    String contentTypeHeader = "multipart/form-data; boundary=" + boundary;
    String paramsContentDisposition = "Content-Disposition: form-data; name=\"params\"";
    String file1ContentDisposition = "Content-Disposition: form-data; name=\"files\"; filename=\"file1.txt\"";
    String file2ContentDisposition = "Content-Disposition: form-data; name=\"files\"; filename=\"file2.txt\"";

    @BeforeEach
    private void setup() throws Exception {
        tmpRoot = Files.createTempDirectory("ocfl-java-http");
        workDir = Files.createTempDirectory("ocfl-work");
        ocflHttp = new OcflHttp(tmpRoot, workDir);
        server = OcflHttp.getServer(8000, 8, 60);
        server.setHandler(ocflHttp);
        server.start();
        client = HttpClient.newHttpClient();
    }

    @AfterEach
    private void teardown() throws Exception {
        server.stop();
        TestUtils.deleteDirectory(tmpRoot);
        TestUtils.deleteDirectory(workDir);
    }

    @Test
    public void testPostToExistingObject() throws Exception {
        //POST to an existing object must fail, so we don't accidentally overwrite an existing object when
        // we're trying to ingest a new one.
        ocflHttp.writeFileToObject(objectId,
                new ByteArrayInputStream("asdf".getBytes(StandardCharsets.UTF_8)),
                "initial_file.txt", new VersionInfo(), false);
        var uri = URI.create("http://localhost:8000/" + objectId + "/files?message=adding%20multiple%20files&userName=someone&userAddress=someone%40school.edu");
        var file1Contents = "... contents of file1.txt ...";
        var file2Contents = "...contents of file2.txt...";
        var multipartData = "--" + boundary + "\r\n" +
                paramsContentDisposition + "\r\n" +
                "\r\n" +
                "{}" + "\r\n" +
                "--" + boundary + "\r\n" +
                file1ContentDisposition + "\r\n" +
                "\r\n" +
                file1Contents + "\r\n" +
                "--" + boundary + "\r\n" +
                file2ContentDisposition + "\r\n" +
                "\r\n" +
                file2Contents + "\r\n" +
                "--" + boundary + "--";
        var request = HttpRequest.newBuilder(uri)
                .header("Content-Type", contentTypeHeader)
                .POST(HttpRequest.BodyPublishers.ofString(multipartData)).build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals("object testsuite:1 already exists. Use PUT to update it.", response.body());
        Assertions.assertEquals(409, response.statusCode());
    }

    @Test
    public void testPutToMissingObject() throws Exception {
        var uri = URI.create("http://localhost:8000/" + objectId + "/files?message=adding%20multiple%20files&username=someone&useraddress=someone%40school.edu");
        var file1Contents = "... contents of file1.txt ...";
        var file2Contents = "...contents of file2.txt...";
        var multipartData = "--" + boundary + "\r\n" +
                paramsContentDisposition + "\r\n" +
                "\r\n" +
                "{}" + "\r\n" +
                "--" + boundary + "\r\n" +
                file1ContentDisposition + "\r\n" +
                "\r\n" +
                file1Contents + "\r\n" +
                "--" + boundary + "\r\n" +
                file2ContentDisposition + "\r\n" +
                "\r\n" +
                file2Contents + "\r\n" +
                "--" + boundary + "--";

        //try putting these files - fails because object doesn't exist
        var request = HttpRequest.newBuilder(uri)
                .header("Content-Type", contentTypeHeader)
                .PUT(HttpRequest.BodyPublishers.ofString(multipartData)).build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(404, response.statusCode());
        Assertions.assertEquals("testsuite:1 doesn't exist. Use POST to create it.", response.body());
    }

    @Test
    public void testPutFileAlreadyExists() throws Exception {
        var uri = URI.create("http://localhost:8000/" + objectId + "/files?message=adding%20multiple%20files&username=someone&useraddress=someone%40school.edu");
        var file1Contents = "... contents of file1.txt ...";
        var file2Contents = "...contents of file2.txt...";
        var multipartData = "--" + boundary + "\r\n" +
                paramsContentDisposition + "\r\n" +
                "\r\n" +
                "{}" + "\r\n" +
                "--" + boundary + "\r\n" +
                file1ContentDisposition + "\r\n" +
                "\r\n" +
                file1Contents + "\r\n" +
                "--" + boundary + "\r\n" +
                file2ContentDisposition + "\r\n" +
                "\r\n" +
                file2Contents + "\r\n" +
                "--" + boundary + "--";

        //initialize object
        ocflHttp.writeFileToObject(objectId,
                new ByteArrayInputStream("asdf".getBytes(StandardCharsets.UTF_8)),
                "file1.txt", new VersionInfo(), false);

        //posting the files should fail because file1.txt already exists
        var request = HttpRequest.newBuilder(uri)
                .header("Content-Type", contentTypeHeader)
                .PUT(HttpRequest.BodyPublishers.ofString(multipartData)).build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals("files [file1.txt] already exist. Add updateExisting=yes parameter to the URL to update them.", response.body());
        Assertions.assertEquals(409, response.statusCode());
        var object = ocflHttp.repo.getObject(ObjectVersionId.head(objectId));
        var files = object.getFiles();
        Assertions.assertEquals(1, files.size());
    }

    @Test
    public void testInvalidChecksum() throws Exception {
        var uri = URI.create("http://localhost:8000/" + objectId + "/files?message=adding%20multiple%20files&username=someone&useraddress=someone%40school.edu");
        var file1Contents = "... contents of file1.txt ...";
        var file2Contents = "...contents of file2.txt...";
        var multipartData = "--" + boundary + "\r\n" +
                paramsContentDisposition + "\r\n" +
                "\r\n" +
                "{\"file1.txt\": {\"checksum\": \"a\"}}" + "\r\n" +
                "--" + boundary + "\r\n" +
                file1ContentDisposition + "\r\n" +
                "\r\n" +
                file1Contents + "\r\n" +
                "--" + boundary + "\r\n" +
                file2ContentDisposition + "\r\n" +
                "\r\n" +
                file2Contents + "\r\n" +
                "--" + boundary + "--";
        //test POST
        var request = HttpRequest.newBuilder(uri)
                .header("Content-Type", contentTypeHeader)
                .POST(HttpRequest.BodyPublishers.ofString(multipartData)).build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(409, response.statusCode());

        //test PUT
        ocflHttp.writeFileToObject(objectId,
                new ByteArrayInputStream("asdf".getBytes(StandardCharsets.UTF_8)),
                "file1.txt", new VersionInfo(), false);
        ocflHttp.writeFileToObject(objectId,
                new ByteArrayInputStream("asdf".getBytes(StandardCharsets.UTF_8)),
                "file2.txt", new VersionInfo(), false);
        uri = URI.create("http://localhost:8000/" + objectId + "/files?updateExisting=yes&message=adding%20multiple%20files&username=someone&useraddress=someone%40school.edu");
        request = HttpRequest.newBuilder(uri)
                .header("Content-Type", contentTypeHeader)
                .PUT(HttpRequest.BodyPublishers.ofString(multipartData)).build();
        response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals("Expected MD5 digest: a; Actual: 7a22164bf25f145183087da4784e81f8", response.body());
        Assertions.assertEquals(409, response.statusCode());
    }

    @Test
    public void testInvalidLocation() throws Exception {
        var uri = URI.create("http://localhost:8000/" + objectId + "/files?message=adding%20multiple%20files&userName=someone&userAddress=someone%40school.edu");
        var multipartData = "--" + boundary + "\r\n" +
                paramsContentDisposition + "\r\n" +
                "\r\n" +
                "{\"file1.txt\": {\"location\": \"invalid_uri\"}}" + "\r\n" +
                "--" + boundary + "--";
        var request = HttpRequest.newBuilder(uri)
                .header("Content-Type", contentTypeHeader)
                .POST(HttpRequest.BodyPublishers.ofString(multipartData)).build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(400, response.statusCode());
        Assertions.assertEquals("invalid location: invalid_uri", response.body());

        multipartData = "--" + boundary + "\r\n" +
                paramsContentDisposition + "\r\n" +
                "\r\n" +
                "{\"file1.txt\": {\"location\": \"file:/invalid_uri\"}}" + "\r\n" +
                "--" + boundary + "--";
        request = HttpRequest.newBuilder(uri)
                .header("Content-Type", contentTypeHeader)
                .POST(HttpRequest.BodyPublishers.ofString(multipartData)).build();
        response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(400, response.statusCode());
        Assertions.assertEquals("invalid location - no such file: file:/invalid_uri", response.body());
    }

    @Test
    public void testUploadMultipleFilesPostAndPut() throws Exception {
        var uri = URI.create("http://localhost:8000/" + objectId + "/files?message=adding%20multiple%20files&userName=someone&userAddress=someone%40school.edu");
        var file1Contents = "... contents of file1.txt ...";
        var file2Contents = "...contents of file2.txt...";
        var multipartData = "--" + boundary + "\r\n" +
                paramsContentDisposition + "\r\n" +
                "\r\n" +
                "{}" + "\r\n" +
                "--" + boundary + "\r\n" +
                file1ContentDisposition + "\r\n" +
                "\r\n" +
                file1Contents + "\r\n" +
                "--" + boundary + "\r\n" +
                file2ContentDisposition + "\r\n" +
                "\r\n" +
                file2Contents + "\r\n" +
                "--" + boundary + "--";

        //post the files - creates the object
        var request = HttpRequest.newBuilder(uri)
                .header("Content-Type", contentTypeHeader)
                .POST(HttpRequest.BodyPublishers.ofString(multipartData)).build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(201, response.statusCode());
        var object = ocflHttp.repo.getObject(ObjectVersionId.head(objectId));
        try (var stream = object.getFile("file1.txt").getStream()) {
            Assertions.assertEquals(file1Contents, new String(stream.readAllBytes()));
        }
        try (var stream = object.getFile("file2.txt").getStream()) {
            Assertions.assertEquals("...contents of file2.txt...", new String(stream.readAllBytes()));
        }
        Assertions.assertEquals("adding multiple files", object.getVersionInfo().getMessage());
        var user = object.getVersionInfo().getUser();
        Assertions.assertEquals("someone", user.getName());
        Assertions.assertEquals("someone@school.edu", user.getAddress());

        //now put the files
        uri = URI.create("http://localhost:8000/" + objectId + "/files?updateExisting=yes&message=updating%20multiple%20files&userName=someoneelse&userAddress=someoneelse%40school.edu");
        var newMultipartData = "--" + boundary + "\r\n" +
                paramsContentDisposition + "\r\n" +
                "\r\n" +
                "{}" + "\r\n" +
                "--" + boundary + "\r\n" +
                "Content-Disposition: form-data; name=\"files\"; filename=\"file1.txt\"" + "\r\n" +
                "\r\n" +
                "new file1 contents\r\n" +
                "--" + boundary + "\r\n" +
                "Content-Disposition: form-data; name=\"files\"; filename=\"file2.txt\"" + "\r\n" +
                "\r\n" +
                "... new contents of file2.txt..." + "\r\n" +
                "--" + boundary + "--";
        request = HttpRequest.newBuilder(uri)
                .header("Content-Type", contentTypeHeader)
                .PUT(HttpRequest.BodyPublishers.ofString(newMultipartData)).build();
        response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(201, response.statusCode());
        object = ocflHttp.repo.getObject(ObjectVersionId.head(objectId));
        try (var stream = object.getFile("file2.txt").getStream()) {
            Assertions.assertEquals("... new contents of file2.txt...", new String(stream.readAllBytes()));
        }
        try (var stream = object.getFile("file1.txt").getStream()) {
            Assertions.assertEquals("new file1 contents", new String(stream.readAllBytes()));
        }
        Assertions.assertEquals("updating multiple files", object.getVersionInfo().getMessage());
        user = object.getVersionInfo().getUser();
        Assertions.assertEquals("someoneelse", user.getName());
        Assertions.assertEquals("someoneelse@school.edu", user.getAddress());
    }

    @Test
    public void testLocationMultipleFilesPostAndPut() throws Exception {
        var file1Contents = "... contents of file1.txt ...";
        var file1Sha512 = "d286cd95d51dcb5c7913c45de7778a6be89e64815427eb85045b87a022db1138f63eba25bcad5f24f6e64dbd46e055670c7d70606ffd0b304ee167773a01fa05";
        var file2Contents = "content";
        var file2Path = Path.of(workDir.toString(), "file2.txt");
        Files.write(file2Path, file2Contents.getBytes(StandardCharsets.UTF_8));
        var file2URI = file2Path.toUri();
        var encodedFile2URI = URLEncoder.encode(file2URI.toString(), StandardCharsets.UTF_8);

        var uri = URI.create("http://localhost:8000/" + objectId + "/files?message=adding%20multiple%20files&userName=someone&userAddress=someone%40school.edu");
        var multipartData = "--" + boundary + "\r\n" +
                paramsContentDisposition + "\r\n" +
                "\r\n" +
                "{\"file1.txt\": {\"checksum\": \"" + file1Sha512 + "\", \"checksumType\": \"SHA-512\"}, \"file2.txt\": {\"location\": \"" + encodedFile2URI + "\"}}" + "\r\n" +
                "--" + boundary + "\r\n" +
                file1ContentDisposition + "\r\n" +
                "\r\n" +
                file1Contents + "\r\n" +
                "--" + boundary + "--";

        //POST files - creates the object
        var request = HttpRequest.newBuilder(uri)
                .header("Content-Type", contentTypeHeader)
                .POST(HttpRequest.BodyPublishers.ofString(multipartData)).build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(201, response.statusCode());
        var object = ocflHttp.repo.getObject(ObjectVersionId.head(objectId));
        try (var stream = object.getFile("file1.txt").getStream()) {
            Assertions.assertEquals(file1Contents, new String(stream.readAllBytes()));
        }
        try (var stream = object.getFile("file2.txt").getStream()) {
            Assertions.assertEquals(file2Contents, new String(stream.readAllBytes()));
        }

        //now PUT files
        uri = URI.create("http://localhost:8000/" + objectId + "/files?updateExisting=yes&message=adding%20multiple%20files&userName=someone&userAddress=someone%40school.edu");
        var newFile2Contents = "new file2 contents updated";
        var newFile1Contents = "new file1 contents updated";
        Files.write(file2Path, newFile2Contents.getBytes(StandardCharsets.UTF_8));
        var newMultipartData = "--" + boundary + "\r\n" +
                paramsContentDisposition + "\r\n" +
                "\r\n" +
                "{\"file2.txt\": {\"location\": \"" + encodedFile2URI + "\"}}" + "\r\n" +
                "--" + boundary + "\r\n" +
                file1ContentDisposition + "\r\n" +
                "\r\n" +
                newFile1Contents + "\r\n" +
                "--" + boundary + "--";
        request = HttpRequest.newBuilder(uri)
                .header("Content-Type", contentTypeHeader)
                .PUT(HttpRequest.BodyPublishers.ofString(newMultipartData)).build();
        response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(201, response.statusCode());
        object = ocflHttp.repo.getObject(ObjectVersionId.head(objectId));
        try (var stream = object.getFile("file1.txt").getStream()) {
            Assertions.assertEquals(newFile1Contents, new String(stream.readAllBytes()));
        }
        try (var stream = object.getFile("file2.txt").getStream()) {
            Assertions.assertEquals(newFile2Contents, new String(stream.readAllBytes()));
        }
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
        var uri = URI.create("http://localhost:8000/" + objectId + "/files?message=adding%20file1");
        var contents = "abcdefghij".repeat(40000);
        var multipartData = "--AaB03x\r\n" +
                "Content-Disposition: form-data; name=\"params\"\r\n" +
                "\r\n" +
                "{}" + "\r\n" +
                "--AaB03x\r\n" +
                "Content-Disposition: form-data; name=\"files\"; filename=\"file1.txt\"\r\n" +
                "\r\n" +
                contents + "\r\n" +
                "--AaB03x--";
        var request = HttpRequest.newBuilder(uri)
                .header("Content-Type", "multipart/form-data; boundary=AaB03x")
                .POST(HttpRequest.BodyPublishers.ofString(multipartData)).build();
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
