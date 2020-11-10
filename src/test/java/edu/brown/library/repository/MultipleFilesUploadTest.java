package edu.brown.library.repository;

import edu.wisc.library.ocfl.api.model.ObjectVersionId;
import edu.wisc.library.ocfl.api.model.VersionInfo;
import org.eclipse.jetty.server.Server;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

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
    public void testUploadMultipleFiles() throws Exception {
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
        Assertions.assertEquals("testsuite:1 doesn't exist. Use POST to create it with these files.", response.body());

        //initialize object
        ocflHttp.writeFileToObject(objectId,
                new ByteArrayInputStream("asdf".getBytes(StandardCharsets.UTF_8)),
                "initial_file.txt", new VersionInfo(), false);

        //try putting files again - should fail now because those files don't exist
        request = HttpRequest.newBuilder(uri)
                .header("Content-Type", contentTypeHeader)
                .PUT(HttpRequest.BodyPublishers.ofString(multipartData)).build();
        response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(404, response.statusCode());
        Assertions.assertEquals("files [file1.txt, file2.txt] don't exist. Use POST to create them.", response.body());

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
        try (var stream = object.getFile("file2.txt").getStream()) {
            Assertions.assertEquals("...contents of file2.txt...", new String(stream.readAllBytes()));
        }
        Assertions.assertEquals("adding multiple files", object.getVersionInfo().getMessage());
        var user = object.getVersionInfo().getUser();
        Assertions.assertEquals("someone", user.getName());
        Assertions.assertEquals("someone@school.edu", user.getAddress());

        //now put the files - should succeed
        uri = URI.create("http://localhost:8000/" + objectId + "/files?message=updating%20multiple%20files&username=someoneelse&useraddress=someoneelse%40school.edu");
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

        //posting the files should fail now
        request = HttpRequest.newBuilder(uri)
                .header("Content-Type", contentTypeHeader)
                .POST(HttpRequest.BodyPublishers.ofString(newMultipartData)).build();
        response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(409, response.statusCode());
        Assertions.assertEquals("files [file1.txt, file2.txt] already exist. Use PUT to update them.", response.body());
    }

    @Test
    public void testMultipleFilesPOST() throws Exception {
        var file1Contents = "... contents of file1.txt ...";
        var file2Contents = "content";
        var file2MD5Digest = "9a0364b9e99bb480dd25e1f0284c8555";
        var file2Path = Path.of(workDir.toString(), "file2.txt");
        Files.write(file2Path, file2Contents.getBytes(StandardCharsets.UTF_8));
        var file2URI = file2Path.toUri();
        var encodedFile2URI = URLEncoder.encode(file2URI.toString(), StandardCharsets.UTF_8);

        var uri = URI.create("http://localhost:8000/" + objectId + "/files?message=adding%20multiple%20files&username=someone&useraddress=someone%40school.edu");
        var multipartData = "--" + boundary + "\r\n" +
                paramsContentDisposition + "\r\n" +
                "\r\n" +
                "{\"file2.txt\": {\"location\": \"" + encodedFile2URI + "\"}}" + "\r\n" +
                "--" + boundary + "\r\n" +
                file1ContentDisposition + "\r\n" +
                "\r\n" +
                file1Contents + "\r\n" +
                "--" + boundary + "--";

        //initialize object
        ocflHttp.writeFileToObject(objectId,
                new ByteArrayInputStream("asdf".getBytes(StandardCharsets.UTF_8)),
                "initial_file.txt", new VersionInfo(), false);

        //POST files
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
    }
}
