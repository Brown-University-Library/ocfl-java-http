package edu.brown.library.repository.ocflhttp;

import edu.wisc.library.ocfl.api.model.DigestAlgorithm;
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
import java.text.Normalizer;
import java.time.format.DateTimeFormatter;
import java.util.stream.Stream;

public class MultipleFilesUploadTest {

    Server server;
    OcflHttp ocflHttp;
    Path tmpRoot;
    Path workDir;
    HttpClient client;
    String objectId = "testsuite:nâtiôn";
    String objectIdNFC = Normalizer.normalize(objectId, Normalizer.Form.NFC);
    String objectIdNFD = Normalizer.normalize(objectId, Normalizer.Form.NFD);
    String encodedObjectId = URLEncoder.encode(objectId, StandardCharsets.UTF_8);
    String encodedObjectIdNFC = URLEncoder.encode(objectIdNFC, StandardCharsets.UTF_8);
    String encodedObjectIdNFD = URLEncoder.encode(objectIdNFD, StandardCharsets.UTF_8);
    String file1Name = "nâtiôn.txt";
    String file1NameNFC = Normalizer.normalize(file1Name, Normalizer.Form.NFC);
    String file1NameNFD = Normalizer.normalize(file1Name, Normalizer.Form.NFD);
    String encodedFile1NameNFC = URLEncoder.encode(file1NameNFC, StandardCharsets.UTF_8);
    String encodedFile1NameNFD = URLEncoder.encode(file1NameNFD, StandardCharsets.UTF_8);
    String boundary = "AaB03x";
    String contentTypeHeader = "multipart/form-data; boundary=" + boundary;
    String renameContentDisposition = "Content-Disposition: form-data; name=\"rename\"";
    String paramsContentDisposition = "Content-Disposition: form-data; name=\"params\"";
    String file1ContentDisposition = "Content-Disposition: form-data; name=\"files\"; filename=\"" + file1NameNFC + "\"";
    String file2ContentDisposition = "Content-Disposition: form-data; name=\"files\"; filename=\"file2.txt\"";
    String message = "“iñtërnâtiônàlĭzætiøn” message";
    String messageNFC = Normalizer.normalize(message, Normalizer.Form.NFC);
    String encodedMessageNFC = URLEncoder.encode(messageNFC, StandardCharsets.UTF_8);

    @BeforeEach
    private void setup() throws Exception {
        tmpRoot = Files.createTempDirectory("ocfl-java-http-tests");
        workDir = Files.createTempDirectory("ocfl-java-http-tests-work");
        ocflHttp = new OcflHttp(tmpRoot, workDir, 1000);
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
        ocflHttp.repo.updateObject(ObjectVersionId.head(objectId), new VersionInfo(), updater -> {
                updater.writeFile(new ByteArrayInputStream("asdf".getBytes(StandardCharsets.UTF_8)),"initial_file.txt");
        });
        var uri = URI.create("http://localhost:8000/" + objectId + "/files?message=adding%20multiple%20files&userName=someone&userAddress=someone%40school.edu");
        var file1Contents = "... contents of first file ...";
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
        Assertions.assertEquals("object " + objectId + " already exists. Use PUT to update it.", response.body());
        Assertions.assertEquals(409, response.statusCode());
    }

    @Test
    public void testPutToMissingObject() throws Exception {
        var uri = URI.create("http://localhost:8000/" + objectId + "/files?message=adding%20multiple%20files&username=someone&useraddress=someone%40school.edu");
        var file1Contents = "... contents of first file ...";
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
        Assertions.assertEquals(objectId + " doesn't exist. Use POST to create it.", response.body());
    }

    @Test
    public void testFileAlreadyExists() throws Exception {
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
        ocflHttp.repo.updateObject(ObjectVersionId.head(objectId), new VersionInfo(), updater -> {
                    updater.writeFile(new ByteArrayInputStream("asdf".getBytes(StandardCharsets.UTF_8)), file1Name);
                });

        //PUT should fail because file1.txt already exists
        var request = HttpRequest.newBuilder(uri)
                .header("Content-Type", contentTypeHeader)
                .PUT(HttpRequest.BodyPublishers.ofString(multipartData)).build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals("files [" + file1Name + "] already exist. Add updateExisting=true parameter to the URL to update them.", response.body());
        Assertions.assertEquals(409, response.statusCode());
        var object = ocflHttp.repo.getObject(ObjectVersionId.head(objectId));
        var files = object.getFiles();
        Assertions.assertEquals(1, files.size());
    }

    @Test
    public void testInvalidChecksumType() throws Exception {
        var uri = URI.create("http://localhost:8000/" + objectId + "/files?message=adding%20multiple%20files&username=someone&useraddress=someone%40school.edu");
        var file1Contents = "... contents of first file ...";
        var multipartData = "--" + boundary + "\r\n" +
                paramsContentDisposition + "\r\n" +
                "\r\n" +
                "{\"" + file1Name + "\": {\"checksumType\": \"SHA-890\", \"checksum\": \"abcd\"}}" + "\r\n" +
                "--" + boundary + "\r\n" +
                file1ContentDisposition + "\r\n" +
                "\r\n" +
                file1Contents + "\r\n" +
                "--" + boundary + "--";
        //test POST
        var request = HttpRequest.newBuilder(uri)
                .header("Content-Type", contentTypeHeader)
                .POST(HttpRequest.BodyPublishers.ofString(multipartData)).build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(400, response.statusCode());
        Assertions.assertEquals("SHA-890 MessageDigest not available", response.body());

        //test PUT
        ocflHttp.repo.updateObject(ObjectVersionId.head(objectId), new VersionInfo(), updater -> {
            updater.writeFile(new ByteArrayInputStream("asdf".getBytes(StandardCharsets.UTF_8)), file1Name);
        });
        uri = URI.create("http://localhost:8000/" + objectId + "/files?updateExisting=true&message=adding%20multiple%20files&username=someone&useraddress=someone%40school.edu");
        request = HttpRequest.newBuilder(uri)
                .header("Content-Type", contentTypeHeader)
                .PUT(HttpRequest.BodyPublishers.ofString(multipartData)).build();
        response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(400, response.statusCode());
        Assertions.assertEquals("SHA-890 MessageDigest not available", response.body());
    }

    @Test
    public void testInvalidChecksum() throws Exception {
        var uri = URI.create("http://localhost:8000/" + objectId + "/files?message=adding%20multiple%20files&username=someone&useraddress=someone%40school.edu");
        var file1Contents = "... contents of first file ...";
        var file2Contents = "...contents of file2.txt...";
        var multipartData = "--" + boundary + "\r\n" +
                paramsContentDisposition + "\r\n" +
                "\r\n" +
                "{\"" + file1Name + "\": {\"checksum\": \"a\"}}" + "\r\n" +
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
        Assertions.assertEquals(response.body(), "Expected MD5 digest: a; Actual: 56e51396188e1a46860c409c274f83a4");

        //test PUT
        ocflHttp.repo.updateObject(ObjectVersionId.head(objectId), new VersionInfo(), updater -> {
                    updater.writeFile(new ByteArrayInputStream("asdf".getBytes(StandardCharsets.UTF_8)), file1Name);
                });
        ocflHttp.repo.updateObject(ObjectVersionId.head(objectId), new VersionInfo(), updater -> {
                    updater.writeFile(new ByteArrayInputStream("asdf".getBytes(StandardCharsets.UTF_8)), "file2.txt");
                });
        uri = URI.create("http://localhost:8000/" + objectId + "/files?updateExisting=true&message=adding%20multiple%20files&username=someone&useraddress=someone%40school.edu");
        request = HttpRequest.newBuilder(uri)
                .header("Content-Type", contentTypeHeader)
                .PUT(HttpRequest.BodyPublishers.ofString(multipartData)).build();
        response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(response.body(), "Expected MD5 digest: a; Actual: 56e51396188e1a46860c409c274f83a4");
        Assertions.assertEquals(409, response.statusCode());
    }

    @Test
    public void testInvalidLocation() throws Exception {
        var file1Path = Path.of(workDir.toString(), "file1.txt");
        Files.write(file1Path, "file1Contents".getBytes(StandardCharsets.UTF_8));
        var file1URI = file1Path.toUri();
        var params = "{\"" + file1Name + "\": {\"location\": \"" + file1URI + "\"}, "
                + "\"file2.txt\": {\"location\": \"invalid_uri\"}}";

        var uri = URI.create("http://localhost:8000/" + objectId + "/files?message=adding%20multiple%20files&userName=someone&userAddress=someone%40school.edu");
        var multipartData = "--" + boundary + "\r\n" +
                paramsContentDisposition + "\r\n" +
                "\r\n" +
                params + "\r\n" +
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
                "{\"" + file1Name + "\": {\"location\": \"file:/invalid_uri\"}}" + "\r\n" +
                "--" + boundary + "--";
        request = HttpRequest.newBuilder(uri)
                .header("Content-Type", contentTypeHeader)
                .POST(HttpRequest.BodyPublishers.ofString(multipartData)).build();
        response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(400, response.statusCode());
        Assertions.assertEquals("invalid location - no such file: file:/invalid_uri", response.body());
    }

    @Test
    public void testInvalidCharacterInFileName() throws Exception {
        var uri = URI.create("http://localhost:8000/" + objectId + "/files?message=adding%20multiple%20files&userName=someone&userAddress=someone%40school.edu");
        var file1Contents = "... contents of first file ...";
        String filename = "b\0.txt";
        file1ContentDisposition = "Content-Disposition: form-data; name=\"files\"; filename=\"" + filename + "\"";
        var multipartData = "--" + boundary + "\r\n" +
                paramsContentDisposition + "\r\n" +
                "\r\n" +
                "{}" + "\r\n" +
                "--" + boundary + "\r\n" +
                file1ContentDisposition + "\r\n" +
                "\r\n" +
                file1Contents + "\r\n" +
                "--" + boundary + "--";
        var request = HttpRequest.newBuilder(uri)
                .header("Content-Type", contentTypeHeader)
                .POST(HttpRequest.BodyPublishers.ofString(multipartData)).build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(400, response.statusCode());
        Assertions.assertEquals("invalid character in filename", response.body());
    }

    @Test
    public void testUploadMultipleFilesPostAndPut() throws Exception {
        var uri = URI.create("http://localhost:8000/" + objectId + "/files?message=" + encodedMessageNFC + "&userName=someone&userAddress=someone%40school.edu");
        var file1Contents = "... contents of first file ...";
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
        try (var stream = object.getFile(file1Name).getStream()) {
            Assertions.assertEquals(file1Contents, new String(stream.readAllBytes()));
        }
        try (var stream = object.getFile("file2.txt").getStream()) {
            Assertions.assertEquals("...contents of file2.txt...", new String(stream.readAllBytes()));
        }
        Assertions.assertEquals(messageNFC, object.getVersionInfo().getMessage());
        var user = object.getVersionInfo().getUser();
        Assertions.assertEquals("someone", user.getName());
        Assertions.assertEquals("someone@school.edu", user.getAddress());

        //now put the files
        uri = URI.create("http://localhost:8000/" + objectId + "/files?updateExisting=true&message=updating%20multiple%20files%20%26%20so%20on&userName=someoneelse&userAddress=someoneelse%40school.edu");
        var newMultipartData = "--" + boundary + "\r\n" +
                paramsContentDisposition + "\r\n" +
                "\r\n" +
                "{}" + "\r\n" +
                "--" + boundary + "\r\n" +
                "Content-Disposition: form-data; name=\"files\"; filename=\"" + file1Name + "\"" + "\r\n" +
                "\r\n" +
                "new first file contents\r\n" +
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
        try (var stream = object.getFile(file1Name).getStream()) {
            Assertions.assertEquals("new first file contents", new String(stream.readAllBytes()));
        }
        Assertions.assertEquals("updating multiple files & so on", object.getVersionInfo().getMessage());
        user = object.getVersionInfo().getUser();
        Assertions.assertEquals("someoneelse", user.getName());
        Assertions.assertEquals("someoneelse@school.edu", user.getAddress());
    }

    @Test
    public void testCreatedParam() throws Exception {
        //missing created value
        var uri = URI.create("http://localhost:8000/" + objectId + "/files?created=&message=adding&userName=someone&userAddress=someone%40school.edu");
        var multipartData = "--" + boundary + "\r\n" +
                paramsContentDisposition + "\r\n" +
                "\r\n" +
                "{}" + "\r\n" +
                "--" + boundary + "\r\n" +
                file1ContentDisposition + "\r\n" +
                "\r\n" +
                "asdf\r\n" +
                "--" + boundary + "--";
        var request = HttpRequest.newBuilder(uri)
                .header("Content-Type", contentTypeHeader)
                .POST(HttpRequest.BodyPublishers.ofString(multipartData)).build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(400, response.statusCode());

        //invalid created value
        uri = URI.create("http://localhost:8000/" + objectId + "/files?created=asdf&message=adding&userName=someone&userAddress=someone%40school.edu");
        request = HttpRequest.newBuilder(uri)
                .header("Content-Type", contentTypeHeader)
                .POST(HttpRequest.BodyPublishers.ofString(multipartData)).build();
        response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(400, response.statusCode());

        //valid created values
        uri = URI.create("http://localhost:8000/" + objectId + "/files?created=2021-01-15T13:05:20Z&message=adding&userName=someone&userAddress=someone%40school.edu");
        request = HttpRequest.newBuilder(uri)
                .header("Content-Type", contentTypeHeader)
                .POST(HttpRequest.BodyPublishers.ofString(multipartData)).build();
        response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(201, response.statusCode());
        var object = ocflHttp.repo.getObject(ObjectVersionId.head(objectId));
        var versionInfo = object.getVersionInfo();
        Assertions.assertEquals("2021-01-15T13:05:20Z", versionInfo.getCreated().format(DateTimeFormatter.ISO_DATE_TIME));

        //now put a change
        uri = URI.create("http://localhost:8000/" + objectId + "/files?created=2021-01-31T21:56:13.012345Z&updateExisting=true&message=updating%20multiple%20files%20%26%20so%20on&userName=someoneelse&userAddress=someoneelse%40school.edu");
        var newMultipartData = "--" + boundary + "\r\n" +
                paramsContentDisposition + "\r\n" +
                "\r\n" +
                "{}" + "\r\n" +
                "--" + boundary + "\r\n" +
                file1ContentDisposition + "\r\n" +
                "\r\n" +
                "new first file contents\r\n" +
                "--" + boundary + "--";
        request = HttpRequest.newBuilder(uri)
                .header("Content-Type", contentTypeHeader)
                .PUT(HttpRequest.BodyPublishers.ofString(newMultipartData)).build();
        response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(201, response.statusCode());
        var objectV2 = ocflHttp.repo.getObject(ObjectVersionId.version(objectId, 2));
        versionInfo = objectV2.getVersionInfo();
        Assertions.assertEquals("2021-01-31T21:56:13.012345Z", versionInfo.getCreated().format(DateTimeFormatter.ISO_DATE_TIME));
    }

    @Test
    public void testPostTwoIdenticalFiles() throws Exception {
        var uri = URI.create("http://localhost:8000/" + objectId + "/files?message=adding%20multiple%20files&userName=someone&userAddress=someone%40school.edu");
        var multipartData = "--" + boundary + "\r\n" +
                paramsContentDisposition + "\r\n" +
                "\r\n" +
                "{}" + "\r\n" +
                "--" + boundary + "\r\n" +
                file1ContentDisposition + "\r\n" +
                "\r\n" +
                "asdf\r\n" +
                "--" + boundary + "\r\n" +
                file2ContentDisposition + "\r\n" +
                "\r\n" +
                "asdf\r\n" +
                "--" + boundary + "--";
        var request = HttpRequest.newBuilder(uri)
                .header("Content-Type", contentTypeHeader)
                .POST(HttpRequest.BodyPublishers.ofString(multipartData)).build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(201, response.statusCode());
        var object = ocflHttp.repo.getObject(ObjectVersionId.head(objectId));
        var files = object.getFiles();
        Assertions.assertEquals(2, files.size());
        var file1 = object.getFile(file1Name);
        var file2 = object.getFile("file2.txt");
        Assertions.assertEquals(file1.getStorageRelativePath(), file2.getStorageRelativePath());
    }

    @Test
    public void testUnicodeNormalizationNFCInput() throws Exception {
        var uri = URI.create("http://localhost:8000/" + encodedObjectIdNFC + "/files");
        var multipartData = "--" + boundary + "\r\n" +
                paramsContentDisposition + "\r\n" +
                "\r\n" +
                "{}" + "\r\n" +
                "--" + boundary + "\r\n" +
                file1ContentDisposition + "\r\n" +
                "\r\n" +
                "asdf\r\n" +
                "--" + boundary + "--";
        var request = HttpRequest.newBuilder(uri)
                .header("Content-Type", contentTypeHeader)
                .POST(HttpRequest.BodyPublishers.ofString(multipartData)).build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(201, response.statusCode());
        uri = URI.create("http://localhost:8000/" + encodedObjectIdNFC + "/files/" + encodedFile1NameNFC + "/content");
        request = HttpRequest.newBuilder(uri).GET().build();
        response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(200, response.statusCode());
        Assertions.assertEquals("asdf", response.body());
        uri = URI.create("http://localhost:8000/" + encodedObjectId + "/files/" + encodedFile1NameNFD + "/content");
        request = HttpRequest.newBuilder(uri).GET().build();
        response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(200, response.statusCode());
        Assertions.assertEquals("asdf", response.body());
        uri = URI.create("http://localhost:8000/" + encodedObjectIdNFD + "/files/" + encodedFile1NameNFD + "/content");
        request = HttpRequest.newBuilder(uri).GET().build();
        response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(200, response.statusCode());
        Assertions.assertEquals("asdf", response.body());
    }

    @Test
    public void testUnicodeNormalizationNFDInput() throws Exception {
        var uri = URI.create("http://localhost:8000/" + encodedObjectIdNFD + "/files");
        var multipartData = "--" + boundary + "\r\n" +
                paramsContentDisposition + "\r\n" +
                "\r\n" +
                "{}" + "\r\n" +
                "--" + boundary + "\r\n" +
                "Content-Disposition: form-data; name=\"files\"; filename=\"" + file1NameNFD + "\"\r\n" +
                "\r\n" +
                "asdf\r\n" +
                "--" + boundary + "--";
        var request = HttpRequest.newBuilder(uri)
                .header("Content-Type", contentTypeHeader)
                .POST(HttpRequest.BodyPublishers.ofString(multipartData)).build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(201, response.statusCode());
        uri = URI.create("http://localhost:8000/" + encodedObjectIdNFC + "/files/" + encodedFile1NameNFC + "/content");
        request = HttpRequest.newBuilder(uri).GET().build();
        response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(200, response.statusCode());
        Assertions.assertEquals("asdf", response.body());
    }

    @Test
    public void testLocationMultipleFilesPostAndPut() throws Exception {
        var file1Contents = "... contents of first file ...";
        var file1Sha512 = "6407d5ecc067dad1a2a3c75d088ecdab97d4df5a580a3bbc1b190ad988cea529b92eab11131fd2f5c0b40fa5891eec979e7e5e96b6bed38e6dddde7a20722345";
        var file2Contents = "content";
        var file2Sha512 = "b2d1d285b5199c85f988d03649c37e44fd3dde01e5d69c50fef90651962f48110e9340b60d49a479c4c0b53f5f07d690686dd87d2481937a512e8b85ee7c617f";
        var file2Name = "filë2.txt";
        var file2Path = Path.of(workDir.toString(), file2Name);
        Files.write(file2Path, file2Contents.getBytes(StandardCharsets.UTF_8));
        var file2URI = file2Path.toUri();

        var uri = URI.create("http://localhost:8000/" + encodedObjectId + "/files?message=adding%20multiple%20files&userName=someone&userAddress=someone%40school.edu");
        var multipartData = "--" + boundary + "\r\n" +
                paramsContentDisposition + "\r\n" +
                "\r\n" +
                "{\"" + file1Name + "\": {\"checksum\": \"" + file1Sha512 + "\", \"checksumType\": \"SHA-512\"}, \"" + file2Name + "\": {\"location\": \"" + file2URI + "\", \"checksum\": \"" + file2Sha512 + "\", \"checksumType\": \"SHA-512\"}}" + "\r\n" +
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
        Assertions.assertEquals("", response.body());
        Assertions.assertEquals(201, response.statusCode());
        var object = ocflHttp.repo.getObject(ObjectVersionId.head(objectId));
        try (var stream = object.getFile(file2Name).getStream()) {
            Assertions.assertEquals(file2Contents, new String(stream.readAllBytes()));
        }
        try (var stream = object.getFile(file1Name).getStream()) {
            Assertions.assertEquals(file1Contents, new String(stream.readAllBytes()));
        }
        Assertions.assertEquals(file2Sha512, object.getFile(file2Name).getFixity().get(DigestAlgorithm.sha512));
        Assertions.assertEquals(file1Sha512, object.getFile(file1Name).getFixity().get(DigestAlgorithm.sha512));

        //now PUT files
        uri = URI.create("http://localhost:8000/" + encodedObjectId + "/files?updateExisting=true&message=adding%20multiple%20files&userName=someone&userAddress=someone%40school.edu");
        var newFile2Contents = "new file2 contents updated";
        var newFile1Contents = "new first file contents updated";
        Files.write(file2Path, newFile2Contents.getBytes(StandardCharsets.UTF_8));
        var newMultipartData = "--" + boundary + "\r\n" +
                paramsContentDisposition + "\r\n" +
                "\r\n" +
                "{\"file2.txt\": {\"location\": \"" + file2URI + "\"}}" + "\r\n" +
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
        try (var stream = object.getFile(file1Name).getStream()) {
            Assertions.assertEquals(newFile1Contents, new String(stream.readAllBytes()));
        }
        try (var stream = object.getFile("file2.txt").getStream()) {
            Assertions.assertEquals(newFile2Contents, new String(stream.readAllBytes()));
        }
    }

    @Test
    public void testLargerFile() throws Exception {
        var jettyDir = workDir.resolve("jetty");
        Files.createDirectories(jettyDir);
        try (Stream stream = Files.list(jettyDir)) {
            var numFiles = stream.count();
            Assertions.assertEquals(0, numFiles);
        }
        var file1Contents = "contents ...".repeat(100);
        var uri = URI.create("http://localhost:8000/" + encodedObjectId + "/files?message=adding%20multiple%20files&userName=someone&userAddress=someone%40school.edu");
        var multipartData = "--" + boundary + "\r\n" +
                paramsContentDisposition + "\r\n" +
                "\r\n" +
                "{}\r\n" +
                "--" + boundary + "\r\n" +
                file1ContentDisposition + "\r\n" +
                "\r\n" +
                file1Contents + "\r\n" +
                "--" + boundary + "--";
        var request = HttpRequest.newBuilder(uri)
                .header("Content-Type", contentTypeHeader)
                .POST(HttpRequest.BodyPublishers.ofString(multipartData)).build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals("", response.body());
        Assertions.assertEquals(201, response.statusCode());
        //the larger file should have been written to disk - make sure it was deleted
        try (Stream stream = Files.list(jettyDir)) {
            var numFiles = stream.count();
            //NOTE: this is a bit unstable - sometimes the file hasn't been deleted yet, so the assertion fails
            Assertions.assertEquals(0, numFiles);
        }
    }

    @Test
    public void testRenameObjectDoesntExist() throws Exception {
        var uri = URI.create("http://localhost:8000/" + encodedObjectId + "/files?message=adding%20multiple%20files&userName=someone&userAddress=someone%40school.edu");
        var multipartData = "--" + boundary + "\r\n" +
                renameContentDisposition + "\r\n" +
                "\r\n" +
                "{\"old\": \"file1.txt\", \"new\": \"file2.doc\"}" + "\r\n" +
                "--" + boundary + "--";
        var request = HttpRequest.newBuilder(uri)
                .header("Content-Type", contentTypeHeader)
                .PUT(HttpRequest.BodyPublishers.ofString(multipartData)).build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(objectId + " doesn't exist", response.body());
        Assertions.assertEquals(404, response.statusCode());
    }

    @Test
    public void testRenameFileDoesntExist() throws Exception {
        ocflHttp.repo.updateObject(ObjectVersionId.head(objectId), new VersionInfo(), updater -> {
            updater.writeFile(new ByteArrayInputStream("asdf".getBytes(StandardCharsets.UTF_8)), "random_file.txt");
        });
        var uri = URI.create("http://localhost:8000/" + encodedObjectId + "/files?message=adding%20multiple%20files&userName=someone&userAddress=someone%40school.edu");
        var multipartData = "--" + boundary + "\r\n" +
                renameContentDisposition + "\r\n" +
                "\r\n" +
                "{\"old\": \"file1.txt\", \"new\": \"file2.doc\"}" + "\r\n" +
                "--" + boundary + "--";
        var request = HttpRequest.newBuilder(uri)
                .header("Content-Type", contentTypeHeader)
                .PUT(HttpRequest.BodyPublishers.ofString(multipartData)).build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals("file1.txt doesn't exist", response.body());
        Assertions.assertEquals(404, response.statusCode());
    }

    @Test
    public void testRenameNewFileAlreadyExists() throws Exception {
        ocflHttp.repo.updateObject(ObjectVersionId.head(objectId), new VersionInfo(), updater -> {
            updater.writeFile(new ByteArrayInputStream("asdf".getBytes(StandardCharsets.UTF_8)), "file1.txt");
            updater.writeFile(new ByteArrayInputStream("asdf".getBytes(StandardCharsets.UTF_8)), "file2.doc");
        });
        var uri = URI.create("http://localhost:8000/" + encodedObjectId + "/files?message=adding%20multiple%20files&userName=someone&userAddress=someone%40school.edu");
        var multipartData = "--" + boundary + "\r\n" +
                renameContentDisposition + "\r\n" +
                "\r\n" +
                "{\"old\": \"file1.txt\", \"new\": \"file2.doc\"}" + "\r\n" +
                "--" + boundary + "--";
        var request = HttpRequest.newBuilder(uri)
                .header("Content-Type", contentTypeHeader)
                .PUT(HttpRequest.BodyPublishers.ofString(multipartData)).build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals("file2.doc already exists", response.body());
        Assertions.assertEquals(409, response.statusCode());
    }

    @Test
    public void testRename() throws Exception {
        ocflHttp.repo.updateObject(ObjectVersionId.head(objectId), new VersionInfo(), updater -> {
            updater.writeFile(new ByteArrayInputStream("asdf".getBytes(StandardCharsets.UTF_8)), "file1.txt");
        });
        var uri = URI.create("http://localhost:8000/" + encodedObjectId + "/files?message=renaming%20file1.txt&userName=someone&userAddress=someone%40school.edu");
        var multipartData = "--" + boundary + "\r\n" +
                renameContentDisposition + "\r\n" +
                "\r\n" +
                "{\"old\": \"file1.txt\", \"new\": \"file2.doc\"}" + "\r\n" +
                "--" + boundary + "--";
        var request = HttpRequest.newBuilder(uri)
                .header("Content-Type", contentTypeHeader)
                .PUT(HttpRequest.BodyPublishers.ofString(multipartData)).build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(204, response.statusCode());
        var object = ocflHttp.repo.getObject(ObjectVersionId.head(objectId));
        var files = object.getFiles();
        Assertions.assertEquals(1, files.size());
        var file2doc = object.getFile("file2.doc");
        try (var stream = file2doc.getStream()) {
            Assertions.assertEquals("asdf", new String(stream.readAllBytes()));
        }
        //verify that this new file points to the old file1.txt on disk, since the contents are the same
        Assertions.assertTrue(file2doc.getStorageRelativePath().endsWith("v1/content/file1.txt"));
        var message = object.getVersionInfo().getMessage();
        Assertions.assertEquals("renaming file1.txt", message);
        var user = object.getVersionInfo().getUser();
        Assertions.assertEquals("someone", user.getName());
        Assertions.assertEquals("someone@school.edu", user.getAddress());
    }
}