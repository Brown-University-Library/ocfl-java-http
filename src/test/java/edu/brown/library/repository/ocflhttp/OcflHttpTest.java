package edu.brown.library.repository.ocflhttp;

import java.io.ByteArrayInputStream;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.URI;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.OffsetDateTime;
import java.util.ArrayList;

import edu.wisc.library.ocfl.api.model.ObjectVersionId;
import edu.wisc.library.ocfl.api.model.VersionInfo;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;

import javax.json.Json;
import javax.json.JsonObject;


public class OcflHttpTest {

    Server server;
    OcflHttp ocflHttp;
    String tmpDir = System.getProperty("java.io.tmpdir");
    Path tmpRoot;
    Path workDir;
    HttpClient client;
    String objectId = "testsuite:nâtiôn";
    String encodedObjectId = URLEncoder.encode(objectId, StandardCharsets.UTF_8);
    String fileName = "nâtiôn.txt";
    String encodedFileName = URLEncoder.encode(fileName, StandardCharsets.UTF_8);

    @BeforeEach
    private void setup() throws Exception {
        tmpRoot = Files.createTempDirectory("ocfl-java-http-tests");
        workDir = Files.createTempDirectory("ocfl-java-http-tests-work");
        final ArrayList<Path> uploadDirs = new ArrayList<>();
        uploadDirs.add(Path.of(tmpDir));
        ocflHttp = new OcflHttp(tmpRoot, workDir, OcflHttpConfig.DEFAULT_FILE_SIZE_THRESHOLD, uploadDirs);
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
        final var rootDirStr = tmpRoot.toString().replace("\\", "\\\\");
        final var tmpDirStr = tmpDir.toString().replace("\\", "\\\\");
        final var expectedStr = "{\"OCFL ROOT\":\"" + rootDirStr + "\",\"ALLOWED-UPLOAD-DIRS\":[\"" + tmpDirStr + "\"]}";
        Assertions.assertEquals(body, expectedStr);

        //test unhandled/not found url
        url = "http://localhost:8000/not-found";
        request = HttpRequest.newBuilder(URI.create(url)).build();
        response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(404, response.statusCode());
    }

    @Test
    public void testVersionsObjectNotFound() throws Exception {
        var url = "http://localhost:8000/testsuite:notfound/versions";
        var request = HttpRequest.newBuilder(URI.create(url)).build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals("testsuite:notfound not found", response.body());
        Assertions.assertEquals(404, response.statusCode());
    }

    @Test
    public void testVersionsWrongMethod() throws Exception {
        var url = "http://localhost:8000/testsuite:notfound/versions";
        var request = HttpRequest.newBuilder(URI.create(url)).POST(HttpRequest.BodyPublishers.ofString("")).build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(405, response.statusCode());
    }

    @Test
    public void testVersions() throws Exception {
        ocflHttp.repo.updateObject(ObjectVersionId.head(objectId), new VersionInfo(), updater -> {
            updater.writeFile(new ByteArrayInputStream("data".getBytes(StandardCharsets.UTF_8)),"file1");
        });
        ocflHttp.repo.updateObject(ObjectVersionId.head(objectId), new VersionInfo(), updater -> {
            updater.removeFile("file1");
        });
        var url = "http://localhost:8000/" + encodedObjectId + "/versions";
        var request = HttpRequest.newBuilder(URI.create(url)).build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(200, response.statusCode());
        JsonObject responseJson = Json.createReader(new ByteArrayInputStream(response.body().getBytes(StandardCharsets.UTF_8))).readObject();
        Assertions.assertTrue(responseJson.getJsonObject("v1").getString("created").endsWith("Z"));
        Assertions.assertTrue(responseJson.getJsonObject("v2").getString("created").endsWith("Z"));
    }

    @Test
    public void testGetFilesObjectNotFound() throws Exception {
        //now test object that doesn't exist
        var url = "http://localhost:8000/testsuite:notfound/files";
        var request = HttpRequest.newBuilder(URI.create(url)).build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(404, response.statusCode());
        Assertions.assertEquals("testsuite:notfound not found", response.body());
    }

    @Test
    public void testGetFilesObjectDeleted() throws Exception {
        //an object is deleted if all the files have been removed in latest version
        ocflHttp.repo.updateObject(ObjectVersionId.head(objectId), new VersionInfo(), updater -> {
            updater.writeFile(new ByteArrayInputStream("data".getBytes(StandardCharsets.UTF_8)),"file1");
        });
        ocflHttp.repo.updateObject(ObjectVersionId.head(objectId), new VersionInfo(), updater -> {
            updater.removeFile("file1");
        });
        var url = "http://localhost:8000/" + encodedObjectId + "/files";
        var request = HttpRequest.newBuilder(URI.create(url)).build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(410, response.statusCode());
        Assertions.assertEquals("object " + objectId + " deleted", response.body());
    }

    @Test
    public void testGetFiles() throws Exception {
        ocflHttp.repo.updateObject(ObjectVersionId.head(objectId), new VersionInfo(), updater -> {
                updater.writeFile(new ByteArrayInputStream("data".getBytes(StandardCharsets.UTF_8)),"file1");
        });
        var url = "http://localhost:8000/" + encodedObjectId + "/files";
        var request = HttpRequest.newBuilder(URI.create(url)).build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(200, response.statusCode());
        Assertions.assertEquals("bytes", response.headers().firstValue("Accept-Ranges").get());
        JsonObject responseJson = Json.createReader(new ByteArrayInputStream(response.body().getBytes(StandardCharsets.UTF_8))).readObject();
        Assertions.assertEquals("v1", responseJson.getString("version"));
        var filesJson = responseJson.getJsonObject("files");
        Assertions.assertEquals("{}", filesJson.getJsonObject("file1").toString());
        var objectJson = responseJson.getJsonObject(("object"));
        Assertions.assertNull(objectJson);
    }

    @Test
    public void testGetVersionFilesNoObject() throws Exception {
        var url = "http://localhost:8000/" + encodedObjectId + "/v1/files";
        var request = HttpRequest.newBuilder(URI.create(url)).build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(404, response.statusCode());
    }

    @Test
    public void testGetVersionFiles() throws Exception {
        ocflHttp.repo.updateObject(ObjectVersionId.head(objectId), new VersionInfo(), updater -> {
            updater.writeFile(new ByteArrayInputStream("data".getBytes(StandardCharsets.UTF_8)),"file1");
            updater.writeFile(new ByteArrayInputStream("file2 data".getBytes(StandardCharsets.UTF_8)),"file2");
        });
        ocflHttp.repo.updateObject(ObjectVersionId.head(objectId), new VersionInfo(), updater -> {
                    updater.removeFile("file1");
        });
        var url = "http://localhost:8000/" + encodedObjectId + "/v1/files";
        var request = HttpRequest.newBuilder(URI.create(url)).build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(200, response.statusCode());
        JsonObject responseJson = Json.createReader(new ByteArrayInputStream(response.body().getBytes(StandardCharsets.UTF_8))).readObject();
        Assertions.assertEquals("v1", responseJson.getString("version"));
        var filesJson = responseJson.getJsonObject("files");
        Assertions.assertEquals("{}", filesJson.getJsonObject("file1").toString());
        Assertions.assertEquals("{}", filesJson.getJsonObject("file2").toString());
        url = "http://localhost:8000/" + encodedObjectId + "/v2/files";
        request = HttpRequest.newBuilder(URI.create(url)).build();
        response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(200, response.statusCode());
        responseJson = Json.createReader(new ByteArrayInputStream(response.body().getBytes(StandardCharsets.UTF_8))).readObject();
        filesJson = responseJson.getJsonObject("files");
        Assertions.assertEquals("{}", filesJson.getJsonObject("file2").toString());
        Assertions.assertNull(filesJson.getJsonObject("file1"));
        url = "http://localhost:8000/" + encodedObjectId + "/v3/files";
        request = HttpRequest.newBuilder(URI.create(url)).build();
        response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(404, response.statusCode());
    }

    @Test
    public void testGetFilesFieldsAndObjectTimestamps() throws Exception {
        ocflHttp.repo.updateObject(ObjectVersionId.head(objectId), new VersionInfo(), updater -> {
            updater.writeFile(new ByteArrayInputStream("data".getBytes(StandardCharsets.UTF_8)), "file1");
        });
        var url = "http://localhost:8000/" + encodedObjectId + "/files?fields=state,size&objectTimestamps=true";
        var request = HttpRequest.newBuilder(URI.create(url)).build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(200, response.statusCode());
        JsonObject responseJson = Json.createReader(new ByteArrayInputStream(response.body().getBytes(StandardCharsets.UTF_8))).readObject();
        var filesJson = responseJson.getJsonObject("files");
        Assertions.assertEquals("A", filesJson.getJsonObject("file1").getString("state"));
        Assertions.assertEquals(4, filesJson.getJsonObject("file1").getInt("size"));
        var objectJson = responseJson.getJsonObject("object");
        Assertions.assertTrue(objectJson.getString("created").endsWith("Z"));
        Assertions.assertTrue(objectJson.getString("lastModified").endsWith("Z"));
    }

    @Test
    public void testGetVersionFilesFields() throws Exception {
        var file2Sha512 = "8fe4e3693f1e8090f279a969eec378086334b32b7457bfe1d16e6a3daa7ce60c26cc2e69c03af3d8b2b266844bf47f8ff7e0d6c70e1b90b6647f45dfc3c5f1ce";
        ocflHttp.repo.updateObject(ObjectVersionId.head(objectId), new VersionInfo(), updater -> {
            updater.writeFile(new ByteArrayInputStream("data".getBytes(StandardCharsets.UTF_8)),"file1");
            updater.writeFile(new ByteArrayInputStream("file2 data".getBytes(StandardCharsets.UTF_8)),"file2");
        });
        ocflHttp.repo.updateObject(ObjectVersionId.head(objectId), new VersionInfo(), updater -> {
            updater.removeFile("file1");
        });
        var url = "http://localhost:8000/" + encodedObjectId + "/v1/files?fields=state,size,checksum,lastModified";
        var request = HttpRequest.newBuilder(URI.create(url)).build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(200, response.statusCode());
        JsonObject responseJson = Json.createReader(new ByteArrayInputStream(response.body().getBytes(StandardCharsets.UTF_8))).readObject();
        var filesJson = responseJson.getJsonObject("files");
        Assertions.assertEquals("A", filesJson.getJsonObject("file1").getString("state"));
        Assertions.assertEquals(10, filesJson.getJsonObject("file2").getInt("size"));
        Assertions.assertEquals(file2Sha512, filesJson.getJsonObject("file2").getString("checksum"));
    }

    @Test
    public void testGetAllFiles() throws Exception {
        //add file1
        ocflHttp.repo.updateObject(ObjectVersionId.head(objectId), new VersionInfo(), updater -> {
            updater.writeFile(new ByteArrayInputStream("data".getBytes(StandardCharsets.UTF_8)), "file1");
        });
        //now delete file1 & add file2
        ocflHttp.repo.updateObject(ObjectVersionId.head(objectId), new VersionInfo(), updater -> {
            updater.removeFile("file1");
            updater.writeFile(new ByteArrayInputStream("file2 data".getBytes(StandardCharsets.UTF_8)), "file2");
        });
        //url w/ no param should just return active/not-deleted files
        var url = "http://localhost:8000/" + encodedObjectId + "/files";
        var request = HttpRequest.newBuilder(URI.create(url)).build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(200, response.statusCode());
        var responseJson = Json.createReader(new ByteArrayInputStream(response.body().getBytes(StandardCharsets.UTF_8))).readObject();
        var filesJson = responseJson.getJsonObject("files");
        Assertions.assertEquals("{}", filesJson.getJsonObject("file2").toString());

        //now include deleted files
        url = "http://localhost:8000/" + encodedObjectId + "/files?" + OcflHttp.IncludeDeletedParameter + "=true";
        request = HttpRequest.newBuilder(URI.create(url)).build();
        response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(200, response.statusCode());
        responseJson = Json.createReader(new ByteArrayInputStream(response.body().getBytes(StandardCharsets.UTF_8))).readObject();
        filesJson = responseJson.getJsonObject("files");
        Assertions.assertTrue(filesJson.containsKey("file1"));
        Assertions.assertTrue(filesJson.containsKey("file2"));
    }

    @Test
    public void testGetAllFilesFields() throws Exception {
        //add file1
        ocflHttp.repo.updateObject(ObjectVersionId.head(objectId), new VersionInfo(), updater -> {
            updater.writeFile(new ByteArrayInputStream("data".getBytes(StandardCharsets.UTF_8)), "file1");
        });
        //now delete file1 & add file2
        ocflHttp.repo.updateObject(ObjectVersionId.head(objectId), new VersionInfo(), updater -> {
            updater.removeFile("file1");
            updater.writeFile(new ByteArrayInputStream("file2 data".getBytes(StandardCharsets.UTF_8)), "file2");
        });
        var url = "http://localhost:8000/" + encodedObjectId + "/files?" + OcflHttp.IncludeDeletedParameter + "=true&fields=state,size,mimetype,checksum,lastModified";
        var request = HttpRequest.newBuilder(URI.create(url)).build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(200, response.statusCode());
        JsonObject responseJson = Json.createReader(new ByteArrayInputStream(response.body().getBytes(StandardCharsets.UTF_8))).readObject();
        var filesJson = responseJson.getJsonObject("files");
        Assertions.assertEquals("D", filesJson.getJsonObject("file1").getString("state"));
        Assertions.assertEquals(4, filesJson.getJsonObject("file1").getInt("size"));
        Assertions.assertEquals("text/plain", filesJson.getJsonObject("file1").getString("mimetype"));
        Assertions.assertEquals("77c7ce9a5d86bb386d443bb96390faa120633158699c8844c30b13ab0bf92760b7e4416aea397db91b4ac0e5dd56b8ef7e4b066162ab1fdc088319ce6defc876",
                filesJson.getJsonObject("file1").getString("checksum"));
        Assertions.assertEquals("SHA-512", filesJson.getJsonObject("file1").getString("checksumType"));
        Assertions.assertTrue(filesJson.getJsonObject("file1").getString("lastModified").endsWith("Z"));
        Assertions.assertEquals("A", filesJson.getJsonObject("file2").getString("state"));
        Assertions.assertEquals(10, filesJson.getJsonObject("file2").getInt("size"));
        Assertions.assertEquals("text/plain", filesJson.getJsonObject("file2").getString("mimetype"));
        Assertions.assertEquals("8fe4e3693f1e8090f279a969eec378086334b32b7457bfe1d16e6a3daa7ce60c26cc2e69c03af3d8b2b266844bf47f8ff7e0d6c70e1b90b6647f45dfc3c5f1ce",
                filesJson.getJsonObject("file2").getString("checksum"));
        Assertions.assertEquals("SHA-512", filesJson.getJsonObject("file2").getString("checksumType"));
        Assertions.assertTrue(filesJson.getJsonObject("file2").getString("lastModified").endsWith("Z"));
    }

    @Test
    public void testDeleteObject() throws Exception {
        ocflHttp.repo.updateObject(ObjectVersionId.head(objectId), new VersionInfo(), updater -> {
            updater.writeFile(new ByteArrayInputStream("data".getBytes(StandardCharsets.UTF_8)), "file1");
        });
        var files = ocflHttp.repo.getObject(ObjectVersionId.head(objectId)).getFiles();
        Assertions.assertFalse(files.isEmpty());
        var url = "http://localhost:8000/" + encodedObjectId + "/files";
        var request = HttpRequest.newBuilder(URI.create(url)).DELETE().build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(204, response.statusCode());
        files = ocflHttp.repo.getObject(ObjectVersionId.head(objectId)).getFiles();
        Assertions.assertTrue(files.isEmpty());
    }

    @Test
    public void testDeleteObjectNotFound() throws Exception {
        var url = "http://localhost:8000/" + encodedObjectId + "/files";
        var request = HttpRequest.newBuilder(URI.create(url)).DELETE().build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(404, response.statusCode());
    }

    @Test
    public void testDeleteObjectAlreadyDeleted() throws Exception {
        ocflHttp.repo.updateObject(ObjectVersionId.head(objectId), new VersionInfo(), updater -> {
            updater.writeFile(new ByteArrayInputStream("data".getBytes(StandardCharsets.UTF_8)), "file1");
        });
        ocflHttp.repo.updateObject(ObjectVersionId.head(objectId), new VersionInfo(), updater -> {
            updater.removeFile("file1");
        });
        var url = "http://localhost:8000/" + encodedObjectId + "/files";
        var request = HttpRequest.newBuilder(URI.create(url)).DELETE().build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(204, response.statusCode());
        var files = ocflHttp.repo.getObject(ObjectVersionId.head(objectId)).getFiles();
        Assertions.assertTrue(files.isEmpty());
    }

    @Test
    public void testGetFileContent() throws Exception {
        //test non-existent object
        var uri = URI.create("http://localhost:8000/" + objectId + "/files/file1/content");
        var request = HttpRequest.newBuilder(uri).GET().build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(404, response.statusCode());
        Assertions.assertEquals(objectId + " not found", response.body());
        //now test object exists, but missing file
        var fileContents = "data";
        ocflHttp.repo.updateObject(ObjectVersionId.head(objectId), new VersionInfo(), updater -> {
                    updater.writeFile(new ByteArrayInputStream(fileContents.getBytes(StandardCharsets.UTF_8)), fileName);
                });
        response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(404, response.statusCode());
        Assertions.assertEquals(objectId + "/file1 not found", response.body());
        //now test success
        uri = URI.create("http://localhost:8000/" + encodedObjectId + "/files/" + encodedFileName + "/content");
        request = HttpRequest.newBuilder(uri).GET().build();
        response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(200, response.statusCode());
        Assertions.assertEquals("bytes", response.headers().firstValue("Accept-Ranges").get());
        Assertions.assertEquals("4", response.headers().firstValue("Content-Length").get());
        Assertions.assertEquals("text/plain", response.headers().firstValue("Content-Type").get());
        Assertions.assertEquals("attachment; filename*=UTF-8''" + encodedFileName, response.headers().firstValue("Content-Disposition").get());
        var lastModifiedHeader = response.headers().firstValue("Last-Modified").get();
        Assertions.assertTrue(lastModifiedHeader.endsWith(" GMT"));
        var fileETag = "\"77c7ce9a5d86bb386d443bb96390faa120633158699c8844c30b13ab0bf92760b7e4416aea397db91b4ac0e5dd56b8ef7e4b066162ab1fdc088319ce6defc876\"";
        Assertions.assertEquals(fileETag, response.headers().firstValue("ETag").get());
        Assertions.assertEquals(fileContents, response.body());

        //test handling if ETag hasn't changed
        request = HttpRequest.newBuilder(uri).header(OcflHttp.IfNoneMatchHeader, fileETag).GET().build();
        response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(304, response.statusCode());
        Assertions.assertEquals("", response.body());

        //test handling if ETag has changed
        request = HttpRequest.newBuilder(uri).header(OcflHttp.IfNoneMatchHeader, "asdf").GET().build();
        response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(200, response.statusCode());
        Assertions.assertEquals(fileETag, response.headers().firstValue("ETag").get());
        Assertions.assertEquals(fileContents, response.body());

        //test If-Modified-Since
        request = HttpRequest.newBuilder(uri).header(OcflHttp.IfModifiedSinceHeader, lastModifiedHeader).GET().build();
        response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(304, response.statusCode());
        Assertions.assertEquals("", response.body());
        var lastModifiedMinus3Seconds = OffsetDateTime.parse(lastModifiedHeader, OcflHttp.IfModifiedFormatter).minusSeconds(3);
        request = HttpRequest.newBuilder(uri).header(OcflHttp.IfModifiedSinceHeader, lastModifiedMinus3Seconds.format(OcflHttp.IfModifiedFormatter)).GET().build();
        response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(200, response.statusCode());
        Assertions.assertEquals(fileContents, response.body());
        var lastModifiedPlus3Seconds = OffsetDateTime.parse(lastModifiedHeader, OcflHttp.IfModifiedFormatter).plusSeconds(3);
        request = HttpRequest.newBuilder(uri).header(OcflHttp.IfModifiedSinceHeader, lastModifiedPlus3Seconds.format(OcflHttp.IfModifiedFormatter)).GET().build();
        response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(304, response.statusCode());
        Assertions.assertEquals("", response.body());

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
        ocflHttp.repo.updateObject(ObjectVersionId.head(objectId), new VersionInfo(), updater -> {
                updater.writeFile(new ByteArrayInputStream(contents.getBytes(StandardCharsets.UTF_8)),"biggerfile");
                });
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
    public void testGetFileContentWrongMethod() throws Exception {
        var url = "http://localhost:8000/" + encodedObjectId + "/files/file1/content";
        var request = HttpRequest.newBuilder(URI.create(url)).POST(HttpRequest.BodyPublishers.ofString("")).build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(405, response.statusCode());
    }

    @Test
    public void testGetFileContentObjectDeleted() throws Exception {
        ocflHttp.repo.updateObject(ObjectVersionId.head(objectId), new VersionInfo(), updater -> {
            updater.writeFile(new ByteArrayInputStream("data".getBytes(StandardCharsets.UTF_8)),"file1");
        });
        ocflHttp.repo.updateObject(ObjectVersionId.head(objectId), new VersionInfo(), updater -> {
            updater.removeFile("file1");
        });
        var url = "http://localhost:8000/" + encodedObjectId + "/files/file1/content";
        var request = HttpRequest.newBuilder(URI.create(url)).build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(410, response.statusCode());
        Assertions.assertEquals("object " + objectId + " deleted", response.body());
    }

    @Test
    public void testGetFileContentFileDeleted() throws Exception {
        ocflHttp.repo.updateObject(ObjectVersionId.head(objectId), new VersionInfo(), updater -> {
            updater.writeFile(new ByteArrayInputStream("data".getBytes(StandardCharsets.UTF_8)),"file1");
            updater.writeFile(new ByteArrayInputStream("data".getBytes(StandardCharsets.UTF_8)),"file2");
        });
        ocflHttp.repo.updateObject(ObjectVersionId.head(objectId), new VersionInfo(), updater -> {
            updater.removeFile("file1");
        });
        var url = "http://localhost:8000/" + encodedObjectId + "/files/file1/content";
        var request = HttpRequest.newBuilder(URI.create(url)).build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(410, response.statusCode());
        Assertions.assertEquals("file file1 deleted", response.body());
    }

    @Test
    public void testGetFileContentsFromPreviousVersion() throws Exception {
        //v1
        ocflHttp.repo.updateObject(ObjectVersionId.head(objectId), new VersionInfo(), updater -> {
            updater.writeFile(new ByteArrayInputStream("asdf".getBytes(StandardCharsets.UTF_8)), "file1.txt");
        });
        //v2
        ocflHttp.repo.updateObject(ObjectVersionId.head(objectId), new VersionInfo(), updater -> {
            updater.writeFile(new ByteArrayInputStream("original data".getBytes(StandardCharsets.UTF_8)), fileName);
        });
        //v3
        ocflHttp.repo.updateObject(ObjectVersionId.head(objectId), new VersionInfo(), updater -> {
            updater.removeFile(fileName);
        });
        //v4
        ocflHttp.repo.updateObject(ObjectVersionId.head(objectId), new VersionInfo(), updater -> {
            updater.writeFile(new ByteArrayInputStream("updated".getBytes(StandardCharsets.UTF_8)), fileName);
        });
        //wrong object id should return 404
        var url = "http://localhost:8000/random/v1/files/" + encodedFileName + "/content";
        var request = HttpRequest.newBuilder(URI.create(url)).build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(404, response.statusCode());
        //v1 should return 404
        url = "http://localhost:8000/" + encodedObjectId + "/v1/files/" + encodedFileName + "/content";
        request = HttpRequest.newBuilder(URI.create(url)).build();
        response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(404, response.statusCode());
        //v2 should return "original data"
        url = "http://localhost:8000/" + encodedObjectId + "/v2/files/" + encodedFileName + "/content";
        request = HttpRequest.newBuilder(URI.create(url)).build();
        response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(200, response.statusCode());
        Assertions.assertEquals("original data", response.body());
        //v3 should return 404
        url = "http://localhost:8000/" + encodedObjectId + "/v3/files/" + encodedFileName + "/content";
        request = HttpRequest.newBuilder(URI.create(url)).build();
        response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(404, response.statusCode());
        //v4 should return "updated"
        url = "http://localhost:8000/" + encodedObjectId + "/v4/files/" + encodedFileName + "/content";
        request = HttpRequest.newBuilder(URI.create(url)).build();
        response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(200, response.statusCode());
        Assertions.assertEquals("updated", response.body());
        //v5 should return 404
        url = "http://localhost:8000/" + encodedObjectId + "/v5/files/" + encodedFileName + "/content";
        request = HttpRequest.newBuilder(URI.create(url)).build();
        response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(404, response.statusCode());
        Assertions.assertEquals("version v5 not found", response.body());
        //POST should return 405
        request = HttpRequest.newBuilder(URI.create(url)).POST(HttpRequest.BodyPublishers.ofString("")).build();
        response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(405, response.statusCode());
    }

    @Test
    public void testDeleteFile() throws Exception {
        ocflHttp.repo.updateObject(ObjectVersionId.head(objectId), new VersionInfo(), updater -> {
            updater.writeFile(new ByteArrayInputStream("data".getBytes(StandardCharsets.UTF_8)),"file1");
        });
        var url = "http://localhost:8000/" + encodedObjectId + "/files/file1";
        var request = HttpRequest.newBuilder(URI.create(url)).DELETE().build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(204, response.statusCode());
        var object = ocflHttp.repo.getObject(ObjectVersionId.head(objectId));
        var files = object.getFiles();
        Assertions.assertTrue(files.isEmpty());
    }

    @Test
    public void testDeleteFileAlreadyDeleted() throws Exception {
        ocflHttp.repo.updateObject(ObjectVersionId.head(objectId), new VersionInfo(), updater -> {
            updater.writeFile(new ByteArrayInputStream("data".getBytes(StandardCharsets.UTF_8)),"file1");
            updater.writeFile(new ByteArrayInputStream("data".getBytes(StandardCharsets.UTF_8)),"file2");
        });
        ocflHttp.repo.updateObject(ObjectVersionId.head(objectId), new VersionInfo(), updater -> {
            updater.removeFile("file1");
        });
        var url = "http://localhost:8000/" + encodedObjectId + "/files/file1";
        var request = HttpRequest.newBuilder(URI.create(url)).DELETE().build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(204, response.statusCode());
        var object = ocflHttp.repo.getObject(ObjectVersionId.head(objectId));
        var files = object.getFiles();
        Assertions.assertEquals(1, files.size());
        Assertions.assertEquals("v2", object.getVersionNum().toString());
    }

    @Test
    public void testDeleteFileDoesntExist() throws Exception {
        ocflHttp.repo.updateObject(ObjectVersionId.head(objectId), new VersionInfo(), updater -> {
            updater.writeFile(new ByteArrayInputStream("data".getBytes(StandardCharsets.UTF_8)),"file1");
        });
        var url = "http://localhost:8000/" + encodedObjectId + "/files/zzzzz";
        var request = HttpRequest.newBuilder(URI.create(url)).DELETE().build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(404, response.statusCode());
    }

    @Test
    public void testDeleteFileObjectDoesntExist() throws Exception {
        var url = "http://localhost:8000/" + encodedObjectId + "/files/zzzzz";
        var request = HttpRequest.newBuilder(URI.create(url)).DELETE().build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(404, response.statusCode());
    }
}