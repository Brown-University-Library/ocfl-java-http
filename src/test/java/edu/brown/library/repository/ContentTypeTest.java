package edu.brown.library.repository;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.file.Files;

public class ContentTypeTest {

    @Test
    public void plainText() throws Exception {
        var tmpFilePath = Files.createTempFile("ocfl-testsuite", "");
        Files.write(tmpFilePath, "some string".getBytes());
        try(InputStream is = Files.newInputStream(tmpFilePath)) {
            var mimetype = OcflHttp.getContentType(is, tmpFilePath.toString());
            Assertions.assertEquals("text/plain", mimetype);
        }
        tmpFilePath.toFile().delete();
    }

    @Test
    public void dng() throws Exception {
        var tmpFilePath = Files.createTempFile("ocfl-testsuite", ".dng");
        try(InputStream is = Files.newInputStream(tmpFilePath)) {
            var mimetype = OcflHttp.getContentType(is, tmpFilePath.toString());
            Assertions.assertEquals("image/x-raw-adobe", mimetype);
        }
        tmpFilePath.toFile().delete();
    }

    @Test
    public void plainTextDngExt() throws Exception {
        var tmpFilePath = Files.createTempFile("ocfl-testsuite", ".dng");
        Files.write(tmpFilePath, "some string".getBytes());
        try(InputStream is = Files.newInputStream(tmpFilePath)) {
            var mimetype = OcflHttp.getContentType(is, tmpFilePath.toString());
            Assertions.assertEquals("text/plain", mimetype);
        }
        tmpFilePath.toFile().delete();
    }
}