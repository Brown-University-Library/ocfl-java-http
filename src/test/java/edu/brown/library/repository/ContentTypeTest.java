package edu.brown.library.repository;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
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

    @Test
    public void emptyFileNoExt() throws Exception {
        var tmpFilePath = Files.createTempFile("ocfl-testsuite", "");
        try(InputStream is = Files.newInputStream(tmpFilePath)) {
            var mimetype = OcflHttp.getContentType(is, tmpFilePath.toString());
            Assertions.assertEquals("application/octet-stream", mimetype);
        }
        tmpFilePath.toFile().delete();
    }

    @Test
    public void relsExt() throws Exception {
        var RELS_EXT = "<rdf:RDF xmlns:fedora-model=\"info:fedora/fedora-system:def/model#\" xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\">\n" +
                "  <rdf:Description rdf:about=\"info:fedora/testsuite:1\">\n" +
                "    <fedora-model:hasModel rdf:resource=\"info:fedora/metadata\"/>\n" +
                "  </rdf:Description>\n" +
                "</rdf:RDF>";
        try(InputStream is = new ByteArrayInputStream(RELS_EXT.getBytes())) {
            var mimetype = OcflHttp.getContentType(is, "RELS-EXT");
            Assertions.assertEquals("text/plain", mimetype);
        }
    }

    @Test
    public void mods() throws Exception {
        var MODS = "<mods:mods xmlns:mods=\"http://www.loc.gov/mods/v3\" xmlns:xlink=\"http://www.w3.org/1999/xlink\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"http://www.loc.gov/mods/v3 http://www.loc.gov/standards/mods/v3/mods-3-7.xsd\">\n" +
                "  <mods:titleInfo>\n" +
                "    <mods:title>test obj</mods:title>\n" +
                "  </mods:titleInfo>\n" +
                "</mods:mods>\n";
        try(InputStream is = new ByteArrayInputStream(MODS.getBytes())) {
            var mimetype = OcflHttp.getContentType(is, "MODS");
            Assertions.assertEquals("text/plain", mimetype);
        }
        try(InputStream is = new ByteArrayInputStream(MODS.getBytes())) {
            var mimetype = OcflHttp.getContentType(is, "MODS.xml");
            Assertions.assertEquals("application/xml", mimetype);
        }

        var MODS_WITH_DECLARATION = "<?xml version = \"1.0\">\n" + MODS;
        try(InputStream is = new ByteArrayInputStream(MODS_WITH_DECLARATION.getBytes())) {
            var mimetype = OcflHttp.getContentType(is, "MODS");
            //tika doesn't seem to recognize xml without the .xml extension on the filename
            // (although note that we're not using the full tika parsers - just the basic (quicker) detection
            Assertions.assertEquals("text/plain", mimetype);
        }
        try(InputStream is = new ByteArrayInputStream(MODS_WITH_DECLARATION.getBytes())) {
            var mimetype = OcflHttp.getContentType(is, "MODS.xml");
            Assertions.assertEquals("application/xml", mimetype);
        }
    }
}