package edu.brown.library.repository.ocflhttp;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class ContentTypeTest {

    @Test
    public void plainText() throws Exception {
        try(InputStream is = new ByteArrayInputStream("some string".getBytes(StandardCharsets.UTF_8))) {
            var mimetype = OcflHttp.getContentType(is, "random_file");
            Assertions.assertEquals("text/plain", mimetype);
        }
    }

    @Test
    public void dng() throws Exception {
        try(InputStream is = new ByteArrayInputStream("".getBytes(StandardCharsets.UTF_8))) {
            var mimetype = OcflHttp.getContentType(is, "file.dng");
            Assertions.assertEquals("image/x-adobe-dng", mimetype);
        }
    }

    @Test
    public void plainTextDngExt() throws Exception {
        try(InputStream is = new ByteArrayInputStream("some string".getBytes(StandardCharsets.UTF_8))) {
            var mimetype = OcflHttp.getContentType(is, "file.dng");
            Assertions.assertEquals("text/plain", mimetype);
        }
    }

    @Test
    public void emptyFileNoExt() throws Exception {
        try(InputStream is = new ByteArrayInputStream("".getBytes(StandardCharsets.UTF_8))) {
            var mimetype = OcflHttp.getContentType(is, "file");
            Assertions.assertEquals("application/octet-stream", mimetype);
        }
    }

    @Test
    public void relsExt() throws Exception {
        var RELS_EXT = "<rdf:RDF xmlns:fedora-model=\"info:fedora/fedora-system:def/model#\" xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\">\n" +
                "  <rdf:Description rdf:about=\"info:fedora/testsuite:1\">\n" +
                "    <fedora-model:hasModel rdf:resource=\"info:fedora/metadata\"/>\n" +
                "  </rdf:Description>\n" +
                "</rdf:RDF>";
        try(InputStream is = new ByteArrayInputStream(RELS_EXT.getBytes(StandardCharsets.UTF_8))) {
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
        try(InputStream is = new ByteArrayInputStream(MODS.getBytes(StandardCharsets.UTF_8))) {
            var mimetype = OcflHttp.getContentType(is, "MODS");
            Assertions.assertEquals("text/plain", mimetype);
        }
        try(InputStream is = new ByteArrayInputStream(MODS.getBytes(StandardCharsets.UTF_8))) {
            var mimetype = OcflHttp.getContentType(is, "MODS.xml");
            Assertions.assertEquals("application/xml", mimetype);
        }

        var MODS_WITH_DECLARATION = "<?xml version = \"1.0\">\n" + MODS;
        try(InputStream is = new ByteArrayInputStream(MODS_WITH_DECLARATION.getBytes(StandardCharsets.UTF_8))) {
            var mimetype = OcflHttp.getContentType(is, "MODS");
            Assertions.assertEquals("application/xml", mimetype);
        }
        try(InputStream is = new ByteArrayInputStream(MODS_WITH_DECLARATION.getBytes(StandardCharsets.UTF_8))) {
            var mimetype = OcflHttp.getContentType(is, "MODS.xml");
            Assertions.assertEquals("application/xml", mimetype);
        }
    }
}