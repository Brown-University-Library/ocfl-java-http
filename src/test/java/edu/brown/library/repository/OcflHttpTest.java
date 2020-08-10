package edu.brown.library.repository;

import org.junit.jupiter.api.Test;


public class OcflHttpTest {

    @Test
    public void testRoot() {
        var handler = new OcflHttp();
        var output = handler.getRootOutput();
        assert output.toString().equals("{\"OCFL ROOT\":\"/tmp\"}");
    }
}