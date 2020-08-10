package edu.brown.library.repository;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.URI;
import java.net.http.HttpResponse;

import org.eclipse.jetty.server.Server;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;


public class OcflHttpTest {

    Server server;

    @BeforeEach
    private void setupServer() throws Exception {
        server = new Server(8000);
        server.setHandler(new OcflHttp());
        server.start();
    }

    @AfterEach
    private void stopServer() throws Exception {
        server.stop();
    }

    @Test
    public void testRoot() throws Exception {
        var url = "http://localhost:8000/";
        var client = HttpClient.newHttpClient();
        var request = HttpRequest.newBuilder(URI.create(url)).build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        Assertions.assertEquals(200, response.statusCode());
        var body = response.body();
        Assertions.assertEquals("{\"OCFL ROOT\":\"/tmp\"}", body);
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