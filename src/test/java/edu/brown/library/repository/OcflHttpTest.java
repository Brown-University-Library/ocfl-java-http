package edu.brown.library.repository;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.URI;
import java.net.http.HttpResponse;

import org.eclipse.jetty.server.Server;
import org.junit.jupiter.api.Test;


public class OcflHttpTest {

    @Test
    public void testRoot() throws Exception {
        var server = new Server(8000);
        server.setHandler(new OcflHttp());
        server.start();

        var url = "http://localhost:8000/";
        var client = HttpClient.newHttpClient();
        var request = HttpRequest.newBuilder(URI.create(url)).build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assert response.statusCode() == 200;
        var body = response.body();
        assert body.equals("{\"OCFL ROOT\":\"/tmp\"}");
    }
}