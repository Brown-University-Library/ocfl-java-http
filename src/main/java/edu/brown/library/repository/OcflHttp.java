package edu.brown.library.repository;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.Request;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import javax.json.Json;
import javax.json.JsonObject;

public class OcflHttp extends AbstractHandler {

    JsonObject getRootOutput() {
        return Json.createObjectBuilder().add("OCFL ROOT", "/tmp").build();
    }

    public void handle(String target,
                       Request baseRequest,
                       HttpServletRequest request,
                       HttpServletResponse response)
            throws IOException
    {
        var pathInfo = request.getPathInfo();
        if (pathInfo.equals("/")) {
            response.setContentType("text/html;charset=utf-8");
            response.setStatus(HttpServletResponse.SC_OK);
            baseRequest.setHandled(true);
            var output = getRootOutput();
            var writer = Json.createWriter(response.getWriter());
            writer.writeObject(output);
        }
        else {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            baseRequest.setHandled(true);
        }
    }

    public static void main(String[] args) throws Exception {
        Server server = new Server(8000);
        server.setHandler(new OcflHttp());
        server.start();
        server.join();
    }

}
