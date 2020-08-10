package edu.brown.library.repository;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.Request;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import javax.json.Json;
import javax.json.JsonObject;
import java.util.regex.Pattern;

public class OcflHttp extends AbstractHandler {

    final Pattern ObjectIdPathPattern = Pattern.compile("^/([a-zA-Z:]+)/([a-zA-Z:]+)$");
    final Pattern ObjectIdPattern = Pattern.compile("^/([a-zA-Z:]+)$");

    JsonObject getRootOutput() {
        return Json.createObjectBuilder().add("OCFL ROOT", "/tmp").build();
    }

    void handleRoot(HttpServletResponse response) throws IOException {
        response.setContentType("application/json");
        response.setStatus(HttpServletResponse.SC_OK);
        var output = getRootOutput();
        var writer = Json.createWriter(response.getWriter());
        writer.writeObject(output);
    }

    void handleObjectPath(HttpServletResponse response,
                          String objectId,
                          String path)
            throws IOException
    {
        response.setStatus(HttpServletResponse.SC_OK);
        response.getWriter().println("objectId: " + objectId + "; path: " + path);
    }

    void handleObject(HttpServletResponse response,
                      String objectId)
        throws IOException
    {
        response.setStatus(HttpServletResponse.SC_OK);
        response.getWriter().println("objectId: " + objectId);
    }

    public void handle(String target,
                       Request baseRequest,
                       HttpServletRequest request,
                       HttpServletResponse response)
            throws IOException
    {
        var pathInfo = request.getPathInfo();
        if (pathInfo.equals("/")) {
            handleRoot(response);
        }
        else {
            var matcher = ObjectIdPathPattern.matcher(pathInfo);
            if (matcher.matches()) {
                var objectId = matcher.group(1);
                var path = matcher.group(2);
                handleObjectPath(response, objectId, path);
            }
            else {
                var objectIdMatcher = ObjectIdPattern.matcher(pathInfo);
                if (objectIdMatcher.matches()) {
                    var objectId = objectIdMatcher.group(1);
                    handleObject(response, objectId);
                }
                else {
                    response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                }
            }
        }
        baseRequest.setHandled(true);
    }

    public static void main(String[] args) throws Exception {
        Server server = new Server(8000);
        server.setHandler(new OcflHttp());
        server.start();
        server.join();
    }

}
