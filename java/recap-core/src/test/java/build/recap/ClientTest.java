package build.recap;

import junit.framework.*;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.http.HttpResponse;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

public class ClientTest extends TestCase {

    private HttpServer server;
    private String serverUrl;
    private TestHttpHandler handler;

    protected void setUp() throws IOException {
        handler = new TestHttpHandler();
        server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/test", handler);
        server.start();
        serverUrl = "http://localhost:" + server.getAddress().getPort() + "/test";
    }

    protected void tearDown() {
        server.stop(0);
    }

    public void testPost() throws Exception {
        Client client = new Client();
        Type type = new Type.Bool();
        HttpResponse<String> response = client.post(serverUrl, type);

        assertNotNull(response);
        assertEquals(200, response.statusCode());
        assertEquals("application/json", handler.getReceivedContentType());
        assertEquals("{\"type\":\"bool\"}", handler.getReceivedBody());
    }

    private static class TestHttpHandler implements HttpHandler {
        private String receivedContentType;
        private String receivedBody;

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            receivedContentType = exchange.getRequestHeaders().getFirst("Content-Type");
            receivedBody = new String(exchange.getRequestBody().readAllBytes());
            exchange.sendResponseHeaders(200, 0);
            try (OutputStream responseBody = exchange.getResponseBody()) {
                responseBody.write(new byte[0]);
            }
        }

        public String getReceivedContentType() {
            return receivedContentType;
        }

        public String getReceivedBody() {
            return receivedBody;
        }
    }
}
