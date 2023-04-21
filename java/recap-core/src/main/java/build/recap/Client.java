package build.recap;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import javax.json.bind.Jsonb;
import javax.json.bind.JsonbBuilder;

public class Client {
    private HttpClient httpClient;

    public Client() {
        this(HttpClient.newHttpClient());
    }

    public Client(HttpClient httpClient) {
        this.httpClient = httpClient;
    }

    public HttpResponse<String> post(String url, Type type) throws Exception {
        URI uri = URI.create(url);
        HttpRequest request = HttpRequest.newBuilder()
                .uri(uri)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(toJson(type)))
                .build();

        return httpClient.send(request, BodyHandlers.ofString());
    }

    private String toJson(Type type) throws Exception {
        try (Jsonb jsonb = JsonbBuilder.create()) {
            return jsonb.toJson(type.toTypeDescription());
        }
    }
}
