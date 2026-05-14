//package config;
//
//import org.slf4j.LoggerFactory;
//import org.slf4j.MDC;
//
//import java.net.URI;
//import java.net.URLEncoder;
//import java.net.http.HttpClient;
//import java.net.http.HttpRequest;
//import java.net.http.HttpResponse;
//import java.nio.charset.StandardCharsets;
//import org.slf4j.Logger;
//
//public class GeoCode {
//    private static final Logger LOG = LoggerFactory.getLogger(GeoCode.class.getName());
//    private static final long MIN_INTERVAL_MS = 1100;
//    private long lastRequestTime = 0;
//
//    public String getCoord(String address) {
//        if (address == null) {
//            throw new IllegalArgumentException("Address cannot be null");
//        }
//
//        long elapsed = System.currentTimeMillis() - lastRequestTime;
//        if (elapsed < MIN_INTERVAL_MS) {
//            try {
//                Thread.sleep(MIN_INTERVAL_MS - elapsed);
//            } catch (InterruptedException e) {
//                Thread.currentThread().interrupt();
//                return null;
//            }
//        }
//
//        String encodedAddress = URLEncoder.encode(address, StandardCharsets.UTF_8);
//        String url = "https://nominatim.openstreetmap.org/search?q=" + encodedAddress + "&format=json&limit=1";
//
//        HttpClient client = HttpClient.newHttpClient();
//        HttpRequest request = HttpRequest.newBuilder()
//                .uri(URI.create(url))
//                .header("User-Agent", "EngineeringThesisProject/1.0")
//                .build();
//            try {
//                lastRequestTime = System.currentTimeMillis();
//                return client.send(request, HttpResponse.BodyHandlers.ofString()).body();
//            } catch (Exception e) {
//                return null;
//            }
//        }
//}
