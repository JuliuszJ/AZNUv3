package org.bp.travel;

import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import com.sun.net.httpserver.HttpServer;
import org.apache.camel.ProducerTemplate;
import org.bp.payment.Amount;
import org.bp.payment.PaymentRequest;
import org.bp.payment.PaymentResponse;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import tools.jackson.databind.json.JsonMapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
class PaymentRouteTests {
    private static HttpServer server;
    private static final AtomicReference<String> requestJson = new AtomicReference<>();
    private static final AtomicReference<String> requestMethod = new AtomicReference<>();
    private static final AtomicReference<String> contentType = new AtomicReference<>();
    private static final AtomicReference<String> requestUri = new AtomicReference<>();

    @Autowired
    private ProducerTemplate template;

    @DynamicPropertySource
    static void configurePaymentService(DynamicPropertyRegistry registry) throws Exception {
        server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        server.createContext("/payment", exchange -> {
            requestJson.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            requestMethod.set(exchange.getRequestMethod());
            requestUri.set(exchange.getRequestURI().toString());
            contentType.set(exchange.getRequestHeaders().getFirst("Content-Type"));
            byte[] response = """
                    {"transactionId":73,"transactionDate":"2026-10-12T10:00:00Z"}
                    """.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, response.length);
            try (var output = exchange.getResponseBody()) {
                output.write(response);
            }
            exchange.close();
        });
        server.start();
        registry.add("travel.payment.host", () -> "http://localhost:" + server.getAddress().getPort());
    }

    @AfterAll
    static void stopPaymentService() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    void postsJsonIgnoresPreviousHttpHeadersAndReturnsTypedResponse() throws Exception {
        PaymentRequest request = new PaymentRequest().amount(
                new Amount().value(new BigDecimal("573.45")).currency("PLN"));
        PaymentResponse response = template.requestBodyAndHeaders("direct:payment", request,
                Map.of("CamelHttpMethod", "GET", "CamelHttpPath", "/wrong",
                        "CamelHttpQuery", "wrong=true", "CamelHttpUri", "http://localhost:1/wrong"),
                PaymentResponse.class);

        assertEquals("POST", requestMethod.get());
        assertEquals("/payment", requestUri.get());
        assertTrue(contentType.get().startsWith("application/json"));
        var json = JsonMapper.builder().build().readTree(requestJson.get());
        assertEquals(new BigDecimal("573.45"), json.path("amount").path("value").decimalValue());
        assertEquals("PLN", json.path("amount").path("currency").asText());
        assertEquals(73, response.getTransactionId());
        assertEquals(OffsetDateTime.parse("2026-10-12T10:00:00Z"), response.getTransactionDate());
    }
}
