package org.bp.travel;

import java.net.URI;
import java.net.ServerSocket;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.math.BigDecimal;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import tools.jackson.databind.json.JsonMapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
class SwaggerUiTests {
    private static BookingServiceStub services;
    private static final String BOOKING = """
            {"person":{"firstName":"Jan","lastName":"Kowalski"},
             "hotel":{"name":"Test Hotel","city":"Warszawa","country":"Polska","address":"Testowa 1",
                      "checkIn":"2026-11-01T14:00:00Z","checkOut":"2026-11-03T10:00:00Z"},
             "flight":{"from":{"airport":"WAW","date":"2026-11-01T08:00:00Z"},
                       "to":{"airport":"CDG","date":"2026-11-01T10:00:00Z"}},
             "paymentCard":{"name":"Jan Kowalski","number":"test-card","validTo":"12/28"}}
            """;

    @DynamicPropertySource
    static void configurePort(DynamicPropertyRegistry registry) throws Exception {
        services = new BookingServiceStub(registry);
        try (ServerSocket socket = new ServerSocket(0)) {
            int availablePort = socket.getLocalPort();
            registry.add("server.port", () -> availablePort);
            registry.add("camel.rest.port", () -> availablePort);
        }
    }

    @BeforeEach
    void resetServices() {
        services.reset();
    }

    @AfterAll
    static void stopServices() {
        services.close();
    }

    @Value("${local.server.port}")
    private int port;

    @Test
    void servesSwaggerUiAssetsAndCamelOpenApi31() throws Exception {
        try (HttpClient client = HttpClient.newHttpClient()) {
            HttpResponse<String> page = get(client, "/webjars/swagger-ui/index.html?url=/api/api-doc&validatorUrl=none");
            assertEquals(200, page.statusCode());
            assertTrue(page.body().contains("swagger-initializer.js"));
            HttpResponse<String> initializer = get(client, "/webjars/swagger-ui/swagger-initializer.js");
            assertEquals(200, initializer.statusCode());
            assertTrue(initializer.body().contains("queryConfigEnabled: true"));
            assertTrue(initializer.body().contains("/api/api-doc"));
            assertEquals(200, get(client, "/webjars/swagger-ui/swagger-ui.css").statusCode());
            assertEquals(200, get(client, "/webjars/swagger-ui/swagger-ui-bundle.js").statusCode());
            assertEquals(200, get(client, "/webjars/swagger-ui/swagger-ui-standalone-preset.js").statusCode());

            HttpResponse<String> spec = get(client, "/api/api-doc");
            assertEquals(200, spec.statusCode());
            var document = JsonMapper.builder().build().readTree(spec.body());
            assertEquals("3.1.0", document.path("openapi").asText());
            assertTrue(document.path("paths").path("/travel/booking").has("post"));
            var errorSchema = document.path("paths").path("/travel/booking").path("post")
                    .path("responses").path("500").path("content").path("application/json").path("schema");
            assertTrue(errorSchema.path("$ref").asText().endsWith("BookingErrorResponse"));
            var errorProperties = document.path("components").path("schemas")
                    .path("BookingErrorResponse").path("properties");
            assertTrue(errorProperties.has("service"));
            assertTrue(errorProperties.has("code"));
            assertTrue(errorProperties.has("message"));

            String serverUrl = document.path("servers").get(0).path("url").asText();
            assertEquals("http://localhost:" + port + "/api", serverUrl);
            HttpRequest booking = HttpRequest.newBuilder(URI.create(serverUrl + "/travel/booking"))
                    .timeout(java.time.Duration.ofSeconds(15))
                    .header("Content-Type", "application/json")
                    .header("Accept", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(BOOKING))
                    .build();
            HttpResponse<String> response = client.send(booking, HttpResponse.BodyHandlers.ofString());
            assertEquals(200, response.statusCode());
            var payment = JsonMapper.builder().build().readTree(response.body());
            assertEquals(73, payment.path("transactionId").asInt());
            assertTrue(payment.hasNonNull("transactionDate"));
            assertTrue(services.parallelBookings);
            assertTrue(services.flightXml.contains("Jan"));
            assertTrue(services.flightXml.contains("WAW"));
            assertTrue(services.flightXml.contains("CDG"));
            assertTrue(services.hotelXml.contains("Test Hotel"));
            assertEquals(1, services.payments.get());
            var sentPayment = JsonMapper.builder().build().readTree(services.paymentJson);
            assertEquals(new BigDecimal("573.45"), sentPayment.path("amount").path("value").decimalValue());
            assertEquals("PLN", sentPayment.path("amount").path("currency").asText());
            assertEquals("test-card", sentPayment.path("paymentCard").path("number").asText());
            assertEquals(0, services.flightCancellations.get());
            assertEquals(0, services.hotelCancellations.get());
        }
    }

    @Test
    void flightFailurePreventsPayment() throws Exception {
        assertBookingFailure("flight", 0);
        assertCompensation("hotel", 73);
        assertEquals(0, services.flightCancellations.get());
    }

    @Test
    void hotelFailurePreventsPayment() throws Exception {
        assertBookingFailure("hotel", 0);
        assertCompensation("flight", 42);
        assertEquals(0, services.hotelCancellations.get());
    }

    @Test
    void paymentFailureDoesNotReturnSuccess() throws Exception {
        assertBookingFailure("payment", 1);
        assertCompensation("flight", 42);
        assertCompensation("hotel", 73);
    }

    @Test
    void flightFailureCompensatesHotelThatFinishesLater() throws Exception {
        services.successfulBookingDelay = 500;
        assertBookingFailure("flight", 0);
        assertCompensation("hotel", 73);
    }

    @Test
    void hotelFailureCompensatesFlightThatFinishesLater() throws Exception {
        services.successfulBookingDelay = 500;
        assertBookingFailure("hotel", 0);
        assertCompensation("flight", 42);
    }

    @Test
    void soapFaultWithoutDetailsReturnsReasonAndStillCompensates() throws Exception {
        services.faultDetails = false;
        assertBookingFailure("flight", 0);
        assertCompensation("hotel", 73);
    }

    @Test
    void flightBusinessFaultSurvivesEmptyHotelCancellationResponse() throws Exception {
        services.emptyCancellationResponses = true;
        assertBookingFailure("flight", 0);
        assertCompensation("hotel", 73);
    }

    @Test
    void hotelBusinessFaultSurvivesEmptyFlightCancellationResponse() throws Exception {
        services.emptyCancellationResponses = true;
        assertBookingFailure("hotel", 0);
        assertCompensation("flight", 42);
    }

    @Test
    void paymentFailureCompensatesWithEmptySoapResponses() throws Exception {
        services.emptyCancellationResponses = true;
        assertBookingFailure("payment", 1);
        assertCompensation("flight", 42);
        assertCompensation("hotel", 73);
    }

    private void assertCompensation(String service, int id) throws Exception {
        assertTrue(services.awaitCancellation(service), "Missing compensation for " + service);
        assertEquals(1, service.equals("flight") ? services.flightCancellations.get() : services.hotelCancellations.get());
        var factory = javax.xml.parsers.DocumentBuilderFactory.newInstance();
        factory.setNamespaceAware(true);
        factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        String xml = service.equals("flight") ? services.flightCancellationXml : services.hotelCancellationXml;
        var document = factory.newDocumentBuilder().parse(new java.io.ByteArrayInputStream(
                xml.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        if (service.equals("flight")) {
            assertEquals(1, document.getElementsByTagNameNS("http://flight.bp.org/", "cancelBooking").getLength());
            assertEquals(Integer.toString(id), document.getElementsByTagName("arg0").item(0).getTextContent());
        } else {
            var root = (org.w3c.dom.Element) document.getElementsByTagNameNS("http://www.bp.org", "cancelBookingRequest").item(0);
            assertEquals(Integer.toString(id), root.getAttribute("bookingId"));
        }
    }

    private void assertBookingFailure(String service, int expectedPayments) throws Exception {
        services.failure = service;
        try (HttpClient client = HttpClient.newHttpClient()) {
            HttpRequest request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/api/travel/booking"))
                    .timeout(java.time.Duration.ofSeconds(15))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(BOOKING)).build();
            var response = client.send(request, HttpResponse.BodyHandlers.ofString());
            assertEquals(500, response.statusCode());
            assertTrue(response.headers().firstValue("Content-Type").orElse("").startsWith("application/json"));
            var error = JsonMapper.builder().build().readTree(response.body());
            assertEquals(service, error.path("service").asText());
            assertEquals(3, error.size());
            if (service.equals("payment")) {
                assertTrue(error.path("code").isNull());
                assertEquals("Payment service failed", error.path("message").asText());
            } else if (!services.faultDetails) {
                assertTrue(error.path("code").isNull());
                assertEquals("Booking failed", error.path("message").asText());
            } else {
                assertEquals(service.equals("flight") ? 5 : 7, error.path("code").asInt());
                assertEquals(service.equals("flight")
                        ? "Start and destination airport can not be the same"
                        : "Hotel \"Test Hotel\" is unavailable", error.path("message").asText());
            }
            assertEquals(expectedPayments, services.payments.get());
        }
    }

    private HttpResponse<String> get(HttpClient client, String path) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
                .header("Accept", "application/json")
                .GET().build();
        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }
}
