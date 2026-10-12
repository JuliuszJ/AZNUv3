package org.bp.travel;

import java.io.ByteArrayInputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.xml.parsers.DocumentBuilderFactory;
import com.sun.net.httpserver.HttpServer;
import org.apache.camel.CamelExecutionException;
import org.apache.camel.ProducerTemplate;
import org.bp.flight.CancelBookingResponse;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SpringBootTest
class FlightCancellationRouteTests {
    private static HttpServer server;
    private static final AtomicReference<String> requestXml = new AtomicReference<>();
    private static final AtomicInteger calls = new AtomicInteger();

    @Autowired
    private ProducerTemplate template;
    @Autowired
    private BookingIdentifierService identifiers;

    @DynamicPropertySource
    static void configureFlightService(DynamicPropertyRegistry registry) throws Exception {
        server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        server.createContext("/flight", exchange -> {
            calls.incrementAndGet();
            requestXml.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            byte[] response = """
                    <soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
                      <soap:Body>
                        <f:cancelBookingResponse xmlns:f="http://flight.bp.org/">
                          <return><id>42</id><cost>123.45</cost></return>
                        </f:cancelBookingResponse>
                      </soap:Body>
                    </soap:Envelope>
                    """.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "text/xml; charset=UTF-8");
            exchange.sendResponseHeaders(200, response.length);
            try (var output = exchange.getResponseBody()) {
                output.write(response);
            }
            exchange.close();
        });
        server.start();
        registry.add("travel.flight.url", () -> "http://localhost:" + server.getAddress().getPort() + "/flight");
    }

    @AfterAll
    static void stopServer() {
        server.stop(0);
    }

    @Test
    void cancelsFlightUsingMappedIdAndReadsSoapResponse() throws Exception {
        String travelId = identifiers.generateBookingId();
        identifiers.assignFlightBookingId(travelId, 42);
        CancelBookingResponse response = template.requestBodyAndHeader("direct:cancelFlight", null,
                "travelBookingId", travelId, CancelBookingResponse.class);
        assertEquals(42, response.getReturn().getId());
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setNamespaceAware(true);
        factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        var document = factory.newDocumentBuilder().parse(
                new ByteArrayInputStream(requestXml.get().getBytes(StandardCharsets.UTF_8)));
        assertEquals(1, document.getElementsByTagNameNS("http://flight.bp.org/", "cancelBooking").getLength());
        assertEquals("42", document.getElementsByTagName("arg0").item(0).getTextContent());
    }

    @Test
    void skipsSoapWhenNoFlightReservationWasAssigned() {
        int before = calls.get();
        template.requestBodyAndHeader("direct:cancelFlight", null,
                "travelBookingId", identifiers.generateBookingId());
        assertEquals(before, calls.get());
    }

    @Test
    void rejectsMissingTravelBookingIdBeforeSoapCall() {
        int before = calls.get();
        var error = assertThrows(CamelExecutionException.class,
                () -> template.requestBody("direct:cancelFlight", (Object) null));
        assertInstanceOf(IllegalArgumentException.class, error.getCause());
        assertEquals(before, calls.get());
    }
}
