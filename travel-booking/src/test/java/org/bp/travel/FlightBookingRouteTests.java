package org.bp.travel;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicReference;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.ByteArrayInputStream;

import com.sun.net.httpserver.HttpServer;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.RouteBuilder;
import org.bp.flight.BookFligthResponse;
import org.bp.flight.Person;
import org.bp.travel.model.TravelBookingRequest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
class FlightBookingRouteTests {
    private static HttpServer server;
    private static final AtomicReference<String> requestXml = new AtomicReference<>();

    @Autowired
    private ProducerTemplate template;
    @Autowired
    private BookingIdentifierService identifiers;

    @DynamicPropertySource
    static void configureFlightService(DynamicPropertyRegistry registry) throws Exception {
        server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        server.createContext("/flight", exchange -> {
            requestXml.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            byte[] response = """
                    <soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
                      <soap:Body>
                        <f:bookFligthResponse xmlns:f="http://flight.bp.org/">
                          <return><cost>123.45</cost><id>42</id></return>
                        </f:bookFligthResponse>
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
    static void stopFlightService() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    void sendsSoapRequestAndUnmarshalsResponseAfterWritingToStdout() throws Exception {
        TravelBookingRequest request = new TravelBookingRequest();
        Person person = new Person();
        person.setFirstName("Jan");
        person.setLastName("Kowalski");
        request.setPerson(person);

        String travelId = identifiers.generateBookingId();
        template.getCamelContext().addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                from("direct:testBookFlight").saga()
                        .setProperty("travelBookingId", constant(travelId))
                        .to("direct:bookFlight");
            }
        });
        BookFligthResponse response = template.requestBody("direct:testBookFlight", request, BookFligthResponse.class);
        assertEquals(42, response.getReturn().getId());
        assertEquals(42, identifiers.getFlightBookingId(travelId));
        assertEquals(new java.math.BigDecimal("123.45"), response.getReturn().getCost());

        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setNamespaceAware(true);
        factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        var document = factory.newDocumentBuilder().parse(
                new ByteArrayInputStream(requestXml.get().getBytes(StandardCharsets.UTF_8)));
        assertEquals(1, document.getElementsByTagNameNS("http://flight.bp.org/", "bookFligth").getLength());
        assertEquals("Jan", document.getElementsByTagName("firstName").item(0).getTextContent());
    }
}
