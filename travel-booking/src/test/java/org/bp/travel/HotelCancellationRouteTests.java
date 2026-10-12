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
import org.bp.BookingInfo;
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
class HotelCancellationRouteTests {
    private static HttpServer server;
    private static final AtomicReference<String> requestXml = new AtomicReference<>();
    private static final AtomicInteger calls = new AtomicInteger();

    @Autowired
    private ProducerTemplate template;
    @Autowired
    private BookingIdentifierService identifiers;

    @DynamicPropertySource
    static void configureHotelService(DynamicPropertyRegistry registry) throws Exception {
        server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        server.createContext("/hotel", exchange -> {
            calls.incrementAndGet();
            requestXml.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            byte[] response = """
                    <soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
                      <soap:Body>
                        <h:cancelBookingResponse xmlns:h="http://www.bp.org" xmlns:t="http://www.bp.org/types">
                          <t:id>73</t:id><t:cost>450.00</t:cost>
                        </h:cancelBookingResponse>
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
        registry.add("travel.hotel.url", () -> "http://localhost:" + server.getAddress().getPort() + "/hotel");
    }

    @AfterAll
    static void stopServer() {
        server.stop(0);
    }

    @Test
    void cancelsHotelUsingMappedIdAndReadsSoapResponse() throws Exception {
        String travelId = identifiers.generateBookingId();
        identifiers.assignHotelBookingId(travelId, 73);
        BookingInfo response = template.requestBodyAndHeader("direct:cancelHotel", null,
                "travelBookingId", travelId, BookingInfo.class);
        assertEquals(73, response.getId());
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setNamespaceAware(true);
        factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        var document = factory.newDocumentBuilder().parse(
                new ByteArrayInputStream(requestXml.get().getBytes(StandardCharsets.UTF_8)));
        assertEquals(1, document.getElementsByTagNameNS("http://www.bp.org", "cancelBookingRequest").getLength());
        assertEquals("73", ((org.w3c.dom.Element) document.getElementsByTagNameNS(
                "http://www.bp.org", "cancelBookingRequest").item(0)).getAttribute("bookingId"));
    }

    @Test
    void skipsSoapWhenNoHotelReservationWasAssigned() {
        int before = calls.get();
        template.requestBodyAndHeader("direct:cancelHotel", null,
                "travelBookingId", identifiers.generateBookingId());
        assertEquals(before, calls.get());
    }

    @Test
    void rejectsMissingTravelBookingIdBeforeSoapCall() {
        int before = calls.get();
        var error = assertThrows(CamelExecutionException.class,
                () -> template.requestBody("direct:cancelHotel", (Object) null));
        assertInstanceOf(IllegalArgumentException.class, error.getCause());
        assertEquals(before, calls.get());
    }
}
