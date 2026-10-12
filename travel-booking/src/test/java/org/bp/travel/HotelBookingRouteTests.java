package org.bp.travel;

import java.io.ByteArrayInputStream;
import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicReference;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.parsers.DocumentBuilderFactory;

import com.sun.net.httpserver.HttpServer;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.RouteBuilder;
import org.bp.BookingInfo;
import org.bp.Hotel;
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
class HotelBookingRouteTests {
    private static HttpServer server;
    private static final AtomicReference<String> requestXml = new AtomicReference<>();

    @Autowired
    private ProducerTemplate template;
    @Autowired
    private BookingIdentifierService identifiers;

    @DynamicPropertySource
    static void configureHotelService(DynamicPropertyRegistry registry) throws Exception {
        server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        server.createContext("/hotel", exchange -> {
            requestXml.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            byte[] response = """
                    <soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
                      <soap:Body>
                        <h:bookRoomResponse xmlns:h="http://www.bp.org" xmlns:t="http://www.bp.org/types">
                          <t:id>73</t:id><t:cost>450.00</t:cost>
                        </h:bookRoomResponse>
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
    static void stopHotelService() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    void sendsHotelAndPersonInSoapAndUnmarshalsBookingInfo() throws Exception {
        TravelBookingRequest request = new TravelBookingRequest();
        Person person = new Person();
        person.setFirstName("Jan");
        person.setLastName("Kowalski");
        request.setPerson(person);
        Hotel hotel = new Hotel();
        hotel.setName("Test Hotel");
        hotel.setCity("Warszawa");
        hotel.setCountry("Polska");
        hotel.setAddress("Testowa 1");
        var dates = DatatypeFactory.newInstance();
        hotel.setCheckIn(dates.newXMLGregorianCalendar("2026-11-01T14:00:00Z"));
        hotel.setCheckOut(dates.newXMLGregorianCalendar("2026-11-03T10:00:00Z"));
        request.setHotel(hotel);

        String travelId = identifiers.generateBookingId();
        template.getCamelContext().addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                from("direct:testBookHotel").saga()
                        .setProperty("travelBookingId", constant(travelId))
                        .to("direct:bookHotel");
            }
        });
        BookingInfo response = template.requestBody("direct:testBookHotel", request, BookingInfo.class);
        assertEquals(73, response.getId());
        assertEquals(73, identifiers.getHotelBookingId(travelId));
        assertEquals(new BigDecimal("450.00"), response.getCost());

        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setNamespaceAware(true);
        factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        var document = factory.newDocumentBuilder().parse(
                new ByteArrayInputStream(requestXml.get().getBytes(StandardCharsets.UTF_8)));
        assertEquals(1, document.getElementsByTagNameNS("http://www.bp.org", "bookRoomRequest").getLength());
        assertEquals("Jan", document.getElementsByTagNameNS("http://www.bp.org/types", "firstName").item(0).getTextContent());
        assertEquals("Test Hotel", document.getElementsByTagNameNS("http://www.bp.org/types", "name").item(0).getTextContent());
    }
}
