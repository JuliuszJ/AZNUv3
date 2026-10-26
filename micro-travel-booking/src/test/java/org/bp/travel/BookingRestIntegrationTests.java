package org.bp.travel;

import java.math.BigDecimal;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

import org.apache.camel.CamelContext;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.model.Model;
import org.apache.camel.model.ProcessorDefinition;
import org.apache.camel.model.ToDefinition;
import org.apache.camel.spring.boot.CamelContextConfiguration;
import org.bp.travel.model.BookTravelRequest;
import org.bp.travel.model.BookingInfo;
import org.bp.travel.model.ExceptionResponse;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import tools.jackson.databind.json.JsonMapper;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(BookingRestIntegrationTests.IsolatedTransports.class)
class BookingRestIntegrationTests {
    @LocalServerPort
    private int port;

    @Autowired
    private CamelContext camelContext;

    @Autowired
    private ProducerTemplate producer;

    private final HttpClient http = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(5)).build();
    private final JsonMapper json = JsonMapper.builder().build();

    @Test
    void createsBookingThenReadsItOverHttpIncludingPaymentUpdate() throws Exception {
        String requestBody = """
            {"person":{"name":"Anna","email":"anna@example.com"},
             "hotel":{"country":"USA","checkIn":"2026-11-01T14:00:00Z"},
             "flight":{"from":{"airport":"JFK","date":"2026-11-01T10:00:00Z"}}}
            """;
        MockEndpoint requests = camelContext.getEndpoint("mock:kafka:TravelReqTopic", MockEndpoint.class);
        requests.reset();

        HttpResponse<String> post = http.send(HttpRequest.newBuilder(uri("/travel/booking"))
            .timeout(Duration.ofSeconds(10))
            .header("Accept", "application/json")
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(requestBody)).build(), HttpResponse.BodyHandlers.ofString());

        assertJsonResponse(post, 202);
        BookingInfo created = json.readValue(post.body(), BookingInfo.class);
        assertNotNull(UUID.fromString(created.getId()));
        assertEquals(BookingInfo.StatusEnum.PENDING, created.getStatus());
        assertEquals(1, requests.getReceivedCounter());
        var published = requests.getExchanges().get(0).getMessage();
        assertEquals(created.getId(), published.getHeader("bookingTravelId"));
        assertEquals(json.readValue(requestBody, BookTravelRequest.class),
            json.readValue(published.getBody(String.class), BookTravelRequest.class));

        HttpResponse<String> get = get("/travel/booking/" + created.getId());
        assertJsonResponse(get, 200);
        assertEquals(created, json.readValue(get.body(), BookingInfo.class));

        BookingInfo completed = new BookingInfo().id(created.getId())
            .status(BookingInfo.StatusEnum.DONE).cost(new BigDecimal("1699"));
        producer.sendBodyAndHeader("direct:test-gatewayPaymentResult", json.writeValueAsString(completed),
            "bookingTravelId", created.getId());

        HttpResponse<String> updated = get("/travel/booking/" + created.getId());
        assertJsonResponse(updated, 200);
        assertEquals(completed, json.readValue(updated.body(), BookingInfo.class));
    }

    @Test
    void returns404AndJsonErrorOverHttpForUnknownBooking() throws Exception {
        String id = UUID.randomUUID().toString();
        HttpResponse<String> response = get("/travel/booking/" + id);

        assertJsonResponse(response, 404);
        ExceptionResponse error = json.readValue(response.body(), ExceptionResponse.class);
        assertEquals("Booking not found: " + id, error.getMessage(), response.body());
        assertNotNull(error.getTimestamp());
    }

    private HttpResponse<String> get(String path) throws Exception {
        return http.send(HttpRequest.newBuilder(uri(path)).timeout(Duration.ofSeconds(10))
            .header("Accept", "application/json").GET().build(), HttpResponse.BodyHandlers.ofString());
    }

    private URI uri(String path) {
        return URI.create("http://localhost:" + port + path);
    }

    private void assertJsonResponse(HttpResponse<String> response, int status) {
        assertEquals(status, response.statusCode(), response.body());
        assertTrue(response.headers().firstValue("Content-Type").orElse("")
            .startsWith("application/json"), response.headers().toString());
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class IsolatedTransports {
        @Bean
        CamelContextConfiguration replaceKafkaAndConsoleEndpoints() {
            return new CamelContextConfiguration() {
                @Override
                public void beforeApplicationStart(CamelContext context) {
                    Model model = context.getCamelContextExtension().getContextPlugin(Model.class);
                    for (var route : List.copyOf(model.getRouteDefinitions())) {
                        if (route.getInput().getEndpointUri().startsWith("kafka:")) {
                            route.getInput().setUri("direct:test-" + route.getId());
                        }
                        replaceOutputs(route);
                    }
                }

                @Override
                public void afterApplicationStart(CamelContext context) {
                }
            };
        }

        private void replaceOutputs(ProcessorDefinition<?> node) {
            for (var output : node.getOutputs()) {
                if (output instanceof ToDefinition to
                        && (to.getUri().startsWith("kafka:") || to.getUri().startsWith("stream:"))) {
                    to.setUri("mock:" + to.getUri().split("\\?", 2)[0]);
                }
                replaceOutputs(output);
            }
        }
    }
}
