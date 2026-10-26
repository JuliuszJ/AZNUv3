package org.bp.travel;

import java.math.BigDecimal;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.camel.CamelContext;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.model.Model;
import org.apache.camel.model.ProcessorDefinition;
import org.apache.camel.model.ToDefinition;
import org.apache.camel.spring.boot.CamelContextConfiguration;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.bp.travel.model.BookingInfo;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.test.annotation.DirtiesContext;
import tools.jackson.databind.json.JsonMapper;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

@EnabledIfSystemProperty(named = "saga.kafka", matches = "true")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(SagaKafkaIntegrationTests.RealKafkaConfiguration.class)
@DirtiesContext
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SagaKafkaIntegrationTests {
    private static final String PREFIX = "saga-it-" + UUID.randomUUID() + "-";
    private static final List<String> TOPICS = List.of("TravelReqTopic", "BookingInfoTopic",
        "TravelBookingFailTopic", "TravelPaymentResultTopic");
    private static final String BROKERS = "localhost:9092";

    @LocalServerPort
    private int port;
    @Autowired
    private CamelContext context;
    @Autowired
    private PaymentService paymentService;

    private final JsonMapper json = JsonMapper.builder().build();
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

    @Test
    void completesSagaThroughHttpAndRealKafka() throws Exception {
        BookingInfo created = book("USA", "JFK");
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            BookingInfo result = get(created.getId());
            assertEquals(BookingInfo.StatusEnum.DONE, result.getStatus());
            assertEquals(0, new BigDecimal("1699").compareTo(result.getCost()));
        });
        assertFalse(observed("bookHotelCompensationAction", created.getId()));
        assertFalse(observed("bookFlightCompensationAction", created.getId()));
    }

    @ParameterizedTest
    @CsvSource({"China,JFK,bookFlightCompensationAction", "USA,PEK,bookHotelCompensationAction", "China,PEK,"})
    void compensatesOtherServiceAfterFailureThroughRealKafka(
            String country, String airport, String compensation) throws Exception {
        BookingInfo created = book(country, airport);
        if (compensation != null) {
            await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertTrue(observed(compensation, created.getId()), "Missing compensation for " + created.getId()));
        }
        assertFalse(observed("notification", created.getId()), "Failed saga must not finalize payment");

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            BookingInfo result = get(created.getId());
            assertEquals(BookingInfo.StatusEnum.CANCELED, result.getStatus());
            assertNull(result.getCost());
        });
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            PaymentService.PaymentData retained = paymentService.getPaymentData(created.getId());
            assertNotNull(retained.bookTravelRequest);
            assertFalse(retained.isReady());
        });
    }

    private BookingInfo book(String country, String airport) throws Exception {
        String payload = """
            {"person":{"name":"Saga test","email":"saga@example.com"},
             "hotel":{"country":"%s"},"flight":{"from":{"airport":"%s"}}}
            """.formatted(country, airport);
        HttpResponse<String> response = http.send(HttpRequest.newBuilder(uri("/travel/booking"))
            .timeout(Duration.ofSeconds(20)).header("Content-Type", "application/json")
            .header("Accept", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(payload)).build(), HttpResponse.BodyHandlers.ofString());
        assertEquals(202, response.statusCode(), response.body());
        BookingInfo info = json.readValue(response.body(), BookingInfo.class);
        assertNotNull(UUID.fromString(info.getId()));
        assertEquals(BookingInfo.StatusEnum.PENDING, info.getStatus());
        return info;
    }

    private BookingInfo get(String id) throws Exception {
        HttpResponse<String> response = http.send(HttpRequest.newBuilder(uri("/travel/booking/" + id))
            .timeout(Duration.ofSeconds(5)).header("Accept", "application/json").GET().build(),
            HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode(), response.body());
        BookingInfo info = json.readValue(response.body(), BookingInfo.class);
        assertEquals(id, info.getId());
        return info;
    }

    private URI uri(String path) {
        return URI.create("http://localhost:" + port + path);
    }

    private boolean observed(String route, String id) {
        return context.getEndpoint("mock:saga-" + route, MockEndpoint.class).getExchanges().stream()
            .anyMatch(exchange -> id.equals(exchange.getMessage().getHeader("bookingTravelId", String.class)));
    }

    @AfterAll
    void cleanUpTestTopics() throws Exception {
        context.stop();
        try (Admin admin = admin()) {
            admin.deleteTopics(TOPICS.stream().map(topic -> PREFIX + topic).toList())
                .all().get(15, TimeUnit.SECONDS);
        }
    }

    private static Admin admin() {
        return Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS,
            AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "10000",
            AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000"));
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class RealKafkaConfiguration {
        @Bean
        CamelContextConfiguration configureRealKafka() {
            return new CamelContextConfiguration() {
                @Override
                public void beforeApplicationStart(CamelContext context) {
                    try (Admin admin = admin()) {
                        admin.describeCluster().clusterId().get(10, TimeUnit.SECONDS);
                        admin.createTopics(TOPICS.stream()
                            .map(topic -> new NewTopic(PREFIX + topic, 1, (short) 1)).toList())
                            .all().get(15, TimeUnit.SECONDS);
                    } catch (Exception e) {
                        throw new IllegalStateException("Test requires Kafka at " + BROKERS, e);
                    }
                    Model model = context.getCamelContextExtension().getContextPlugin(Model.class);
                    for (var route : List.copyOf(model.getRouteDefinitions())) {
                        String uri = route.getInput().getEndpointUri();
                        if (uri.startsWith("kafka:")) {
                            // Separate groups preserve pub/sub fan-out between saga participants.
                            String topic = uri.substring("kafka:".length()).split("\\?", 2)[0];
                            route.getInput().setUri("kafka:" + PREFIX + topic + "?brokers=" + BROKERS
                                + "&groupId=" + PREFIX + route.getId() + "&autoOffsetReset=earliest");
                        }
                        configureOutputs(route, route.getId());
                    }
                }

                @Override
                public void afterApplicationStart(CamelContext context) {
                }
            };
        }

        private void configureOutputs(ProcessorDefinition<?> node, String routeId) {
            for (var output : node.getOutputs()) {
                if (output instanceof ToDefinition to) {
                    if (to.getUri().startsWith("kafka:")) {
                        String topic = to.getUri().substring("kafka:".length()).split("\\?", 2)[0];
                        if (!topic.startsWith(PREFIX)) {
                            to.setUri("kafka:" + PREFIX + topic + "?brokers=" + BROKERS);
                        }
                    } else if (to.getUri().startsWith("stream:")) {
                        to.setUri("mock:saga-" + routeId);
                    }
                }
                configureOutputs(output, routeId);
            }
        }
    }
}
