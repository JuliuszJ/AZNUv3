package org.bp.travel;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.camel.Exchange;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.AdviceWith;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.model.ProcessorDefinition;
import org.apache.camel.model.ToDefinition;
import org.bp.travel.model.BookTravelRequest;
import org.bp.travel.model.BookingInfo;
import org.bp.travel.model.ExceptionResponse;
import org.bp.travel.state.StateService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tools.jackson.databind.json.JsonMapper;

import static org.junit.jupiter.api.Assertions.*;

class LegacyBookingFlowTests {
    private final JsonMapper json = JsonMapper.builder().build();
    private DefaultCamelContext context;
    private ProducerTemplate producer;
    private BookingInfoService bookingInfoService;
    private PaymentService paymentService;

    @BeforeEach
    void startRoutesWithoutExternalServices() throws Exception {
        ApplicationConfig config = new ApplicationConfig();
        paymentService = new PaymentService();
        paymentService.init();
        bookingInfoService = new BookingInfoService();
        context = new DefaultCamelContext();
        context.addRoutes(new MicroTravelBookingService(
            new BookingIdentifierService(), paymentService,
            new StateService(config.basicStateMachineBuilder()),
            new StateService(config.basicStateMachineBuilder()), bookingInfoService));

        // These tests exercise the processing routes, not REST transport.
        context.getRestDefinitions().clear();
        context.getRouteDefinitions().removeIf(route -> route.getInput().getEndpointUri().startsWith("rest"));
        for (var route : List.copyOf(context.getRouteDefinitions())) {
            replaceExternalOutputs(route);
            String id = route.getId();
            boolean kafkaConsumer = route.getInput().getEndpointUri().startsWith("kafka:");
            AdviceWith.adviceWith(context, id, false, advice -> {
                if (kafkaConsumer) {
                    advice.replaceFromWith("direct:test-" + id);
                }
                if (id.equals("notification") || id.endsWith("CompensationAction")) {
                    advice.weaveAddLast().to("mock:" + id);
                }
            });
        }
        context.start();
        producer = context.createProducerTemplate();
    }

    private void replaceExternalOutputs(ProcessorDefinition<?> node) {
        for (var output : node.getOutputs()) {
            if (output instanceof ToDefinition to
                    && (to.getUri().startsWith("kafka:") || to.getUri().startsWith("stream:"))) {
                to.setUri("mock:" + to.getUri().split("\\?", 2)[0]);
            }
            replaceExternalOutputs(output);
        }
    }

    @AfterEach
    void stopRoutes() throws Exception {
        if (producer != null) {
            producer.stop();
        }
        if (context != null) {
            context.stop();
        }
    }

    @Test
    void gatewayPublishesOriginalRequestWithIdentifierAndReturnsBookingInfo() {
        BookTravelRequest request = json.readValue(request("USA", "JFK"), BookTravelRequest.class);
        Exchange response = send("direct:bookTravel", request, Map.of());
        BookingInfo info = response.getMessage().getBody(BookingInfo.class);
        assertNotNull(info);
        assertNotNull(UUID.fromString(info.getId()));
        assertEquals(BookingInfo.StatusEnum.PENDING, info.getStatus());
        assertEquals(202, response.getMessage().getHeader(Exchange.HTTP_RESPONSE_CODE, Integer.class));
        assertEquals(info, bookingInfoService.findById(info.getId()));
        Exchange published = only("kafka:TravelReqTopic");
        assertEquals(info.getId(), published.getMessage().getHeader("bookingTravelId"));
        assertEquals(request, json.readValue(published.getMessage().getBody(String.class), BookTravelRequest.class));
    }

    @Test
    void gatewayKeepsDifferentBookingsUnderTheirOwnIdentifiers() {
        BookTravelRequest request = json.readValue(request("USA", "JFK"), BookTravelRequest.class);
        BookingInfo first = send("direct:bookTravel", request, Map.of()).getMessage().getBody(BookingInfo.class);
        BookingInfo second = send("direct:bookTravel", request, Map.of()).getMessage().getBody(BookingInfo.class);

        assertNotEquals(first.getId(), second.getId());
        assertEquals(first, bookingInfoService.findById(first.getId()));
        assertEquals(second, bookingInfoService.findById(second.getId()));
        assertNull(bookingInfoService.findById("unknown-booking"));
    }

    @ParameterizedTest
    @CsvSource({"USA,JFK,1699,true", "Poland,WAW,1488,true", "USA,JFK,1699,false", "Poland,WAW,1488,false"})
    void finalizesPaymentOnlyAfterBothBookingsAndRequestArrive(
            String country, String airport, String cost, boolean requestFirst) {
        String id = UUID.randomUUID().toString();
        String request = request(country, airport);
        bookingInfoService.save(new BookingInfo().id(id).status(BookingInfo.StatusEnum.PENDING));
        if (requestFirst) {
            consume("paymentTravelReq", request, id);
        }
        consume("bookHotel", request, id);
        deliver(bookingMessages().get(0), "paymentBookingInfo");
        assertCount("notification", 0);
        consume("bookFlight", request, id);
        deliver(bookingMessages().get(1), "paymentBookingInfo");
        if (!requestFirst) {
            assertCount("notification", 0);
            consume("paymentTravelReq", request, id);
        }
        BookingInfo result = only("notification").getMessage().getBody(BookingInfo.class);
        assertEquals(id, result.getId());
        assertEquals(0, new BigDecimal(cost).compareTo(result.getCost()));
        assertEquals(BookingInfo.StatusEnum.DONE, result.getStatus());
        Exchange paymentResult = only("kafka:TravelPaymentResultTopic");
        assertEquals(id, paymentResult.getMessage().getHeader("bookingTravelId"));
        assertEquals(BookingInfo.StatusEnum.PENDING, bookingInfoService.findById(id).getStatus());
        deliver(paymentResult, "gatewayPaymentResult");
        BookingInfo stored = bookingInfoService.findById(id);
        assertEquals(id, stored.getId());
        assertEquals(BookingInfo.StatusEnum.DONE, stored.getStatus());
        assertEquals(0, new BigDecimal(cost).compareTo(stored.getCost()));
        assertCount("kafka:TravelBookingFailTopic", 0);
        assertCount("bookHotelCompensationAction", 0);
        assertCount("bookFlightCompensationAction", 0);
    }

    @Test
    void pendingIsStoredBeforeRequestIsPublished() {
        List<BookingInfo> storedAtPublication = new ArrayList<>();
        endpoint("kafka:TravelReqTopic").whenAnyExchangeReceived(exchange -> storedAtPublication.add(
            bookingInfoService.findById(exchange.getMessage().getHeader("bookingTravelId", String.class))));
        BookTravelRequest request = json.readValue(request("USA", "JFK"), BookTravelRequest.class);

        BookingInfo info = send("direct:bookTravel", request, Map.of()).getMessage().getBody(BookingInfo.class);

        assertEquals(List.of(info), storedAtPublication);
    }

    @Test
    void fastPaymentResultIsNotOverwrittenByBookingResponse() {
        BookTravelRequest request = json.readValue(request("USA", "JFK"), BookTravelRequest.class);
        BookingInfo[] completed = new BookingInfo[1];
        // Deliver the saga result while the gateway is still handling the HTTP request.
        endpoint("kafka:TravelReqTopic").whenAnyExchangeReceived(exchange -> {
            String id = exchange.getMessage().getHeader("bookingTravelId", String.class);
            completed[0] = new BookingInfo().id(id).cost(new BigDecimal("1699"))
                .status(BookingInfo.StatusEnum.DONE);
            consume("gatewayPaymentResult", json.writeValueAsString(completed[0]), id);
        });

        BookingInfo response = send("direct:bookTravel", request, Map.of()).getMessage().getBody(BookingInfo.class);

        assertEquals(BookingInfo.StatusEnum.PENDING, response.getStatus());
        assertEquals(completed[0], bookingInfoService.findById(response.getId()));
    }

    @Test
    void cancellationSurvivesDuplicateEventsAndLateDoneOrPending() {
        String id = UUID.randomUUID().toString();
        BookingInfo canceled = new BookingInfo().id(id).status(BookingInfo.StatusEnum.CANCELED);
        consume("gatewayPaymentResult", json.writeValueAsString(canceled), id);
        consume("gatewayPaymentResult", json.writeValueAsString(canceled), id);
        consume("gatewayPaymentResult", json.writeValueAsString(new BookingInfo().id(id)
            .status(BookingInfo.StatusEnum.DONE).cost(new BigDecimal("1699"))), id);
        send("direct:bookRequester", null, Map.of("bookingTravelId", id));

        assertEquals(canceled, bookingInfoService.findById(id));
    }

    @ParameterizedTest
    @CsvSource({"PENDING", "DONE", "CANCELED"})
    void retrievesStoredBookingWithItsCurrentStatus(String status) {
        String id = UUID.randomUUID().toString();
        BookingInfo stored = new BookingInfo().id(id).status(BookingInfo.StatusEnum.valueOf(status));
        if (status.equals("DONE")) {
            stored.cost(new BigDecimal("1699"));
        }
        bookingInfoService.save(stored);

        Exchange response = send("direct:getBookingInfo", null, Map.of("id", id));

        assertEquals(200, response.getMessage().getHeader(Exchange.HTTP_RESPONSE_CODE, Integer.class));
        assertEquals(stored, response.getMessage().getBody(BookingInfo.class));
    }

    @Test
    void returnsJsonErrorForUnknownBooking() {
        String id = UUID.randomUUID().toString();
        Exchange response = send("direct:getBookingInfo", null, Map.of("id", id));

        assertEquals(404, response.getMessage().getHeader(Exchange.HTTP_RESPONSE_CODE, Integer.class));
        assertEquals("application/json", response.getMessage().getHeader(Exchange.CONTENT_TYPE));
        ExceptionResponse error = json.readValue(response.getMessage().getBody(String.class), ExceptionResponse.class);
        assertEquals("Booking not found: " + id, error.getMessage());
        assertNotNull(error.getTimestamp());
        assertNull(bookingInfoService.findById(id));
    }

    @ParameterizedTest
    @CsvSource({"hotel,China,JFK,bookFlight,bookFlightCompensation,bookFlightCompensationAction",
                "flight,USA,PEK,bookHotel,bookHotelCompensation,bookHotelCompensationAction"})
    void compensatesCompletedBookingWhenOtherServiceFails(
            String failedService, String country, String airport,
            String successfulRoute, String compensationRoute, String compensationAction) {
        String id = UUID.randomUUID().toString();
        String request = request(country, airport);
        consume("paymentTravelReq", request, id);
        consume(successfulRoute, request, id);
        deliver(bookingMessages().get(0), "paymentBookingInfo");
        consume(failedService.equals("hotel") ? "bookHotel" : "bookFlight", request, id);
        Exchange failure = assertFailure(failedService, id);
        deliver(failure, compensationRoute);
        assertCount(compensationAction, 1);
        assertEquals(id, only(compensationAction).getMessage().getHeader("bookingTravelId"));
        assertCount("notification", 0);
        assertCancellationReachesGateway(id);
        assertEquals(1, bookingMessages().size());
    }

    @ParameterizedTest
    @CsvSource({"hotel,China,JFK,bookFlight,bookFlightCompensation,bookFlightCompensationAction",
                "flight,USA,PEK,bookHotel,bookHotelCompensation,bookHotelCompensationAction"})
    void cancellationBeforeOtherBookingPreventsPublishingItsSuccess(
            String failedService, String country, String airport,
            String otherRoute, String compensationRoute, String compensationAction) {
        String id = UUID.randomUUID().toString();
        String request = request(country, airport);
        consume(failedService.equals("hotel") ? "bookHotel" : "bookFlight", request, id);
        deliver(assertFailure(failedService, id), compensationRoute);
        assertCount(compensationAction, 0);
        consume(otherRoute, request, id);
        assertCount(compensationAction, 1);
        assertEquals(id, only(compensationAction).getMessage().getHeader("bookingTravelId"));
        assertCount("kafka:BookingInfoTopic", 0);
        assertCount("notification", 0);
        assertCancellationReachesGateway(id);
    }

    @ParameterizedTest
    @CsvSource({"hotel,China,JFK,bookFlight,bookFlightCompensation",
                "flight,USA,PEK,bookHotel,bookHotelCompensation"})
    void paymentCurrentlyRetainsCanceledSagaAndFinalizesOnLateSuccess(
            String failedService, String country, String airport,
            String successfulRoute, String compensationRoute) {
        String id = UUID.randomUUID().toString();
        String request = request(country, airport);
        consume("paymentTravelReq", request, id);
        consume(successfulRoute, request, id);
        deliver(bookingMessages().get(0), "paymentBookingInfo");
        consume(failedService.equals("hotel") ? "bookHotel" : "bookFlight", request, id);
        deliver(assertFailure(failedService, id), compensationRoute);
        assertCancellationReachesGateway(id);

        PaymentService.PaymentData retained = paymentService.getPaymentData(id);
        assertNotNull(retained.bookTravelRequest);
        assertFalse(retained.isReady());
        assertCount("notification", 0);
        if (failedService.equals("hotel")) {
            assertNull(retained.hotelBookingInfo);
            assertNotNull(retained.flightBookingInfo);
        } else {
            assertNull(retained.flightBookingInfo);
            assertNotNull(retained.hotelBookingInfo);
        }

        // Characterization of the current defect: cancellation is not known to payment.
        BookingInfo lateSuccess = new BookingInfo().id("late-booking").cost(new BigDecimal("100"));
        send("direct:test-paymentBookingInfo", json.writeValueAsString(lateSuccess),
            Map.of("bookingTravelId", id, "serviceType", failedService));

        assertTrue(retained.isReady());
        assertCount("notification", 1);
        assertEquals(BookingInfo.StatusEnum.DONE,
            only("notification").getMessage().getBody(BookingInfo.class).getStatus());
        assertEquals(BookingInfo.StatusEnum.CANCELED, bookingInfoService.findById(id).getStatus());
    }

    private void assertCancellationReachesGateway(String id) {
        Exchange result = only("kafka:TravelPaymentResultTopic");
        assertEquals(id, result.getMessage().getHeader("bookingTravelId"));
        String payload = result.getMessage().getBody(String.class);
        result.getMessage().setBody(payload);
        BookingInfo canceled = json.readValue(payload, BookingInfo.class);
        assertEquals(id, canceled.getId());
        assertEquals(BookingInfo.StatusEnum.CANCELED, canceled.getStatus());
        assertNull(canceled.getCost());
        deliver(result, "gatewayPaymentResult");
        assertEquals(canceled, bookingInfoService.findById(id));
    }

    private Exchange assertFailure(String service, String id) {
        Exchange failure = only("kafka:TravelBookingFailTopic");
        assertEquals(service, failure.getMessage().getHeader("serviceType"));
        assertEquals(id, failure.getMessage().getHeader("bookingTravelId"));
        String payload = failure.getMessage().getBody(String.class);
        // Materialize a serialized stream before inspecting and delivering it again.
        failure.getMessage().setBody(payload);
        ExceptionResponse error = json.readValue(payload, ExceptionResponse.class);
        assertNotNull(error.getTimestamp());
        assertEquals(service.equals("hotel") ? "Not serviced destination: China" : "Not serviced airport: PEK",
            error.getMessage());
        return failure;
    }

    private void consume(String route, String body, String id) {
        send("direct:test-" + route, body, Map.of("bookingTravelId", id));
    }

    private void deliver(Exchange message, String route) {
        send("direct:test-" + route, message.getMessage().getBody(String.class),
            Map.copyOf(message.getMessage().getHeaders()));
    }

    private Exchange send(String uri, Object body, Map<String, Object> headers) {
        Exchange result = producer.request(uri, exchange -> {
            exchange.getMessage().setBody(body);
            exchange.getMessage().setHeaders(headers);
        });
        assertNull(result.getException(), () -> "Route failed: " + uri + ": " + result.getException());
        return result;
    }

    private List<Exchange> bookingMessages() {
        return endpoint("kafka:BookingInfoTopic").getExchanges();
    }

    private MockEndpoint endpoint(String name) {
        return context.getEndpoint("mock:" + name, MockEndpoint.class);
    }

    private Exchange only(String name) {
        assertCount(name, 1);
        return endpoint(name).getExchanges().get(0);
    }

    private void assertCount(String name, int count) {
        assertEquals(count, endpoint(name).getReceivedCounter(), "Messages at " + name);
    }

    private String request(String country, String airport) {
        return """
            {"person":{"name":"Anna","email":"anna@example.com"},
             "hotel":{"country":"%s"},"flight":{"from":{"airport":"%s"}}}
            """.formatted(country, airport);
    }
}
