package org.bp.travel;

import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.converter.jaxb.JaxbDataFormat;
import org.apache.camel.http.base.HttpOperationFailedException;
import org.apache.camel.model.SagaPropagation;
import org.apache.camel.model.dataformat.JsonLibrary;
import org.apache.camel.model.rest.RestBindingMode;
import org.bp.travel.model.BookingErrorResponse;
import org.bp.travel.model.TravelBookingRequest;
import org.bp.travel.model.Utils;
import org.springframework.stereotype.Component;
import org.springframework.ws.soap.client.SoapFaultClientException;
import java.math.BigDecimal;
import static org.apache.camel.model.rest.RestParamType.body;

@Component
public class TravelBookingService extends RouteBuilder {
    private final BookingIdentifierService bookingIdentifierService;

    public TravelBookingService(
            BookingIdentifierService bookingIdentifierService) {
        this.bookingIdentifierService = bookingIdentifierService;
    }

    @Override
    public void configure() throws Exception {
        restConfiguration()
                .component("platform-http")
                .host("{{camel.rest.host}}")
                .port("{{camel.rest.port}}")
                .useXForwardHeaders(false)
                .bindingMode(RestBindingMode.json)
                .skipBindingOnErrorCode(true)
                .dataFormatProperty("prettyPrint", "true")
                .enableCORS(true)
                .contextPath("/api")
                .apiContextPath("/api-doc")
                .apiProperty("openapi.version", "3.1.0")
                .apiProperty("api.title", "Travel Booking API")
                .apiProperty("api.version", "1.0.0");

        rest("/travel").description("Travel booking REST service")
                .consumes("application/json")
                .produces("application/json")
                .post("/booking").description("Book a travel")
                .type(TravelBookingRequest.class)
                .outType(org.bp.payment.PaymentResponse.class)
                .param().name("body").type(body).description("The travel to book").endParam()
                .responseMessage().code(200).message("Travel successfully booked").endResponseMessage()
                .responseMessage().code(500).message("Travel booking failed")
                    .responseModel(BookingErrorResponse.class).endResponseMessage()
                .to("direct:travelHttp");

        from("direct:travelHttp").routeId("travelHttp")
                .doTry()
                    .to("direct:bookTravel")
                .doCatch(Exception.class)
                    .process(exchange -> {
                        Throwable failure = exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Throwable.class);
                        BookingErrorResponse response = new BookingErrorResponse(
                                "travel", null, "Travel booking failed");
                        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
                            if (cause instanceof BookingBusinessException businessFailure) {
                                response = new BookingErrorResponse(businessFailure.getService(),
                                        businessFailure.getCode(), businessFailure.getMessage());
                                break;
                            }
                            if (cause instanceof HttpOperationFailedException) {
                                response = new BookingErrorResponse("payment", null, "Payment service failed");
                            }
                        }
                        if ("travel".equals(response.service())) {
                            org.slf4j.LoggerFactory.getLogger(TravelBookingService.class)
                                    .error("Unmapped travel booking failure", failure);
                        }
                        exchange.getMessage().setBody(response);
                        exchange.getMessage().removeHeaders("Camel*");
                        exchange.getMessage().setHeader(Exchange.HTTP_RESPONSE_CODE, 500);
                        exchange.getMessage().setHeader(Exchange.CONTENT_TYPE, "application/json");
                    })
                    .marshal().json(JsonLibrary.Jackson)
                    .convertBodyTo(String.class)
                .end();

        from("direct:bookTravel").routeId("bookTravel")
                .log("bookTravel fired")
                .process(exchange -> {
                    exchange.setProperty("paymentRequest",
                            Utils.preparePaymentRequest(exchange.getMessage().getBody(TravelBookingRequest.class)));
                    exchange.setProperty("travelBookingId", bookingIdentifierService.generateBookingId());
                }
                )
                .saga()
                .multicast()
                .parallelProcessing()
                .aggregationStrategy((previous, current) -> {
                    if (current.getException() != null) {
                        return current;
                    }
                    if (previous != null && previous.getException() != null) {
                        return previous;
                    }
                    org.bp.payment.PaymentRequest paymentRequest = previous == null
                            ? current.getProperty("paymentRequest", org.bp.payment.PaymentRequest.class)
                            : previous.getMessage().getBody(org.bp.payment.PaymentRequest.class);
                    Object response = current.getMessage().getBody();
                    BigDecimal cost;
                    if (response instanceof org.bp.BookingInfo hotelBooking) {
                        cost = hotelBooking.getCost();
                    } else if (response instanceof org.bp.flight.BookFligthResponse flightBooking
                            && flightBooking.getReturn() != null) {
                        cost = flightBooking.getReturn().getCost();
                    } else {
                        throw new IllegalStateException("Unexpected booking response");
                    }
                    if (cost == null) {
                        throw new IllegalStateException("Booking response is missing cost");
                    }
                    paymentRequest.getAmount().setValue(paymentRequest.getAmount().getValue().add(cost));
                    current.getMessage().setBody(paymentRequest);
                    return current;
                })
                .to("direct:bookFlight", "direct:bookHotel")
                .end()
                .to("direct:payment")
                .removeHeaders("Camel*")
                .setHeader(Exchange.HTTP_RESPONSE_CODE, constant(200));

        final JaxbDataFormat jaxbFlight = new JaxbDataFormat(
                org.bp.flight.BookFligthResponse.class.getPackage().getName());
        from("direct:bookFlight").routeId("bookFlight")
                .log("bookFlight fired")
                .saga()
                .propagation(SagaPropagation.MANDATORY)
                .compensation("direct:cancelFlight").option("travelBookingId", simple("${exchangeProperty.travelBookingId}"))
                .process(exchange -> exchange.getMessage().setBody(
                        Utils.prepareFlightBookingRequest(
                                exchange.getMessage().getBody(TravelBookingRequest.class))))
                .marshal(jaxbFlight)
                .doTry()
                    .to("spring-ws:{{travel.flight.url}}")
                .doCatch(SoapFaultClientException.class)
                    .process(exchange -> {
                        throw BookingBusinessException.fromSoapFault("flight",
                                exchange.getProperty(Exchange.EXCEPTION_CAUGHT, SoapFaultClientException.class));
                    })
                .end()
                .convertBodyTo(String.class)
                .to("stream:out")
                .unmarshal(jaxbFlight)
                .process((exchange) -> {
                    org.bp.flight.BookFligthResponse bookFlightResponse = exchange.getMessage().getBody(org.bp.flight.BookFligthResponse.class);
                    String travelBookingId=exchange.getProperty("travelBookingId", String.class);
                    bookingIdentifierService.assignFlightBookingId(travelBookingId, bookFlightResponse.getReturn().getId());
                });

        final JaxbDataFormat jaxbHotel = new JaxbDataFormat(
                org.bp.BookingInfo.class.getPackage().getName());
        from("direct:bookHotel").routeId("bookHotel")
                .log("bookHotel fired")
                .saga()
                    .propagation(SagaPropagation.MANDATORY)
                    .compensation("direct:cancelHotel").option("travelBookingId", simple("${exchangeProperty.travelBookingId}"))
                .process(exchange -> exchange.getMessage().setBody(
                        Utils.prepareHotelBookingRequest(
                                exchange.getMessage().getBody(TravelBookingRequest.class))))
                .marshal(jaxbHotel)
                .doTry()
                    .to("spring-ws:{{travel.hotel.url}}")
                .doCatch(SoapFaultClientException.class)
                    .process(exchange -> {
                        throw BookingBusinessException.fromSoapFault("hotel",
                                exchange.getProperty(Exchange.EXCEPTION_CAUGHT, SoapFaultClientException.class));
                    })
                .end()
                .convertBodyTo(String.class)
                .to("stream:out")
                .unmarshal(jaxbHotel)
                .process((exchange) -> {
                    org.bp.BookingInfo bookhotelResponse = exchange.getMessage().getBody(org.bp.BookingInfo.class);
                    String travelBookingId=exchange.getProperty("travelBookingId", String.class);
                    bookingIdentifierService.assignHotelBookingId(travelBookingId, bookhotelResponse.getId());
                })
                ;

        from("direct:cancelFlight").routeId("cancelFlight")
                .log("cancelFlight fired")
                .process(exchange -> {
                    String travelBookingId = exchange.getMessage().getHeader("travelBookingId", String.class);
                    if (travelBookingId == null || travelBookingId.isBlank()) {
                        throw new IllegalArgumentException("Missing travelBookingId header");
                    }
                    int flightBookingId = bookingIdentifierService.getFlightBookingId(travelBookingId);
                    if (flightBookingId == 0) {
                        exchange.setRouteStop(true);
                        return;
                    }
                    org.bp.flight.CancelBooking cancelFlightRequest = new org.bp.flight.CancelBooking();
                    cancelFlightRequest.setArg0(flightBookingId);
                    exchange.getMessage().setBody(cancelFlightRequest);
                })
                .marshal(jaxbFlight)
                .to("spring-ws:{{travel.flight.url}}")
                .convertBodyTo(String.class)
                .choice()
                    .when(exchange -> {
                        String response = exchange.getMessage().getBody(String.class);
                        return response != null && !response.isBlank();
                    })
                        .to("stream:out")
                        .unmarshal(jaxbFlight)
                .end();

        from("direct:cancelHotel").routeId("cancelHotel")
                .log("cancelHotel fired")
                .process(exchange -> {
                    String travelBookingId = exchange.getMessage().getHeader("travelBookingId", String.class);
                    if (travelBookingId == null || travelBookingId.isBlank()) {
                        throw new IllegalArgumentException("Missing travelBookingId header");
                    }
                    int hotelBookingId = bookingIdentifierService.getHotelBookingId(travelBookingId);
                    if (hotelBookingId == 0) {
                        exchange.setRouteStop(true);
                        return;
                    }
                    org.bp.CancelBookingRequest cancelHotelRequest = new org.bp.CancelBookingRequest();
                    cancelHotelRequest.setBookingId(hotelBookingId);
                    exchange.getMessage().setBody(cancelHotelRequest);
                })
                .marshal(jaxbHotel)
                .to("spring-ws:{{travel.hotel.url}}")
                .convertBodyTo(String.class)
                .choice()
                    .when(exchange -> {
                        String response = exchange.getMessage().getBody(String.class);
                        return response != null && !response.isBlank();
                    })
                        .to("stream:out")
                        .unmarshal(jaxbHotel)
                .end();

        from("direct:payment").routeId("payment")
                .log("payment fired")
                .marshal().json(JsonLibrary.Jackson)
                .removeHeaders("CamelHttp*")
                .to("rest:post:payment?host={{travel.payment.host}}&producerComponentName=http"
                        + "&bindingMode=off&consumes=application/json&produces=application/json")
                .convertBodyTo(String.class)
                .to("stream:out")
                .unmarshal().json(JsonLibrary.Jackson, org.bp.payment.PaymentResponse.class);
    }
}
