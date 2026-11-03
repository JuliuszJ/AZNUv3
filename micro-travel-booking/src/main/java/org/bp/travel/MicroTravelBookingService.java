package org.bp.travel;

import java.math.BigDecimal;
import java.time.OffsetDateTime;

import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.model.dataformat.JsonLibrary;
import org.apache.camel.model.rest.RestBindingMode;
import org.apache.camel.model.rest.RestHostNameResolver;
import org.bp.travel.exceptions.FlightException;
import org.bp.travel.exceptions.HotelException;
import org.bp.travel.model.BookTravelRequest;
import org.bp.travel.model.BookingInfo;
import org.bp.travel.model.ExceptionResponse;
import org.bp.travel.state.ProcessingEvent;
import org.bp.travel.state.ProcessingState;
import org.bp.travel.state.StateService;
import org.springframework.stereotype.Component;


@Component
public class MicroTravelBookingService extends RouteBuilder {

    @org.springframework.beans.factory.annotation.Value("${travel.kafka.server}")
    private String travelKafkaServer;
    @org.springframework.beans.factory.annotation.Value("${travel.service.type}")
    private String travelServiceType;

    private final StateService flightStateService;
    private final StateService hotelStateService;
    private final BookingInfoService bookingInfoService;
    private final BookingIdentifierService bookingIdentifierService;
    private final PaymentService paymentService;

    public MicroTravelBookingService(
            BookingIdentifierService bookingIdentifierService,
            PaymentService paymentService,
            StateService flightStateService,
            StateService hotelStateService,
            BookingInfoService bookingInfoService) {
        this.bookingIdentifierService = bookingIdentifierService;
        this.paymentService = paymentService;
        this.flightStateService = flightStateService;
        this.hotelStateService = hotelStateService;
        this.bookingInfoService = bookingInfoService;
    }

    @Override
    public void configure() throws Exception {
        if (travelServiceType.equals("all") || travelServiceType.equals("hotel"))
            bookHotelExceptionHandlers();
        if (travelServiceType.equals("all") || travelServiceType.equals("flight"))
            bookFlightExceptionHandlers();
        if (travelServiceType.equals("all") || travelServiceType.equals("hotel")
                || travelServiceType.equals("flight"))
            bookingCanceled();
        if (travelServiceType.equals("all") || travelServiceType.equals("gateway"))
            gateway();
        if (travelServiceType.equals("all") || travelServiceType.equals("hotel"))
            hotel();
        if (travelServiceType.equals("all") || travelServiceType.equals("flight"))
            flight();
        if (travelServiceType.equals("all") || travelServiceType.equals("payment"))
            payment();
    }

    private void bookHotelExceptionHandlers() {
        onException(HotelException.class)
                .process((exchange) -> {
                            ExceptionResponse er = new ExceptionResponse();
                            er.setTimestamp(OffsetDateTime.now());
                            Exception cause = exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Exception.class);
                            er.setMessage(cause.getMessage());
                            exchange.getMessage().setBody(er);
                        }
                )
                .marshal().json()
                .to("stream:out")
                .setHeader("serviceType", constant("hotel"))
                .setHeader(KafkaConstants.KEY, header("bookingTravelId"))
                .to("kafka:TravelBookingFailTopic?brokers=" + travelKafkaServer + "&groupId=" + travelServiceType)
                .to("direct:publishBookingCanceled")
                .handled(true)
        ;
    }

    private void bookFlightExceptionHandlers() {
        onException(FlightException.class)
                .process((exchange) -> {
                            ExceptionResponse er = new ExceptionResponse();
                            er.setTimestamp(OffsetDateTime.now());
                            Exception cause = exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Exception.class);
                            er.setMessage(cause.getMessage());
                            exchange.getMessage().setBody(er);
                        }
                )
                .marshal().json()
                .to("stream:out")
                .setHeader("serviceType", constant("flight"))
                .setHeader(KafkaConstants.KEY, header("bookingTravelId"))
                .to("kafka:TravelBookingFailTopic?brokers=" + travelKafkaServer + "&groupId=" + travelServiceType)
                .to("direct:publishBookingCanceled")
                .handled(true)
        ;
    }

    private void gateway() {

        restConfiguration()
                .apiContextPath("/api-doc")
                .hostNameResolver(RestHostNameResolver.none)
                .bindingMode(RestBindingMode.json)
                .skipBindingOnErrorCode(true)
                .bindingPackageScan("org.bp.travel.model");

        rest().openApi().specification("travel-booking.yaml");

        from("direct:bookTravel").routeId("bookTravel")
                .log("bookTravel fired")
                .process((exchange) -> {
                    String bookingTravelId = bookingIdentifierService.getBookingIdentifier();
                    exchange.getMessage().setHeader("bookingTravelId", bookingTravelId);
                    // Store PENDING before publishing, so a fast saga result cannot be overwritten.
                    bookingInfoService.save(new BookingInfo()
                            .id(bookingTravelId)
                            .status(BookingInfo.StatusEnum.PENDING));
                })
                .to("direct:TravelBookRequest")
                .to("direct:bookRequester");

        from("direct:bookRequester").routeId("bookRequester")
                .log("bookRequester fired")
                .setBody(exchange -> new BookingInfo()
                        .id(exchange.getMessage().getHeader("bookingTravelId", String.class))
                        .status(BookingInfo.StatusEnum.PENDING))
                .setHeader(Exchange.HTTP_RESPONSE_CODE, constant(202))
        ;

        from("direct:TravelBookRequest").routeId("TravelBookRequest")
                .log("brokerTopic fired")
                .marshal().json()
                .setHeader(KafkaConstants.KEY, header("bookingTravelId"))
                .to("kafka:TravelReqTopic?brokers=" + travelKafkaServer + "&groupId=" + travelServiceType)
        ;

        from("kafka:TravelPaymentResultTopic?brokers=" + travelKafkaServer + "&groupId=" + travelServiceType)
                .routeId("gatewayPaymentResult")
                .unmarshal().json(JsonLibrary.Jackson, BookingInfo.class)
                .process(exchange -> bookingInfoService.save(exchange.getMessage().getBody(BookingInfo.class)));

        from("direct:getBookingInfo").routeId("getBookingInfo")
                .process(exchange -> {
                    String id = exchange.getMessage().getHeader("id", String.class);
                    BookingInfo bookingInfo = bookingInfoService.findById(id);
                    if (bookingInfo == null) {
                        exchange.getMessage().setBody(new ExceptionResponse()
                                .timestamp(OffsetDateTime.now())
                                .message("Booking not found: " + id));
                        exchange.getMessage().setHeader(Exchange.HTTP_RESPONSE_CODE, 404);
                    } else {
                        exchange.getMessage().setBody(bookingInfo);
                        exchange.getMessage().setHeader(Exchange.HTTP_RESPONSE_CODE, 200);
                    }
                })
                .choice()
                    // REST binding skips error responses, so serialize the error explicitly.
                    .when(header(Exchange.HTTP_RESPONSE_CODE).isEqualTo(404))
                        .marshal().json()
                        .convertBodyTo(String.class)
                .end();

    }

    private void bookingCanceled() {
        from("direct:publishBookingCanceled").routeId("publishBookingCanceled")
                .setBody(exchange -> new BookingInfo()
                        .id(exchange.getMessage().getHeader("bookingTravelId", String.class))
                        .status(BookingInfo.StatusEnum.CANCELED))
                .marshal().json()
                .setHeader(KafkaConstants.KEY, header("bookingTravelId"))
                .to("kafka:TravelPaymentResultTopic?brokers=" + travelKafkaServer + "&groupId=" + travelServiceType);
    }

    private void hotel() {
        from("kafka:TravelReqTopic?brokers=" + travelKafkaServer + "&groupId=" + travelServiceType).routeId("bookHotel")
                .log("fired bookHotel")
                .unmarshal().json(JsonLibrary.Jackson, BookTravelRequest.class)
                .process(
                        (exchange) -> {
                            String bookingTravelId = exchange.getMessage().getHeader("bookingTravelId", String.class);
                            ProcessingState previousState = hotelStateService.sendEvent(bookingTravelId, ProcessingEvent.START);
                            if (previousState!=ProcessingState.CANCELLED) {
                                BookingInfo bi = new BookingInfo();
                                bi.setId(bookingIdentifierService.getBookingIdentifier());

                                BookTravelRequest btr= exchange.getMessage().getBody(BookTravelRequest.class);
                                if (btr!=null && btr.getHotel()!=null
                                        && btr.getHotel().getCountry()!=null ) {
                                    String country = btr.getHotel().getCountry();
                                    if (country.equals("USA")) {
                                        bi.setCost(new BigDecimal(999));
                                    }
                                    else if (country.equals("China")){
                                        throw new HotelException("Not serviced destination: "+country);
                                    }
                                    else {
                                        bi.setCost(new BigDecimal(888));
                                    }
                                }
                                exchange.getMessage().setBody(bi);
                                previousState = hotelStateService.sendEvent(bookingTravelId, ProcessingEvent.FINISH);
                            }
                            exchange.getMessage().setHeader("previousState", previousState);        		}
                )
                .marshal().json()
                .to("stream:out")
                .choice()
                .when(header("previousState").isEqualTo(ProcessingState.CANCELLED))
                .to("direct:bookHotelCompensationAction")
                .otherwise()
                .setHeader("serviceType", constant("hotel"))
                .setHeader(KafkaConstants.KEY, header("bookingTravelId"))
                .to("kafka:BookingInfoTopic?brokers=" + travelKafkaServer + "&groupId=" + travelServiceType)
                .endChoice()
        ;

        from("kafka:TravelBookingFailTopic?brokers=" + travelKafkaServer + "&groupId=" + travelServiceType).routeId("bookHotelCompensation")
                .log("fired bookHotelCompensation")
                .unmarshal().json(JsonLibrary.Jackson, ExceptionResponse.class)
                .choice()
                .when(header("serviceType").isNotEqualTo("hotel"))
                .process((exchange) -> {
                    String bookingTravelId = exchange.getMessage().getHeader("bookingTravelId", String.class);
                    ProcessingState previousState = hotelStateService.sendEvent(bookingTravelId, ProcessingEvent.CANCEL);
                    exchange.getMessage().setHeader("previousState", previousState);
                })
                .choice()
                .when(header("previousState").isEqualTo(ProcessingState.FINISHED))
                .to("direct:bookHotelCompensationAction")
                .endChoice()
                .endChoice();

        from("direct:bookHotelCompensationAction").routeId("bookHotelCompensationAction")
                .log("fired bookHotelCompensationAction")
                .to("stream:out");
    }

    private void flight() {
        from("kafka:TravelReqTopic?brokers=" + travelKafkaServer + "&groupId=" + travelServiceType).routeId("bookFlight")
                .log("fired bookFlight")
                .unmarshal().json(JsonLibrary.Jackson, BookTravelRequest.class)
                .process(
                        (exchange) -> {
                            String bookingTravelId = exchange.getMessage().getHeader("bookingTravelId", String.class);
                            ProcessingState previousState = flightStateService.sendEvent(bookingTravelId, ProcessingEvent.START);
                            if (previousState!=ProcessingState.CANCELLED) {
                                BookingInfo bi = new BookingInfo();
                                bi.setId(bookingIdentifierService.getBookingIdentifier());
                                BookTravelRequest btr= exchange.getMessage().getBody(BookTravelRequest.class);
                                if (btr!=null && btr.getFlight()!=null && btr.getFlight().getFrom()!=null
                                        && btr.getFlight().getFrom().getAirport()!=null) {
                                    String from=btr.getFlight().getFrom().getAirport();
                                    if (from.equals("JFK")) {
                                        bi.setCost(new BigDecimal(700));
                                    }
                                    else if (from.equals("PEK")){
                                        throw new FlightException("Not serviced airport: "+from);
                                    }
                                    else {
                                        bi.setCost(new BigDecimal(600));
                                    }
                                }

                                exchange.getMessage().setBody(bi);
                                previousState = flightStateService.sendEvent(bookingTravelId, ProcessingEvent.FINISH);
                            }
                            exchange.getMessage().setHeader("previousState", previousState);
                        }
                )
                .marshal().json()
                .to("stream:out")
                .choice()
                .when(header("previousState").isEqualTo(ProcessingState.CANCELLED))
                .to("direct:bookFlightCompensationAction")
                .otherwise()
                .setHeader("serviceType", constant("flight"))
                .setHeader(KafkaConstants.KEY, header("bookingTravelId"))
                .to("kafka:BookingInfoTopic?brokers=" + travelKafkaServer + "&groupId=" + travelServiceType)
                .endChoice()
        ;

        from("kafka:TravelBookingFailTopic?brokers=" + travelKafkaServer + "&groupId=" + travelServiceType).routeId("bookFlightCompensation")
                .log("fired bookFlightCompensation")
                .unmarshal().json(JsonLibrary.Jackson, ExceptionResponse.class)
                .choice()
                .when(header("serviceType").isNotEqualTo("flight"))
                .process((exchange) -> {
                    String bookingTravelId = exchange.getMessage().getHeader("bookingTravelId", String.class);
                    ProcessingState previousState = flightStateService.sendEvent(bookingTravelId, ProcessingEvent.CANCEL);
                    exchange.getMessage().setHeader("previousState", previousState);
                })
                .choice()
                .when(header("previousState").isEqualTo(ProcessingState.FINISHED))
                .to("direct:bookFlightCompensationAction")
                .endChoice()
                .endChoice();

        from("direct:bookFlightCompensationAction").routeId("bookFlightCompensationAction")
                .log("fired bookFlightCompensationAction")
                .to("stream:out");
    }

    private void payment() {
        from("kafka:BookingInfoTopic?brokers=" + travelKafkaServer + "&groupId=" + travelServiceType).routeId("paymentBookingInfo")
                .log("fired paymentBookingInfo")
                .unmarshal().json(JsonLibrary.Jackson, BookingInfo.class)
                .process(
                        (exchange) -> {
                            String bookingTravelId = exchange.getMessage().getHeader("bookingTravelId", String.class);
                            boolean isReady= paymentService.addBookingInfo(
                                    bookingTravelId,
                                    exchange.getMessage().getBody(BookingInfo.class),
                                    exchange.getMessage().getHeader("serviceType", String.class));
                            exchange.getMessage().setHeader("isReady", isReady);
                        }
                )
                .choice()
                .when(header("isReady").isEqualTo(true)).to("direct:finalizePayment")
                .endChoice();

        from("kafka:TravelReqTopic?brokers=" + travelKafkaServer + "&groupId=" + travelServiceType).routeId("paymentTravelReq")
                .log("fired paymentTravelReq")
                .unmarshal().json(JsonLibrary.Jackson, BookTravelRequest.class)
                .process(
                        (exchange) -> {
                            String bookingTravelId = exchange.getMessage().getHeader("bookingTravelId", String.class);
                            boolean isReady= paymentService.addBookTravelRequest(
                                    bookingTravelId,
                                    exchange.getMessage().getBody(BookTravelRequest.class));
                            exchange.getMessage().setHeader("isReady", isReady);
                        }
                )
                .choice()
                .when(header("isReady").isEqualTo(true)).to("direct:finalizePayment")
                .endChoice();

        from("direct:finalizePayment").routeId("finalizePayment")
                .log("fired finalizePayment")
                .process(
                        (exchange) -> {
                            String bookingTravelId = exchange.getMessage().getHeader("bookingTravelId", String.class);
                            PaymentService.PaymentData paymentData = paymentService.getPaymentData(bookingTravelId);
                            BigDecimal hotelCost=paymentData.hotelBookingInfo.getCost();
                            BigDecimal flightCost=paymentData.flightBookingInfo.getCost();
                            BigDecimal totalCost=hotelCost.add(flightCost);
                            BookingInfo travelBookingInfo = new BookingInfo();
                            travelBookingInfo.setId(bookingTravelId);
                            travelBookingInfo.setCost(totalCost);
                            travelBookingInfo.setStatus(BookingInfo.StatusEnum.DONE);
                            exchange.getMessage().setBody(travelBookingInfo);
                        }
                )
                .to("direct:notification")
                .marshal().json()
                .setHeader(KafkaConstants.KEY, header("bookingTravelId"))
                .to("kafka:TravelPaymentResultTopic?brokers=" + travelKafkaServer + "&groupId=" + travelServiceType)
        ;

        from("direct:notification").routeId("notification")
                .log("fired notification")
                .to("stream:out");

    }


}
