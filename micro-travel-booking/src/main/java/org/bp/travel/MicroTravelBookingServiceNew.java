package org.bp.travel;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.rest.RestBindingMode;
import org.apache.camel.model.rest.RestHostNameResolver;
import org.springframework.stereotype.Component;

@Component
public class MicroTravelBookingServiceNew  {

    private final BookingIdentifierService bookingIdentifierService;

    public MicroTravelBookingServiceNew(BookingIdentifierService bookingIdentifierService) {
        this.bookingIdentifierService = bookingIdentifierService;
    }


    private void gateway() {
        /*

        restConfiguration()
                .apiContextPath("/api-doc")
                .hostNameResolver(RestHostNameResolver.none)
                .bindingMode(RestBindingMode.json)
                .bindingPackageScan("org.bp.travel.model");

        rest().openApi("travel-booking.yaml");
        from("direct:bookTravel")
                .routeId("bookTravel")
                .log("bookTravel fired")
                .setHeader("bookingTravelId", bookingIdentifierService::getBookingIdentifier)
                .wireTap("direct:publishBookingRequest")
                .setBody(exchange -> new BookingInfo()
                        .id(exchange.getMessage().getHeader("bookingTravelId", String.class))
                        .status(BookingInfo.StatusEnum.PENDING))
                .setHeader(Exchange.HTTP_RESPONSE_CODE, constant(202));

        from("direct:publishBookingRequest")
                .routeId("publish-booking-request")
                .marshal().json()
                .convertBodyTo(String.class)
                .removeHeaders("*", "bookingTravelId")
                .setHeader(KafkaConstants.KEY, header("bookingTravelId"))
                .to("kafka:{{travel.kafka.topic}}?lazyStartProducer=true");

        from("direct:getBookingInfo")
                .routeId("get-booking-info")
                // TODO: Load the booking by id from the booking store.
                .setBody(exchange -> new BookingInfo()
                        .id(exchange.getMessage().getHeader("id", String.class))
                        .status(BookingInfo.StatusEnum.PENDING))
                .setHeader(Exchange.HTTP_RESPONSE_CODE, constant(200));

 */
    }
}
