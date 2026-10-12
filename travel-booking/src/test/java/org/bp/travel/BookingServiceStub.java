package org.bp.travel;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.springframework.test.context.DynamicPropertyRegistry;

final class BookingServiceStub implements AutoCloseable {
    private final HttpServer server;
    private final java.util.concurrent.ExecutorService executor = Executors.newCachedThreadPool();
    volatile String failure;
    volatile String paymentJson;
    volatile String flightXml;
    volatile String hotelXml;
    volatile boolean parallelBookings;
    volatile long successfulBookingDelay;
    volatile boolean faultDetails;
    volatile boolean emptyCancellationResponses;
    final AtomicInteger flightCancellations = new AtomicInteger();
    final AtomicInteger hotelCancellations = new AtomicInteger();
    volatile String flightCancellationXml;
    volatile String hotelCancellationXml;
    private volatile CountDownLatch cancelledFlight;
    private volatile CountDownLatch cancelledHotel;
    final AtomicInteger payments = new AtomicInteger();
    private volatile CountDownLatch bookings;

    BookingServiceStub(DynamicPropertyRegistry registry) throws IOException {
        server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        server.setExecutor(executor);
        server.createContext("/flight", exchange -> book(exchange, "flight"));
        server.createContext("/hotel", exchange -> book(exchange, "hotel"));
        server.createContext("/payment", exchange -> {
            paymentJson = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            payments.incrementAndGet();
            reply(exchange, "payment".equals(failure) ? 500 : 200, "application/json", """
                    {"transactionId":73,"transactionDate":"2026-10-12T10:00:00Z"}
                    """);
        });
        server.start();
        String host = "http://localhost:" + server.getAddress().getPort();
        registry.add("travel.flight.url", () -> host + "/flight");
        registry.add("travel.hotel.url", () -> host + "/hotel");
        registry.add("travel.payment.host", () -> host);
        reset();
    }

    void reset() {
        failure = null;
        paymentJson = null;
        flightXml = null;
        hotelXml = null;
        parallelBookings = false;
        successfulBookingDelay = 0;
        faultDetails = true;
        emptyCancellationResponses = false;
        flightCancellations.set(0);
        hotelCancellations.set(0);
        flightCancellationXml = null;
        hotelCancellationXml = null;
        cancelledFlight = new CountDownLatch(1);
        cancelledHotel = new CountDownLatch(1);
        payments.set(0);
        bookings = new CountDownLatch(2);
    }

    private void book(HttpExchange exchange, String service) throws IOException {
        String request = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
        if (request.contains("cancelBooking")) {
            String body;
            if (service.equals("flight")) {
                flightCancellationXml = request;
                flightCancellations.incrementAndGet();
                body = "<f:cancelBookingResponse xmlns:f=\"http://flight.bp.org/\"><return><id>42</id><cost>123.45</cost></return></f:cancelBookingResponse>";
            } else {
                hotelCancellationXml = request;
                hotelCancellations.incrementAndGet();
                body = "<h:cancelBookingResponse xmlns:h=\"http://www.bp.org\" xmlns:t=\"http://www.bp.org/types\"><t:id>73</t:id><t:cost>450.00</t:cost></h:cancelBookingResponse>";
            }
            reply(exchange, 200, "text/xml; charset=UTF-8", envelope(emptyCancellationResponses ? "" : body));
            (service.equals("flight") ? cancelledFlight : cancelledHotel).countDown();
            return;
        }
        if (service.equals("flight")) {
            flightXml = request;
        } else {
            hotelXml = request;
        }
        bookings.countDown();
        try {
            parallelBookings = bookings.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        }
        String body;
        boolean failed = service.equals(failure);
        try {
            if (!failed && successfulBookingDelay > 0) {
                Thread.sleep(successfulBookingDelay);
            } else if (failed && successfulBookingDelay == 0) {
                Thread.sleep(200);
            }
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        }
        if (failed) {
            String detail = service.equals("flight")
                    ? """
                      <detail>
                        <ns1:Fault xmlns:ns1="http://flight.bp.org/">
                          <text xmlns:ns2="http://flight.bp.org/">Start and destination airport can not be the same</text>
                          <code xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xs="http://www.w3.org/2001/XMLSchema"
                                xmlns:ns2="http://flight.bp.org/" xsi:type="xs:int">5</code>
                        </ns1:Fault>
                      </detail>
                      """
                    : "<detail><h:hotelFault xmlns:h=\"http://www.bp.org\" xmlns:t=\"http://www.bp.org/types\"><t:text>Hotel &quot;Test Hotel&quot; is unavailable</t:text><t:code>7</t:code></h:hotelFault></detail>";
            body = "<soap:Fault><faultcode>soap:Server</faultcode><faultstring>Booking failed</faultstring>"
                    + (faultDetails ? detail : "") + "</soap:Fault>";
        } else if (service.equals("flight")) {
            body = """
                    <f:bookFligthResponse xmlns:f="http://flight.bp.org/">
                      <return><cost>123.45</cost><id>42</id></return>
                    </f:bookFligthResponse>
                    """;
        } else {
            body = """
                    <h:bookRoomResponse xmlns:h="http://www.bp.org" xmlns:t="http://www.bp.org/types">
                      <t:id>73</t:id><t:cost>450.00</t:cost>
                    </h:bookRoomResponse>
                    """;
        }
        reply(exchange, failed ? 500 : 200, "text/xml; charset=UTF-8",
                envelope(body));
    }

    boolean awaitCancellation(String service) throws InterruptedException {
        return (service.equals("flight") ? cancelledFlight : cancelledHotel).await(5, TimeUnit.SECONDS);
    }

    private static String envelope(String body) {
        return "<soap:Envelope xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\"><soap:Body>"
                + body + "</soap:Body></soap:Envelope>";
    }

    private static void reply(HttpExchange exchange, int status, String type, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", type);
        exchange.sendResponseHeaders(status, bytes.length);
        try (var output = exchange.getResponseBody()) {
            output.write(bytes);
        } finally {
            exchange.close();
        }
    }

    @Override
    public void close() {
        server.stop(0);
        executor.shutdownNow();
    }
}
