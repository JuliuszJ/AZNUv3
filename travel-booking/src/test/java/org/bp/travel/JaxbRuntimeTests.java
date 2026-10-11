package org.bp.travel;

import java.io.StringReader;
import java.io.StringWriter;
import javax.xml.namespace.QName;

import jakarta.xml.bind.JAXBContext;
import org.bp.BookRoomRequest;
import org.bp.CancelBookingRequest;
import org.bp.flight.BookFligth;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class JaxbRuntimeTests {

    @Test
    void marshalsAndUnmarshalsGeneratedRequests() throws Exception {
        assertRoundTrip(new BookFligth(), "bookFligth", "http://flight.bp.org/");
        assertRoundTrip(new org.bp.flight.CancelBooking(), "cancelBooking", "http://flight.bp.org/");
        assertRoundTrip(new BookRoomRequest(), "bookRoomRequest", "http://www.bp.org");
        CancelBookingRequest cancellation = new CancelBookingRequest();
        cancellation.setBookingId(42);
        CancelBookingRequest restored = (CancelBookingRequest) assertRoundTrip(
                cancellation, "cancelBookingRequest", "http://www.bp.org");
        assertEquals(cancellation.getBookingId(), restored.getBookingId());
    }

    private Object assertRoundTrip(Object request, String elementName, String namespace) throws Exception {
        JAXBContext context = JAXBContext.newInstance(request.getClass());
        assertEquals(new QName(namespace, elementName), context.createJAXBIntrospector().getElementName(request));
        StringWriter xml = new StringWriter();
        context.createMarshaller().marshal(request, xml);
        Object restored = context.createUnmarshaller().unmarshal(new StringReader(xml.toString()));
        assertEquals(request.getClass(), restored.getClass());
        return restored;
    }
}
