package org.bp.travel;

import java.util.Map;

import com.hazelcast.core.HazelcastInstance;
import org.bp.travel.model.BookingInfo;
import org.springframework.stereotype.Service;

@Service
public class BookingInfoService {
    private final Map<String, BookingInfo> bookings;

    public BookingInfoService(HazelcastInstance hazelcastInstance) {
        bookings = hazelcastInstance.getMap("bookings");
    }

    public void save(BookingInfo bookingInfo) {
        bookings.compute(bookingInfo.getId(), (id, current) ->
            current != null && current.getStatus() == BookingInfo.StatusEnum.CANCELED
                ? current : bookingInfo);
    }

    public BookingInfo findById(String bookingTravelId) {
        return bookings.get(bookingTravelId);
    }
}
