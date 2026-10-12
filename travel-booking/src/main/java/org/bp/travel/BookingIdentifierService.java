package org.bp.travel;

import java.util.concurrent.ConcurrentHashMap;
import java.util.UUID;

import org.springframework.stereotype.Service;

@Service
public class BookingIdentifierService {
	private final ConcurrentHashMap<String, BookingIds> bookingIdsMap = new ConcurrentHashMap<>();
	
	public String generateBookingId() {
		String bookingID=UUID.randomUUID().toString();
		BookingIds bookingIds= new BookingIds();
		bookingIdsMap.put(bookingID, bookingIds);
		return bookingID;
	}
	
	public void assignHotelBookingId(String travelBookingId, int hotelBookingId) {
		bookingIdsMap.get(travelBookingId).setHotelBookingId(hotelBookingId);
	}
	
	public void assignFlightBookingId(String travelBookingId, int flightBookingId) {
		bookingIdsMap.get(travelBookingId).setFlightBoogingId(flightBookingId);
	}
	
	public int getHotelBookingId(String travelBookingId) {
		return bookingIdsMap.get(travelBookingId).getHotelBookingId();
	}
	
	public int getFlightBookingId(String travelBookingId) {
		return bookingIdsMap.get(travelBookingId).getFlightBoogingId();
	}
	
	public static class BookingIds{
		private volatile int hotelBookingId;
		private volatile int flightBoogingId;

		public int getHotelBookingId() {
			return hotelBookingId;
		}
		public void setHotelBookingId(int hotelBookingId) {
			this.hotelBookingId = hotelBookingId;
		}
		public int getFlightBoogingId() {
			return flightBoogingId;
		}
		public void setFlightBoogingId(int flightBoogingId) {
			this.flightBoogingId = flightBoogingId;
		}

		
	}

}
