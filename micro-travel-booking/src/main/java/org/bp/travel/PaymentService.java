package org.bp.travel;


import java.io.Serializable;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

import org.bp.travel.model.BookTravelRequest;
import org.bp.travel.model.BookingInfo;
import org.springframework.stereotype.Service;

@Service
public class PaymentService {
	private final IMap<String, PaymentData> payments;

	public PaymentService(HazelcastInstance hazelcastInstance) {
		payments=hazelcastInstance.getMap("payments");
	}

	public static class PaymentData implements Serializable {
		BookTravelRequest bookTravelRequest;
		BookingInfo hotelBookingInfo;
		BookingInfo flightBookingInfo;
		public boolean isReady() {
			return bookTravelRequest!=null && hotelBookingInfo!=null && flightBookingInfo!=null;
		}
	}

	public boolean addBookTravelRequest(String bookTravelId, BookTravelRequest bookTravelRequest) {
		payments.lock(bookTravelId);
		try {
			PaymentData paymentData = getPaymentData(bookTravelId);
			paymentData.bookTravelRequest=bookTravelRequest;
			payments.set(bookTravelId, paymentData);
			return paymentData.isReady();
		}
		finally {
			payments.unlock(bookTravelId);
		}
	}


	public boolean addBookingInfo(String bookTravelId, BookingInfo bookingInfo, String serviceType) {
		payments.lock(bookTravelId);
		try {
			PaymentData paymentData = getPaymentData(bookTravelId);
			if (serviceType.equals("flight"))
				paymentData.flightBookingInfo=bookingInfo;
			else
				paymentData.hotelBookingInfo=bookingInfo;
			payments.set(bookTravelId, paymentData);
			return paymentData.isReady();
		}
		finally {
			payments.unlock(bookTravelId);
		}
	}


	public PaymentData getPaymentData(String bookTravelId) {
		PaymentData paymentData = payments.get(bookTravelId);
		if (paymentData==null) {
			paymentData = new PaymentData();
		}
		return paymentData;
	}

}
