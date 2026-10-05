package org.bp.hotel;

import org.bp.BookRoomRequest;
import org.bp.CancelBookingRequest;
import org.bp.types.BookingInfo;

@org.springframework.stereotype.Service
public class HotelBookingEndpoint implements HotelBooking{
    @Override
    public BookingInfo bookRoom(BookRoomRequest payload) throws HotelFaultMsg {
        if (payload!=null && payload.getHotel()!=null
                && "China".equals(payload.getHotel().getCountry())) {
            org.bp.types.Fault hotelFault = new org.bp.types.Fault();
            hotelFault.setCode(10);
            hotelFault.setText("China is temporarily not operated destination");
            HotelFaultMsg fault = new HotelFaultMsg("Room is not available", hotelFault);
            throw fault;
        }
        BookingInfo response = new BookingInfo();
        response.setId(1);
        response.setCost(new java.math.BigDecimal(999));
        return response;
    }

    @Override
    public BookingInfo cancelBooking(CancelBookingRequest payload) throws HotelFaultMsg {
        return null;
    }
}
