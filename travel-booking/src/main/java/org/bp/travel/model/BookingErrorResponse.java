package org.bp.travel.model;

public record BookingErrorResponse(String service, Integer code, String message) {
}
