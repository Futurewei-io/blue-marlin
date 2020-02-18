package com.bluemarlin.ims.imsservice.exceptions;

import org.junit.Test;

import static org.junit.Assert.*;

public class TestBookingNotFoundException {

    @Test
    public void getMessage() {
        BookingNotFoundException bkExc = new BookingNotFoundException();
        assertNotNull(bkExc);
        assertTrue(bkExc.getMessage().equals("Booking not found."));
    }
}