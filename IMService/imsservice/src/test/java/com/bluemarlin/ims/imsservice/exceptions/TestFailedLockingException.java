package com.bluemarlin.ims.imsservice.exceptions;

import org.junit.Test;

import static org.junit.Assert.*;

public class TestFailedLockingException {

    @Test
    public void getMessage() {
        FailedLockingException flckExc = new FailedLockingException();
        assertNotNull(flckExc);
        assertTrue(flckExc.getMessage().equals("Locking of booking index failed."));
    }
}