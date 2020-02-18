package com.bluemarlin.ims.imsservice.exceptions;

import org.junit.Test;

import static org.junit.Assert.*;

public class TestNoInventoryExceptions {

    @Test
    public void getMessage() {
        NoInventoryExceptions noInvExc = new NoInventoryExceptions();
        assertNotNull(noInvExc);
        assertTrue(noInvExc.getMessage().equals("No inventory found."));
    }
}