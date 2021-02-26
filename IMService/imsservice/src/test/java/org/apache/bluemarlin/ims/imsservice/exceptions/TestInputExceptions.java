package org.apache.bluemarlin.ims.imsservice.exceptions;

import org.junit.Test;

import static org.junit.Assert.*;

public class TestInputExceptions {

    @Test
    public void getMessage() {
        InputExceptions iptExc = new InputExceptions();
        assertNotNull(iptExc);
        iptExc = new InputExceptions("input exception");
        assertTrue(iptExc.getMessage().equals("input exception"));
    }
}