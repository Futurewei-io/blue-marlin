package org.apache.bluemarlin.ims.imsservice.exceptions;

import org.junit.Test;

import static org.junit.Assert.*;

public class TestIMSException {

    @Test
    public void getMessage() {
        IMSException imsExc = new IMSException("ims exception");
        assertNotNull(imsExc);
        assertTrue(imsExc.getMessage().equals("ims exception"));
    }
}