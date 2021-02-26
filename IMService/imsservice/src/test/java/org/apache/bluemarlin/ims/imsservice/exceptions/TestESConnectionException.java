package org.apache.bluemarlin.ims.imsservice.exceptions;

import org.junit.Test;

import static org.junit.Assert.*;

public class TestESConnectionException {

    @Test
    public void getMessage() {
        ESConnectionException esconnExc = new ESConnectionException();
        assertNotNull(esconnExc);
        assertTrue(esconnExc.getMessage().equals("Elasticsearch connection failed."));
        esconnExc = new ESConnectionException("es-connection exception");
        assertTrue(esconnExc.getMessage().equals("es-connection exception"));
    }
}