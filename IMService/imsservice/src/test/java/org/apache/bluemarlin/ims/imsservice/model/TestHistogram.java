package org.apache.bluemarlin.ims.imsservice.model;

import org.junit.Test;

import static org.junit.Assert.*;

public class TestHistogram {

    @Test
    public void getH() {
        Histogram hstm = new Histogram();
        assertNull(hstm.getH());
        hstm.setH("2018-01-05");
        assertEquals("2018-01-05", hstm.getH());
    }

    @Test
    public void setH() {
        Histogram hstm = new Histogram();
        hstm.setH("2018-01-05");
        assertEquals("2018-01-05", hstm.getH());
    }

    @Test
    public void getT() {
        Histogram hstm = new Histogram();
        assertNull(hstm.getT());
        hstm.setT((long) 123456);
        assertEquals(123456L, hstm.getT(), 0);
    }

    @Test
    public void setT() {
        Histogram hstm = new Histogram();
        hstm.setT((long) 123456);
        assertEquals(123456L, hstm.getT(), 0);
    }
}