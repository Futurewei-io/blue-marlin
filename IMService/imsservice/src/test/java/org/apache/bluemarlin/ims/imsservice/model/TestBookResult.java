package org.apache.bluemarlin.ims.imsservice.model;

import org.junit.Test;

import static org.junit.Assert.*;

public class TestBookResult {

    @Test
    public void setTotalBooked() {
        Long bkQty = (long) 10, setQty = (long) 20;
        BookResult bkres1 = new BookResult("test_bkid_1", bkQty);
        bkres1.setTotalBooked(setQty);

        assertNotNull(bkres1);
        assertEquals(setQty, bkres1.getTotalBooked());
    }

    @Test
    public void getBookId() {
        Long bkQty = (long) 10;
        BookResult bkres1 = new BookResult("test_bkid_1", bkQty);
        assertEquals("test_bkid_1", bkres1.getBookId());
    }

    @Test
    public void getTotalBooked() {
        Long bkQty = (long) 10;
        BookResult bkres1 = new BookResult("test_bkid_1", bkQty);
        assertEquals(bkQty, bkres1.getTotalBooked());
    }
}