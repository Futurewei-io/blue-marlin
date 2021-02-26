package org.apache.bluemarlin.ims.imsservice.model;

import org.junit.Test;

import static org.junit.Assert.*;

public class TestInventoryResult {

    @Test
    public void getAvailCount() {
        InventoryResult ivRes = new InventoryResult();
        assertEquals(0, ivRes.getAvailCount(), 0);
        ivRes.setAvailCount(98765);
        assertEquals(98765, ivRes.getAvailCount(), 0);

        InventoryResult ivRes2 = new InventoryResult(123456);
        assertEquals(123456, ivRes2.getAvailCount(), 0);
    }

    @Test
    public void setAvailCount() {
        InventoryResult ivRes = new InventoryResult();
        assertEquals(0, ivRes.getAvailCount(), 0);
        ivRes.setAvailCount(45678);
        assertEquals(45678, ivRes.getAvailCount(), 0);
    }
}