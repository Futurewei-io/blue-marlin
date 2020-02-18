package com.bluemarlin.ims.imsservice.model;

import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;

public class TestBookingBucket {

    @Test
    public void testClone() {
        String day = "20180105", bookingId = "test_bkid_1";
        List<String> prvBkOrder = new ArrayList();
        BookingBucket bb1 = new BookingBucket(day, bookingId, prvBkOrder, 1);
        BookingBucket res = BookingBucket.clone(bb1);
        assertNotSame(bb1, res);
        assertEquals(0, bb1.compareTo(res), 0);
    }

    @Test
    /**
     * Coverage purpose only. Data not verified.
     */
    public void getId() {
        String day = "20180105", bookingId = "test_bkid_1";
        List<String> prvBkOrder = new ArrayList();
        BookingBucket bb1 = new BookingBucket(day, bookingId, prvBkOrder, 1);
        assertNull(bb1.getId());
    }

    @Test
    public void getDay() {
        String day = "20180105", bookingId = "test_bkid_1";
        List<String> prvBkOrder = new ArrayList();
        BookingBucket bb1 = new BookingBucket(day, bookingId, prvBkOrder, 1);
        assertEquals("20180105", bb1.getDay());
    }

    @Test
    public void getPriority() {
        String day = "20180105", bookingId = "test_bkid_1";
        List<String> prvBkOrder = new ArrayList();
        BookingBucket bb1 = new BookingBucket(day, bookingId, prvBkOrder, 3);
        assertEquals(3D, bb1.getPriority(), 0);
    }

    @Test
    public void getAllocatedAmounts() {
        String day = "20180105", bookingId = "test_bkid_1";
        List<String> prvBkOrder = new ArrayList();
        BookingBucket bb1 = new BookingBucket(day, bookingId, prvBkOrder, 1);
        Map exp = new HashMap();
        assertTrue(bb1.getAllocatedAmounts().equals(exp));
    }

    @Test
    public void getSumOfAllocatedAmounts() {
        String day = "20180105", bookingId = "test_bkid_1";
        List<String> prvBkOrder = new ArrayList();
        BookingBucket bb1 = new BookingBucket(day, bookingId, prvBkOrder, 1);
        assertEquals(0D, bb1.getSumOfAllocatedAmounts(), 0);

        Map source = new HashMap(), alloc = new HashMap();
        alloc.put("alloc1", 1D);
        alloc.put("alloc2", 0.6999);
        source.put("day", day);
        source.put("minus_bk_ids", Arrays.asList("mnBkIds"));
        source.put("and_bk_ids", Arrays.asList("ndBkIds"));
        source.put("allocated_amount", alloc);
        source.put("priority", 2);
        String id2 = "test_MockID";
        BookingBucket bb2 = new BookingBucket(id2, source);
        assertEquals(1.6999, bb2.getSumOfAllocatedAmounts(), 0.1D);
    }

    @Test
    public void compareTo() {
        String day = "20180105", bookingId = "test_bkid_1";
        List<String> prvBkOrder = new ArrayList();
        BookingBucket bb1 = new BookingBucket(day, bookingId, prvBkOrder, 1);
        String day2 = "20180105", bookingId2 = "test_bkid_2";
        List<String> prvBkOrder2 = new ArrayList();
        BookingBucket bb2 = new BookingBucket(day2, bookingId2, prvBkOrder2, 2);
        assertEquals(1, bb1.compareTo(bb2), 0);
    }

    @Test
    public void getAvgTbrMap() {
        String day = "20180105", bookingId = "test_bkid_1";
        List<String> prvBkOrder = new ArrayList();
        BookingBucket bb1 = new BookingBucket(day, bookingId, prvBkOrder, 1);
        assertTrue(bb1.getAvgTbrMap().equals(new HashMap()));

        Map source = new HashMap(), alloc = new HashMap(), avg = new HashMap();
        alloc.put("alloc1", 1D);
        alloc.put("alloc2", 0.6999);
        avg.put("avg1", 1D);
        avg.put("avg2", 2D);
        source.put("day", day);
        source.put("minus_bk_ids", Arrays.asList("mnBkIds"));
        source.put("and_bk_ids", Arrays.asList("ndBkIds"));
        source.put("allocated_amount", alloc);
        source.put("priority", 2);
        source.put("avg_tbr_map", avg);
        String id2 = "test_MockID";
        BookingBucket bb2 = new BookingBucket(id2, source);
        assertTrue(bb2.getAvgTbrMap().equals(avg));
    }

    @Test
    public void setAvgTbrMap() {
        String day = "20180105", bookingId = "test_bkid_1";
        List<String> prvBkOrder = new ArrayList();
        BookingBucket bb1 = new BookingBucket(day, bookingId, prvBkOrder, 1);
        Map<String, List<Double>> avgMap = new HashMap();
        List<Double> list1 = new ArrayList(), list2 = new ArrayList();
        list1.add(1.0);
        list1.add(0.0);
        list2.add(10.0);
        list2.add(20.0);
        avgMap.put("test_bkid_1", list1);
        avgMap.put("test_bkid_2", list2);
        bb1.setAvgTbrMap(avgMap);
        assertTrue(bb1.getAvgTbrMap().equals(avgMap));
    }

    @Test
    public void getAndBookingsIds() {
        String day = "20180105", bookingId = "test_bkid_1";
        List<String> prvBkOrder = new ArrayList();
        BookingBucket bb1 = new BookingBucket(day, bookingId, prvBkOrder, 1);
        assertEquals(Arrays.asList(bookingId), bb1.getAndBookingsIds());
    }

    @Test
    public void getMinusBookingsIds() {
        String day = "20180105", bookingId = "test_bkid_1";
        List<String> prvBkOrder = new ArrayList();
        BookingBucket bb1 = new BookingBucket(day, bookingId, prvBkOrder, 1);
        assertEquals(0, bb1.getMinusBookingsIds().size(), 0);
    }

    @Test
    public void getAvrTBRInsight() {
        Set<String> bkIds = new HashSet();
        bkIds.add("test_bkid_1");
        bkIds.add("test_bkid_2");
        bkIds.add("test_bkid_3");

        String day = "20180105", bookingId = "test_bkid_1";
        List<String> prvBkOrder = new ArrayList();
        BookingBucket bb1 = new BookingBucket(day, bookingId, prvBkOrder, 1);

        Map<String, List<Double>> avgMap = new HashMap();
        List<Double> list1 = new ArrayList(), list2 = new ArrayList();
        list1.add(1.0);
        list1.add(2.0);
        list1.add(3.0);
        list1.add(4.0);
        list1.add(5.0);
        list1.add(6.0);
        list2.add(10.0);
        list2.add(20.0);
        list2.add(30.0);
        list2.add(40.0);
        list2.add(50.0);
        list2.add(60.0);
        avgMap.put("test_bkid_1", list1);
        avgMap.put("test_bkid_2", list2);
        bb1.setAvgTbrMap(avgMap);

        BookingBucket.AvrTBRInsight avrTbrI1 = bb1.getAvrTBRInsight(bkIds);

        assertNotNull(avrTbrI1);
        assertEquals(11D, avrTbrI1.getDenominator(), 0);
        assertEquals(202D, avrTbrI1.getNominator(), 0);

        Impression exp = new Impression();
        exp.setH0(new H0(600L));
        exp.setH1(new H1(800L));
        exp.setH2(new H2(1000L));
        exp.setH3(new H3(1200L));
        assertTrue(avrTbrI1.getMaxImpression().toString().equals(exp.toString()));
    }
}