package com.bluemarlin.ims.imsservice.model;

import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;

public class TestBooking {


    @Test
    public void bookingWithMap() {
        Map source = new HashMap(), query = new HashMap();
        source.put("days", Arrays.asList("20180105"));
        source.put("bk_id", "test_MockBookingId");
        source.put("price", 100);
        source.put("amount", 37);
        source.put("adv_id", "test_MockAdvId");
        source.put("query", query);
        source.put("del", false);
        String id = "test_MockID";

        Booking bk = new Booking(id, source);

        assertNotNull(bk);
        assertEquals(id, bk.getId());
        assertEquals(Arrays.asList("20180105"), bk.getDays());
        assertEquals("test_MockBookingId", bk.getBookingId());
        assertEquals(100D, bk.getPrice(), 0);
        assertEquals(37D, bk.getAmount(), 0);
        assertEquals("test_MockAdvId", bk.getAdvId());
     }


    @Test
    public void extractNumber() {
        Object obj = null;
        Number num = Booking.extractNumber(obj);
        assertNull(num);
        obj = "1234";
        num = Booking.extractNumber(obj);
        assertEquals(1234.0, num);
        obj = 123456789;
        num = Booking.extractNumber(obj);
        assertEquals(1.23456789E8, num);
        obj = -123456789;
        num = Booking.extractNumber(obj);
        assertEquals(-1.23456789E8, num);
        obj = Double.MAX_VALUE + 1;
        num = Booking.extractNumber(obj);
        assertEquals(Double.MAX_VALUE + 1, num);
        obj = Double.MIN_VALUE - 1;
        num = Booking.extractNumber(obj);
        assertEquals(Double.MIN_VALUE - 1, num);
        obj = "12345678901234567890123456789012345678901234567890";
        num = Booking.extractNumber(obj);
        assertEquals(1.2345678901234567E49, num);
    }

    @Test
    public void buildBookingMap() {

        List<Range> ranges = new ArrayList<>();
        Range r1 = new Range("2018-01-05", "2018-01-05", "0", "23");
        ranges.add(r1);

        Booking bk1 = new Booking(new TargetingChannel(), ranges, 0, "", 0);
        List<Booking> bks1 = new ArrayList<>();
        bks1.add(bk1);
        assertEquals(1, Booking.buildBookingMap(bks1).size(), 0);

        TargetingChannel tc2_1 = new TargetingChannel();
        tc2_1.setG(Arrays.asList("g_m"));
        Booking bk2_1 = new Booking(tc2_1, ranges, 0, "advid2001", 0);
        List<Booking> bks2_1 = new ArrayList<>();
        bks2_1.add(bk1);
        bks2_1.add(bk2_1);
        assertEquals(2, Booking.buildBookingMap(bks2_1).size(), 0);

        TargetingChannel tc2_2 = new TargetingChannel();
        tc2_2.setA(Arrays.asList("3"));
        Booking bk2_2 = new Booking(tc2_2, ranges, 0, "advid2002", 0);
        List<Booking> bks2_2 = new ArrayList<>();
        bks2_1.add(bk2_2);
        assertEquals(0, Booking.buildBookingMap(bks2_2).size(), 0);

        TargetingChannel tc3 = new TargetingChannel();
        tc3.setG(Arrays.asList("g_m"));
        tc3.setA(Arrays.asList("3"));
        Booking bk3 = new Booking(tc3, ranges, 0, "advid300", 0);
        List<Booking> bks3 = new ArrayList<>();
        bks3.add(bk3);
        assertEquals(1, Booking.buildBookingMap(bks3).size(), 0);
    }

    @Test
    public void extractBookingIds() {
        TargetingChannel tc1 = new TargetingChannel();

        List<Range> ranges = new ArrayList<>();
        Range r1 = new Range("2018-01-05", "2018-01-05", "0", "23");
        ranges.add(r1);

        Booking bk1 = new Booking(tc1, ranges, 0, "", 0);
        List<Booking> bks1 = new ArrayList<>();
        bks1.add(bk1);

        assertEquals(1, Booking.extractBookingIds(bks1).size(), 0);
    }

    @Test
    /**
     * Coverage purpose only. Data not verified.
     */
    public void getId() {
        List<Range> ranges = new ArrayList<>();
        Range r1 = new Range("2018-01-05", "2018-01-05", "0", "23");
        ranges.add(r1);

        Booking bk1 = new Booking(new TargetingChannel(), ranges, 0, "", 0);

        assertNull(bk1.getId());
    }

    @Test
    public void getDays() {
        List<Range> ranges = new ArrayList<>();
        Range r1 = new Range("2018-01-05", "2018-01-05", "0", "23");
        ranges.add(r1);

        Booking bk1 = new Booking(new TargetingChannel(), ranges, 0, "", 0);

        List<String> exp = new ArrayList();
        exp.add("20180105");
        assertEquals(exp, bk1.getDays());
    }

    @Test
    /**
     * Coverage purpose only. Data not verified.
     */
    public void getBookingId() {
        List<Range> ranges = new ArrayList<>();
        Range r1 = new Range("2018-01-05", "2018-01-05", "0", "23");
        ranges.add(r1);

        Booking bk1 = new Booking(new TargetingChannel(), ranges, 0, "", 0);

        assertNotNull(bk1.getBookingId());
    }

    @Test
    public void getAdvId() {
        List<Range> ranges = new ArrayList<>();
        Range r1 = new Range("2018-01-05", "2018-01-05", "0", "23");
        ranges.add(r1);

        Booking bk1 = new Booking(new TargetingChannel(), ranges, 0, "testGetAdvId", 0);

        assertEquals("testGetAdvId", bk1.getAdvId());
    }

    @Test
    public void getPrice() {
        List<Range> ranges = new ArrayList<>();
        Range r1 = new Range("2018-01-05", "2018-01-05", "0", "23");
        ranges.add(r1);

        Booking bk1 = new Booking(new TargetingChannel(), ranges, 50, "", 0);

        assertEquals(50D, bk1.getPrice(), 0);
    }

    @Test
    public void getAmount() {
        List<Range> ranges = new ArrayList<>();
        Range r1 = new Range("2018-01-05", "2018-01-05", "0", "23");
        ranges.add(r1);

        Booking bk1 = new Booking(new TargetingChannel(), ranges, 0, "", 60);

        assertEquals(60D, bk1.getAmount(), 0);
    }

    @Test
    public void getQuery() {
        TargetingChannel tc = new TargetingChannel();
        List<Range> ranges = new ArrayList<>();
        Range r1 = new Range("2018-01-05", "2018-01-05", "0", "23");
        ranges.add(r1);

        Booking bk1 = new Booking(tc, ranges, 10, "", 0);

        assertTrue(bk1.getQuery().equals(tc));
    }
}