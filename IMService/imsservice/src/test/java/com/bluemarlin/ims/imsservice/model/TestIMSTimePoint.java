package com.bluemarlin.ims.imsservice.model;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class TestIMSTimePoint {

    @Test
    public void testToString() {
        long epTime = Long.valueOf("1574641835000");   //2019-11-25-12:30:35am GMT = 2019-11-24-4:30:35pm PST
        IMSTimePoint epochTime = new IMSTimePoint(epTime);
        assertEquals("2019-11-24-16", epochTime.toString());
    }

    @Test
    public void getDate() {
        long epTime = Long.valueOf("1574641835000");   //2019-11-25-12:30:35am GMT = 2019-11-24-4:30:35pm PST
        IMSTimePoint epochTime = new IMSTimePoint(epTime);
        assertEquals("2019-11-24", epochTime.getDate());
    }

    @Test
    public void setDate() {
        long epTime = Long.valueOf("1574641835000");   //2019-11-25-12:30:35am GMT = 2019-11-24-4:30:35pm PST
        IMSTimePoint epochTime = new IMSTimePoint(epTime);
        assertEquals("2019-11-24", epochTime.getDate());
        epochTime.setDate("2018-11-05");
        assertEquals("2018-11-05", epochTime.getDate());
    }

    @Test
    public void getHourIndex() {
        long epTime = Long.valueOf("1574641835000");   //2019-11-25-12:30:35am GMT = 2019-11-24-4:30:35pm PST
        IMSTimePoint epochTime = new IMSTimePoint(epTime);
        assertEquals(16, epochTime.getHourIndex(), 0);
        epochTime.setHourIndex(3);
        assertEquals(3, epochTime.getHourIndex(), 0);
        epochTime.setHourIndex(33);
        assertEquals(33, epochTime.getHourIndex(), 0);
    }

    @Test
    public void setHourIndex() {
        long epTime = Long.valueOf("1574641835000");   //2019-11-25-12:30:35am GMT = 2019-11-24-4:30:35pm PST
        IMSTimePoint epochTime = new IMSTimePoint(epTime);
        assertEquals(16, epochTime.getHourIndex(), 0);
        epochTime.setHourIndex(3);
        assertEquals(3, epochTime.getHourIndex(), 0);
    }

    @Test
    public void incrementHour() {
        long epTime = Long.valueOf("1574641835000");   //2019-11-25-12:30:35am GMT = 2019-11-24-4:30:35pm PST
        IMSTimePoint epochTime = new IMSTimePoint(epTime);
        assertEquals(16, epochTime.getHourIndex(), 0);

        long epTime2 = Long.valueOf("1574645435000");   //2019-11-25-1:30:35pm GMT = 2019-11-24-5:30:35pm PST
        IMSTimePoint epochTime2 = new IMSTimePoint(epTime2);
        assertEquals(17, epochTime2.getHourIndex(), 0);
        epochTime.incrementHour(epochTime2);
        assertEquals(16, epochTime.getHourIndex(), 0);
        assertEquals(18, epochTime2.getHourIndex(), 0);

        epochTime2.setHourIndex(33);
        assertEquals(33, epochTime2.getHourIndex(), 0);
        epochTime.incrementHour(epochTime2);
        assertEquals(16, epochTime.getHourIndex(), 0);
        assertEquals(0, epochTime2.getHourIndex(), 0);

    }

    @Test
    public void incrementDay() {
        long epTime = Long.valueOf("1574641835000");   //2019-11-25-12:30:35am GMT = 2019-11-24-4:30:35pm PST
        IMSTimePoint epochTime = new IMSTimePoint(epTime);
        assertEquals("2019-11-24", epochTime.getDate());
        long epTime2 = Long.valueOf("1574645435000");   //2019-11-25-1:30:35pm GMT = 2019-11-24-5:30:35pm PST
        IMSTimePoint epochTime2 = new IMSTimePoint(epTime2);
        assertEquals("2019-11-24", epochTime2.getDate());

        IMSTimePoint.incrementDay(epochTime);
        IMSTimePoint.incrementDay(epochTime2);
        assertEquals("2019-11-25", epochTime.getDate());
        assertEquals("2019-11-25", epochTime2.getDate());

        epochTime2.setDate("2018-11-01");
        assertEquals("2018-11-01", epochTime2.getDate());
        IMSTimePoint.incrementDay(epochTime2);
        assertEquals("2019-11-25", epochTime.getDate());
        assertEquals("2018-11-02", epochTime2.getDate());

    }

    @Test
    public void build() {
        IMSTimePoint epochTime = new IMSTimePoint(0);
        List<Range> ranges = new ArrayList<>();
        Range r1 = new Range();
        r1.setSt("2018-01-05");
        r1.setEd("2018-01-05");
        r1.setSh("0");
        r1.setEh("23");
        Range r2 = new Range();
        r2.setSt("2018-01-06");
        r2.setEd("2018-01-06");
        r2.setSh("0");
        r2.setEh("23");
        Range r3 = new Range();
        r3.setSt("2018-01-07");
        r3.setEd("2018-01-07");
        r3.setSh("0");
        r3.setEh("23");
        ranges.add(r1);
        ranges.add(r2);
        ranges.add(r3);

        long epTime = Long.valueOf("1574641835000");   //2019-11-25-12:30:35am GMT = 2019-11-24-4:30:35pm PST
        IMSTimePoint epochTime2 = new IMSTimePoint(epTime);

        epochTime.build(ranges);
        assertEquals("1969-12-31-16", epochTime.toString());

        epochTime2.build(ranges);
        assertEquals("2019-11-24-16", epochTime2.toString());

    }

    @Test
    /**
     * Coverage purpose only. Data not verified.
     */
    public void testHashCode() {
        long epTime = Long.valueOf("1574641835000");   //2019-11-25-12:30:35am GMT = 2019-11-24-4:30:35pm PST
        IMSTimePoint epochTime = new IMSTimePoint(epTime);
        assertNotNull(epochTime.hashCode());
    }

    @Test
    public void testEquals() {
        long epTime = Long.valueOf("1574641835000");   //2019-11-25-12:30:35am GMT = 2019-11-24-4:30:35pm PST
        IMSTimePoint epochTime = new IMSTimePoint(epTime);
        assertEquals("2019-11-24", epochTime.getDate());
        long epTime2 = Long.valueOf("1574645435000");   //2019-11-25-1:30:35pm GMT = 2019-11-24-5:30:35pm PST
        IMSTimePoint epochTime2 = new IMSTimePoint(epTime2);
        assertEquals("2019-11-24", epochTime2.getDate());

        assertTrue(epochTime.equals(epochTime));
        assertFalse(epochTime.equals(epochTime2));
        assertFalse(new IMSTimePoint(0).equals(null));
        IMSTimePoint epochTime3 = null;
        assertNull(epochTime3);
        epochTime3 = new IMSTimePoint(epTime);
        assertFalse(epochTime3.equals(null));

        epochTime3.setDate("");
        assertFalse(epochTime3.equals(null));
        assertFalse(epochTime3.equals(new IMSTimePoint(0)));

        IMSTimePoint timePoint = new IMSTimePoint(123L);
        assertFalse(timePoint.equals(123L));
        assertFalse(epochTime3.equals(null));
        assertFalse(epochTime3.equals(null));


        timePoint = new IMSTimePoint(0L);
        timePoint.setDate(null);
        IMSTimePoint tp = new IMSTimePoint(0L);
        tp.setDate("2018-11-07");
        assertFalse(timePoint.equals(tp));

    }

    @Test
    public void appendListFromStartToEndHours() {

        List<IMSTimePoint> res = IMSTimePoint.appendListFromStartToEndHours(0, 23, new ArrayList(), new Range());

        List<IMSTimePoint> exp = new ArrayList();
        for (int i = 0; i < 24; i++) {
            IMSTimePoint localTempIMSTimePoint = new IMSTimePoint(0);
            localTempIMSTimePoint.setDate(new Range().getEd());
            localTempIMSTimePoint.setHourIndex(i);
            exp.add(localTempIMSTimePoint);
        }
        assertEquals(exp.toString(), res.toString());

        long epTime = Long.valueOf("1574641835000");   //2019-11-25-12:30:35am GMT = 2019-11-24-4:30:35pm PST
        IMSTimePoint epochTime = new IMSTimePoint(epTime);
        long epTime2 = Long.valueOf("1574645435000");   //2019-11-25-1:30:35pm GMT = 2019-11-24-5:30:35pm PST
        IMSTimePoint epochTime2 = new IMSTimePoint(epTime2);
        List<IMSTimePoint> list = new ArrayList();
        list.add(epochTime);
        list.add(epochTime2);
        Range r1 = new Range();
        r1.setSt("2018-01-05");
        r1.setEd("2018-01-05");
        r1.setSh("0");
        r1.setEh("23");
        List<IMSTimePoint> res2 = IMSTimePoint.appendListFromStartToEndHours(0, 23, list, r1);

        exp = new ArrayList();
        exp.add(epochTime);
        exp.add(epochTime2);
        for (int i = 0; i < 24; i++) {
            IMSTimePoint localTempIMSTimePoint = new IMSTimePoint(0);
            localTempIMSTimePoint.setDate(r1.getEd());
            localTempIMSTimePoint.setHourIndex(i);
            exp.add(localTempIMSTimePoint);
        }

        assertEquals(exp.toString(), res2.toString());
    }

    @Test
    public void getDateTime() {
        long epTime = Long.valueOf("1574641835000");   //2019-11-25-12:30:35am GMT = 2019-11-24-4:30:35pm PST
        IMSTimePoint epochTime = new IMSTimePoint(epTime);
        assertEquals("2019-11-24T16:00", epochTime.getDateTime().toString());
    }

    @Test
    public void compareTo() {
        long epTime = Long.valueOf("1574641835000");   //2019-11-25-12:30:35am GMT = 2019-11-24-4:30:35pm PST
        IMSTimePoint epochTime = new IMSTimePoint(epTime);
        long epTime2 = Long.valueOf("1574645435000");   //2019-11-25-1:30:35pm GMT = 2019-11-24-5:30:35pm PST
        IMSTimePoint epochTime2 = new IMSTimePoint(epTime2);

        assertEquals(-1, epochTime.compareTo(epochTime2), 0);
        assertEquals(1, epochTime2.compareTo(epochTime), 0);
        assertEquals(0, epochTime.compareTo(epochTime), 0);
    }
}