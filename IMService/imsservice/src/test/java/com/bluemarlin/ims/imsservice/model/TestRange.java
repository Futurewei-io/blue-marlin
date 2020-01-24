package com.bluemarlin.ims.imsservice.model;

import org.junit.Test;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

public class TestRange {

    /*
    List<Range> ranges = new ArrayList<>();
    Range r1 = new Range();
    r1.setSt("2018-01-07");
    r1.setEd("2018-01-07");
    r1.setSh("0");
    r1.setEh("23");
    ranges.add(r1);
    */

    @Test
    public void getLength() throws ParseException {
        Range r1 = new Range();
        r1.setSt("2018-01-07");
        r1.setEd("2018-01-07");
        assertEquals(1, r1.getLength(), 0);
    }

    @Test
    public void testGetLength() {
        String st = "2018-01-07", ed = "2018-01-07";
        assertEquals(1, Range.getLength(st, ed), 0);

        st = "2018-01-07";
        ed = "2018-01-08";
        assertEquals(2, Range.getLength(st, ed), 0);
        assertEquals(0, Range.getLength(ed, st), 0);
    }

    @Test
    public void getSt() {
        Range r1 = new Range();
        assertNull(r1.getSt());

        r1.setSt("2018-01-07");
        assertEquals("2018-01-07", r1.getSt());

        r1.setSt("");
        assertEquals("", r1.getSt());

        r1.setSt("0");
        assertEquals("0", r1.getSt());
    }

    @Test
    public void setSt() {
        Range r1 = new Range();
        assertNull(r1.getSt());

        r1.setSt("2018-01-07");
        assertEquals("2018-01-07", r1.getSt());

        r1.setSt("");
        assertEquals("", r1.getSt());

        r1.setSt("0");
        assertEquals("0", r1.getSt());
    }

    @Test
    public void getEd() {
        Range r1 = new Range();
        assertNull(r1.getEd());

        r1.setEd("2018-01-07");
        assertEquals("2018-01-07", r1.getEd());

        r1.setEd("");
        assertEquals("", r1.getEd());

        r1.setEd("0");
        assertEquals("0", r1.getEd());
    }

    @Test
    public void setEd() {
        Range r1 = new Range();
        assertNull(r1.getEd());

        r1.setEd("2018-01-07");
        assertEquals("2018-01-07", r1.getEd());

        r1.setEd("");
        assertEquals("", r1.getEd());

        r1.setEd("0");
        assertEquals("0", r1.getEd());
    }

    @Test
    public void getSh() {
        Range r1 = new Range();
        assertNull(r1.getSh());

        r1.setSh("0");
        ;
        assertEquals("0", r1.getSh());

        r1.setSh("");
        assertEquals("", r1.getSh());

        r1.setSh("2018-01-07");
        assertEquals("2018-01-07", r1.getSh());
    }

    @Test
    public void setSh() {
        Range r1 = new Range();
        assertNull(r1.getSh());

        r1.setSh("0");
        ;
        assertEquals("0", r1.getSh());

        r1.setSh("");
        assertEquals("", r1.getSh());

        r1.setSh("2018-01-07");
        assertEquals("2018-01-07", r1.getSh());
    }

    @Test
    public void getEh() {
        Range r1 = new Range();
        assertNull(r1.getEh());

        r1.setEh("0");
        ;
        assertEquals("0", r1.getEh());

        r1.setEh("");
        assertEquals("", r1.getEh());

        r1.setEh("2018-01-07");
        assertEquals("2018-01-07", r1.getEh());
    }

    @Test
    public void setEh() {
        Range r1 = new Range();
        assertNull(r1.getEh());

        r1.setEh("0");

        assertEquals("0", r1.getEh());

        r1.setEh("");
        assertEquals("", r1.getEh());

        r1.setEh("2018-01-07");
        assertEquals("2018-01-07", r1.getEh());
    }

    @Test
    public void normalize() {
        List<Range> ranges = new ArrayList<>();
        Range r1 = new Range();
        r1.setSt("2018-01-07");
        r1.setEd("2018-01-07");
        r1.setSh("0");
        r1.setEh("23");

        Range r2 = new Range();
        r2.setSt("2018-01-09");
        r2.setEd("2018-01-09");
        r2.setSh("5");
        r2.setEh("22");

        Range r3 = new Range();
        r3.setSt("2018-11-05");
        r3.setEd("2018-11-05");
        r3.setSh("7");
        r3.setEh("11");

        ranges.add(r1);
        ranges.add(r2);
        ranges.add(r3);

        List<Range> res = Range.normalize(ranges);

        assertEquals(ranges.toString(), res.toString());
    }

    @Test
    public void getDays() {
        List<Range> ranges = new ArrayList<>();
        Range r1 = new Range();
        r1.setSt("2018-01-07");
        r1.setEd("2018-01-07");
        r1.setSh("0");
        r1.setEh("23");

        Range r2 = new Range();
        r2.setSt("2018-01-09");
        r2.setEd("2018-01-09");
        r2.setSh("5");
        r2.setEh("22");

        Range r3 = new Range();
        r3.setSt("2018-11-05");
        r3.setEd("2018-11-05");
        r3.setSh("7");
        r3.setEh("11");

        ranges.add(r1);
        ranges.add(r2);
        ranges.add(r3);
        List<String> res = Range.getDays(ranges);

        List<String> exp = new ArrayList();
        exp.add("2018-01-07");
        exp.add("2018-01-09");
        exp.add("2018-11-05");

        assertEquals(exp.toString(), res.toString());
    }

    @Test
    public void testToString() {
        Range r1 = new Range();
        r1.setSt("2018-01-07");
        r1.setEd("2018-01-07");
        r1.setSh("0");
        r1.setEh("23");

        String exp = "Range{st='2018-01-07,ed='2018-01-07,sh=0,eh=23}";
        assertEquals(exp, r1.toString());
        exp = "Range{st='null,ed='null,sh=null,eh=null}";
        assertEquals(exp, new Range().toString());
    }
}