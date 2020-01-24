package com.bluemarlin.ims.imsservice.util;

import com.bluemarlin.ims.imsservice.model.TargetingChannel;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import static org.junit.Assert.*;

public class TestCommonUtil {

    @Test
    public void isEmpty() {
        List list = new ArrayList();
        assertTrue(CommonUtil.isEmpty(list));

        list.add("abc");
        assertFalse(CommonUtil.isEmpty(list));
    }

    @Test
    public void isBlank() {
        String s = null;
        assertTrue(CommonUtil.isBlank(null));
        assertTrue(CommonUtil.isBlank(s));
        s = "abc";
        assertFalse(CommonUtil.isBlank(s));
        s = "";
        assertTrue(CommonUtil.isBlank(s));
    }

    @Test
    public void sanitize() {
        String s = null;
        assertNull(CommonUtil.sanitize(s));

        s = "WAESDasweraq";
        assertEquals("waesdasweraq", CommonUtil.sanitize(s));

        s = "abc";
        assertEquals("abc", CommonUtil.sanitize(s));

        s = " sf - wWA wE_ + SD=14asdsSAD fweraq  aA";
        assertEquals("sf - wwa we_ + sd=14asdssad fweraq  aa", CommonUtil.sanitize(s));
    }

    @Test
    public void dateToDayString() throws ParseException {
        String s = "2018-11-10";
        Date date = new SimpleDateFormat("yyyy-MM-dd").parse(s);
        assertEquals(s, CommonUtil.DateToDayString(date));
    }

    @Test
    public void dayToDate() throws ParseException {
        String s = "201-765-51";
        Date date = new SimpleDateFormat("yyyy-MM-dd").parse(s);
        assertEquals(date, CommonUtil.dayToDate(s));
        s = "2018-01-05";
        date = new SimpleDateFormat("yyyy-MM-dd").parse(s);
        assertEquals(date, CommonUtil.dayToDate(s));
    }

    @Test
    public void convertToString() {
        List list = new ArrayList();
        assertEquals("", CommonUtil.convertToString(list));

        list.add("abc");
        list.add("qowrqpwd");
        assertEquals("abc-qowrqpwd", CommonUtil.convertToString(list));
    }

    @Test
    public void convertToMap() {
        class TestObj {
            @JsonProperty("name")
            String name;
            @JsonProperty("age")
            Integer age;
        }
        TestObj obj = new TestObj();
        obj.name = "abc";
        obj.age = 10;

        Map exp = new HashMap();
        exp.put("name", "abc");
        exp.put("age", 10);

        assertTrue(CommonUtil.convertToMap(obj).equals(exp));
    }

    @Test
    public void removeStringFromItems() {
        List<String> list = new ArrayList();
        list.add("abc");
        list.add("123");
        list.add("987654321");
        list.add("qwer");
        list.add("asdf");
        list.add("ZXCV");
        String rmv = "asdf";

        List<String> exp = new ArrayList();
        exp.add("abc");
        exp.add("123");
        exp.add("987654321");
        exp.add("qwer");
        exp.add("");
        exp.add("ZXCV");
        assertEquals(exp, CommonUtil.removeStringFromItems(list, rmv));

        list.add("++++++");
        exp.add("++++++");
        assertEquals(exp, CommonUtil.removeStringFromItems(list, rmv));
    }

    @Test
    public void determinePriceCatForCPM() {
        double d = 123.456;
        assertEquals(3, (int) CommonUtil.determinePriceCatForCPM(d));
        d = 456789123456D;
        assertEquals(3, (int) CommonUtil.determinePriceCatForCPM(d));
        d = 11.9999999;
        assertEquals(1, (int) CommonUtil.determinePriceCatForCPM(d));
        d = 12.00001;
        assertEquals(2, (int) CommonUtil.determinePriceCatForCPM(d));
        d = 25.00001;
        assertEquals(3, (int) CommonUtil.determinePriceCatForCPM(d));
    }

    @Test
    public void determinePriceCatForCPC() {
        double d = 10.456;
        assertEquals(3, (int) CommonUtil.determinePriceCatForCPC(d), 0);
        d = 005D;
        assertEquals(3, (int) CommonUtil.determinePriceCatForCPC(d), 0);
        d = 0.69999999999999;
        assertEquals(1, (int) CommonUtil.determinePriceCatForCPC(d), 0);
        d = 0.70000001;
        assertEquals(2, (int) CommonUtil.determinePriceCatForCPC(d), 0);
        d = -1D;
        assertEquals(3, (int) CommonUtil.determinePriceCatForCPC(d), 0);
        d = 1.9633331;
        assertEquals(2, (int) CommonUtil.determinePriceCatForCPC(d), 0);
    }

    @Test
    public void determinePriceCatForCPD() {
        double d = 10.456;
        assertEquals(3, (int) CommonUtil.determinePriceCatForCPD(d));
        d = 005D;
        assertEquals(2, (int) CommonUtil.determinePriceCatForCPD(d));
        d = 0.69999999999999;
        assertEquals(1, (int) CommonUtil.determinePriceCatForCPD(d));
        d = 7.123456789;
        assertEquals(3, (int) CommonUtil.determinePriceCatForCPD(d));
        d = -1D;
        assertEquals(3, (int) CommonUtil.determinePriceCatForCPD(d));
        d = 1.9633331;
        assertEquals(1, (int) CommonUtil.determinePriceCatForCPD(d));
    }

    @Test
    public void determinePriceCatForCPT() {
        double d = 10.456;
        assertEquals(1, (int) CommonUtil.determinePriceCatForCPT(d));
        d = 300000D;
        assertEquals(2, (int) CommonUtil.determinePriceCatForCPT(d));
        d = 69999999999999D;
        assertEquals(3, (int) CommonUtil.determinePriceCatForCPT(d));
        d = 123.456789;
        assertEquals(1, (int) CommonUtil.determinePriceCatForCPT(d));
        d = -1D;
        assertEquals(3, (int) CommonUtil.determinePriceCatForCPT(d));

    }

    @Test
    public void determinePriceCat() {
        double p0 = 0.1, p1 = -2;
        TargetingChannel tcp1 = new TargetingChannel();
        tcp1.setPm("CPC");
        assertEquals(1, (int) CommonUtil.determinePriceCat(p0, tcp1.getPm()));
        assertEquals(0, (int) CommonUtil.determinePriceCat(p1, tcp1.getPm()));
        assertEquals(2, (int) CommonUtil.determinePriceCat(0.70001, tcp1.getPm()));
        assertEquals(3, (int) CommonUtil.determinePriceCat(10D, tcp1.getPm()));
        tcp1.setPm("CPM");
        assertEquals(1, (int) CommonUtil.determinePriceCat(p0, tcp1.getPm()));
        assertEquals(2, (int) CommonUtil.determinePriceCat(12.0001, tcp1.getPm()));
        assertEquals(3, (int) CommonUtil.determinePriceCat(25.0001, tcp1.getPm()));
        tcp1.setPm("CPD");
        assertEquals(1, (int) CommonUtil.determinePriceCat(p0, tcp1.getPm()));
        assertEquals(2, (int) CommonUtil.determinePriceCat(5D, tcp1.getPm()));
        assertEquals(3, (int) CommonUtil.determinePriceCat(100D, tcp1.getPm()));
        tcp1.setPm("CPT");
        assertEquals(1, (int) CommonUtil.determinePriceCat(p0, tcp1.getPm()));
        assertEquals(2, (int) CommonUtil.determinePriceCat(300000D, tcp1.getPm()));
        assertEquals(0, (int) CommonUtil.determinePriceCat(p1, tcp1.getPm()));
        assertEquals(3, (int) CommonUtil.determinePriceCat(400005D, tcp1.getPm()));
        tcp1.setPm("NONE");
        assertEquals(3, (int) CommonUtil.determinePriceCat(p0, tcp1.getPm()));
        assertEquals(0, (int) CommonUtil.determinePriceCat(p1, tcp1.getPm()));
    }

    @Test
    public void removeEscapingCharacters() {
        String s = " aasd " + "\n" + "ASDFSf safw" + "\"" + "sfdnsd" + "&gt " + "waofes nasdfw " + "\n";
        assertTrue(CommonUtil.removeEscapingCharacters(s).equals(" aasd ASDFSf safwsfdnsd waofes nasdfw "));
    }

    @Test
    public void equalNumbers() {
        double d1 = 1.0000, d2 = 2.0000;
        assertFalse(CommonUtil.equalNumbers(d1, d2));
        d2 = 1.0001;
        assertTrue(CommonUtil.equalNumbers(d1, d2));
        d2 = 1.001;
        assertTrue(CommonUtil.equalNumbers(d1, d2));
        d2 = 1.01;
        assertFalse(CommonUtil.equalNumbers(d1, d2));
    }
}