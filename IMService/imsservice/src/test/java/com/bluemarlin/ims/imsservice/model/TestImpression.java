package com.bluemarlin.ims.imsservice.model;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

public class TestImpression {

    @Test
    public void getTotal() {
        H0 h0 = new H0();
        h0.setH("00");
        h0.setT((long) 0);
        H1 h1 = new H1();
        h1.setH("01");
        h1.setT((long) 1);
        H2 h2 = new H2();
        h2.setH("02");
        h2.setT((long) 2);
        H3 h3 = new H3();
        h3.setH("03");
        h3.setT((long) 3);
        Impression imp = new Impression(h0, h1, h2, h3);
        assertEquals(6, imp.getTotal(), 0);
        Impression imp2 = new Impression();
        assertEquals(0, imp2.getTotal(), 0);
    }

    @Test
    public void getValueForHIndex() {
        H0 h0 = new H0();
        h0.setH("00");
        h0.setT((long) 0);
        H1 h1 = new H1();
        h1.setH("11");
        h1.setT((long) 1);
        H2 h2 = new H2();
        h2.setH("22");
        h2.setT((long) 2);
        H3 h3 = new H3();
        h3.setH("33");
        h3.setT((long) 3);
        Impression imp = new Impression(h0, h1, h2, h3);

        assertEquals(0D, imp.getValueForHIndex(0), 0);
        assertEquals(1D, imp.getValueForHIndex(1), 0);
        assertEquals(2D, imp.getValueForHIndex(2), 0);
        assertEquals(3D, imp.getValueForHIndex(3), 0);
        assertEquals(0D, imp.getValueForHIndex(4), 0);
        assertEquals(0D, imp.getValueForHIndex(-4), 0);
        assertEquals(0D, imp.getValueForHIndex(Integer.MAX_VALUE + 1), 0);
        assertEquals(0D, imp.getValueForHIndex(Integer.MIN_VALUE - 1), 0);
    }

    @Test
    /**
     * Coverage purpose only. Data not verified.
     */
    public void countImpressions() {
        H0 h0 = new H0();
        h0.setH("00");
        h0.setT((long) 0);
        H1 h1 = new H1();
        h1.setH("11");
        h1.setT((long) 1);
        H2 h2 = new H2();
        h2.setH("22");
        h2.setT((long) 2);
        H3 h3 = new H3();
        h3.setH("33");
        h3.setT((long) 3);
        Impression imp = new Impression(h0, h1, h2, h3);
        double price = 100D;
        TargetingChannel tc = new TargetingChannel();
        tc.setPm("CPC");
        long res = imp.countImpressions(price, tc.getPm());
        assertNotNull(res);
        assertEquals(6, res, Double.MAX_VALUE);
        price = Long.MAX_VALUE;
        res = imp.countImpressions(price, tc.getPm());
        assertNotNull(res);
        assertEquals(6, res, Double.MAX_VALUE);
    }

    @Test
    public void getH1() {
        H1 h1 = new H1();
        h1.setH("11");
        h1.setT((long) 1);
        Impression imp = new Impression(new H0(), h1, new H2(), new H3());

        assertEquals(h1, imp.getH1());
    }

    @Test
    public void setH1() {
        H1 h1 = new H1();
        h1.setH("11");
        h1.setT((long) 1);
        Impression imp = new Impression(new H0(), h1, new H2(), new H3());
        assertEquals(h1, imp.getH1());
        H1 h12 = new H1();
        h12.setH("121");
        h12.setT((long) 21);
        imp.setH1(h12);
        assertEquals(h12, imp.getH1());
    }

    @Test
    public void getH0() {
        H0 h0 = new H0();
        h0.setH("00");
        h0.setT((long) 0);
        Impression imp = new Impression(h0, new H1(), new H2(), new H3());
        assertEquals(h0, imp.getH0());
    }

    @Test
    public void setH0() {
        H0 h0 = new H0();
        h0.setH("00");
        h0.setT((long) 0);
        Impression imp = new Impression(h0, new H1(), new H2(), new H3());
        assertEquals(h0, imp.getH0());
        H0 h02 = new H0();
        h02.setH("020");
        h02.setT((long) 20);
        imp.setH0(h02);
        assertEquals(h02, imp.getH0());
    }

    @Test
    public void getH2() {
        H2 h2 = new H2();
        h2.setH("22");
        h2.setT((long) 2);
        Impression imp = new Impression(new H0(), new H1(), h2, new H3());
        assertEquals(h2, imp.getH2());
    }

    @Test
    public void setH2() {
        H2 h2 = new H2();
        h2.setH("22");
        h2.setT((long) 2);
        Impression imp = new Impression(new H0(), new H1(), h2, new H3());
        assertEquals(h2, imp.getH2());
        H2 h22 = new H2();
        h22.setH("222");
        h22.setT((long) 22);
        imp.setH2(h22);
        assertEquals(h22, imp.getH2());
    }

    @Test
    public void getH3() {
        H3 h3 = new H3();
        h3.setH("33");
        h3.setT((long) 3);
        Impression imp = new Impression(new H0(), new H1(), new H2(), h3);
        assertEquals(h3, imp.getH3());
    }

    @Test
    public void setH3() {
        H3 h3 = new H3();
        h3.setH("33");
        h3.setT((long) 3);
        Impression imp = new Impression(new H0(), new H1(), new H2(), h3);
        assertEquals(h3, imp.getH3());
        H3 h32 = new H3();
        h32.setH("323");
        h32.setT((long) 23);
        imp.setH3(h32);
        assertEquals(h32, imp.getH3());
    }

    @Test
    /**
     * Coverage purpose only. Data not verified.
     */
    public void getH_index() {
        H0 h0 = new H0();
        h0.setH("00");
        h0.setT((long) 0);
        H1 h1 = new H1();
        h1.setH("11");
        h1.setT((long) 1);
        H2 h2 = new H2();
        h2.setH("22");
        h2.setT((long) 2);
        H3 h3 = new H3();
        h3.setH("33");
        h3.setT((long) 3);
        Impression imp = new Impression(h0, h1, h2, h3);
        assertNull(imp.getH_index());
    }

    @Test
    public void setH_index() {
        H0 h0 = new H0();
        h0.setH("00");
        h0.setT((long) 0);
        H1 h1 = new H1();
        h1.setH("11");
        h1.setT((long) 1);
        H2 h2 = new H2();
        h2.setH("22");
        h2.setT((long) 2);
        H3 h3 = new H3();
        h3.setH("33");
        h3.setT((long) 3);
        Impression imp = new Impression(h0, h1, h2, h3);
        assertNull(imp.getH_index());
        imp.setH_index(99);
        assertEquals(99, imp.getH_index(), 0);
    }

    @Test
    public void adjustPositive() {
        H0 h0 = new H0();
        h0.setH("-00");
        h0.setT((long) -0);
        H1 h1 = new H1();
        h1.setH("-11");
        h1.setT((long) -1);
        H2 h2 = new H2();
        h2.setH("-22");
        h2.setT((long) -2);
        H3 h3 = new H3();
        h3.setH("-33");
        h3.setT((long) -3);
        Impression imp = new Impression(h0, h1, h2, h3);

        List<Double> exp = new ArrayList();
        exp.add(0D);
        exp.add(-1D);
        exp.add(-2D);
        exp.add(-3D);
        assertEquals(exp.toString(), imp.getHs().toString());
        imp.adjustPositive();
        exp = new ArrayList();
        for (int i = 0; i < 4; i++) exp.add(0D);
        assertEquals(exp.toString(), imp.getHs().toString());
    }

    @Test
    public void getHs() {
        H0 h0 = new H0();
        h0.setH("00");
        h0.setT((long) 0);
        H1 h1 = new H1();
        h1.setH("11");
        h1.setT((long) 1);
        H2 h2 = new H2();
        h2.setH("22");
        h2.setT((long) 2);
        H3 h3 = new H3();
        h3.setH("33");
        h3.setT((long) 3);
        Impression imp = new Impression(h0, h1, h2, h3);
        List<Double> exp = new ArrayList();
        exp.add(0D);
        exp.add(1D);
        exp.add(2D);
        exp.add(3D);
        assertEquals(exp.toString(), imp.getHs().toString());
    }

    @Test
    public void add() {
        Double[] dayCnt = new Double[]{1D, 2D, 3D, 4D};
        Impression i1 = new Impression(dayCnt);
        double[] dayCnt2 = new double[]{10D, 11D, 12D, 13D};
        Impression i2 = new Impression(dayCnt2);
        Impression res = Impression.add(i1, i2);

        Double[] expCnt = new Double[]{11D, 13D, 15D, 17D};
        Impression exp = new Impression(expCnt);

        assertEquals(exp.toString(), res.toString());
    }

    @Test
    public void subtract() {
        Double[] dayCnt = new Double[]{1D, 2D, 3D, 4D};
        Impression i1 = new Impression(dayCnt);
        double[] dayCnt2 = new double[]{10D, 11D, 12D, 13D};
        Impression i2 = new Impression(dayCnt2);
        Impression res = Impression.subtract(i1, i2);

        Double[] expCnt = new Double[]{-9D, -9D, -9D, -9D};
        Impression exp = new Impression(expCnt);

        assertEquals(exp.toString(), res.toString());
    }

    @Test
    public void multiply() {
        Double[] dayCnt = new Double[]{1.1, 2.2, 3.3, 5.5};
        Impression i1 = new Impression(dayCnt);

        Impression res = Impression.multiply(i1, 10);

        double d1 = (double) Math.round(Double.parseDouble("1.1"));
        double d2 = (double) Math.round(Double.parseDouble("2.2"));
        double d3 = (double) Math.round(Double.parseDouble("3.3"));
        double d4 = (double) Math.round(Double.parseDouble("5.5"));
        Double[] expCnt = new Double[]{d1 * 10, d2 * 10, d3 * 10, d4 * 10};
        Impression exp = new Impression(expCnt);

        assertEquals(exp.toString(), res.toString());
    }

    @Test
    public void getCountByPriceCategory() {
        Double[] dayCnt = new Double[]{1.1, 2.2, 3.3, 4.4};
        Impression i1 = new Impression(dayCnt);
        assertEquals(1, i1.getCountByPriceCategory(0), 0);
        assertEquals(3, i1.getCountByPriceCategory(1), 0);
        assertEquals(6, i1.getCountByPriceCategory(2), 0);
        assertEquals(10, i1.getCountByPriceCategory(3), 0);
        assertEquals(0, i1.getCountByPriceCategory(4), 0);
    }

    @Test
    public void getIndividualHistogramCountByPriceCategory() {
        Double[] dayCnt = new Double[]{1.1, 2.2, 3.3, 4.4};
        Impression i1 = new Impression(dayCnt);

        assertEquals(1, i1.getIndividualHistogramCountByPriceCategory(0), 0);
        assertEquals(2, i1.getIndividualHistogramCountByPriceCategory(1), 0);
        assertEquals(3, i1.getIndividualHistogramCountByPriceCategory(2), 0);
        assertEquals(4, i1.getIndividualHistogramCountByPriceCategory(3), 0);
        assertEquals(0, i1.getIndividualHistogramCountByPriceCategory(4), 0);
    }

    @Test
    public void adjustValue() {
        Double[] dayCnt = new Double[]{1.1, 2.2, 3.3, 4.4};
        Impression i1 = new Impression(dayCnt);
        assertEquals(10, i1.getCountByPriceCategory(3), 0);
        i1.adjustValue(0, 55);
        assertEquals(64, i1.getCountByPriceCategory(3), 0);
    }

    @Test
    /**
     * Coverage purpose only. Data not verified.
     */
    public void testToString() {
        Double[] dayCnt = new Double[]{1.1, 2.2, 3.3, 4.4};
        Impression i1 = new Impression(dayCnt);
        String exp = "{{h:1,t:h0}{h:2,t:h1}{h:3,t:h2}{h:4,t:h3}}";
        assertEquals(exp, i1.toString());

    }
}