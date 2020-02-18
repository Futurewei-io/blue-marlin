package com.bluemarlin.ims.imsservice.ml;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class TestIMSLinearRegression {

    @Test
    public void predict() {
        List<Long> input = new ArrayList();
        input.add(123L);
        input.add(456L);
        input.add(1L);
        input.add(10L);
        int num = 2;
        double res = IMSLinearRegression.predict(input, num);
        assertEquals(789D, res, 0);
    }

    @Test
    public void testPredict() {
        double[] input = new double[]{1.1, 2.2, 3.3, 4.4, 500};
        double[] input2 = new double[]{1, 20, 300, 4000, 50000};
        double res = IMSLinearRegression.predict(input);
        double res2 = IMSLinearRegression.predict(input2);
        assertEquals(402.2, res, 0.1D);
        assertEquals(42057.6D, res2, 0.1D);
    }

    @Test
    public void average() {
        List<Long> input = new ArrayList();
        input.add(123L);
        input.add(456L);
        input.add(1L);
        input.add(10L);
        input.add(1000000L);
        int num = 3;
        double res = IMSLinearRegression.average(input, num);
        double exp = 1000011 / 3;
        assertEquals(exp, res, 0);
    }
}