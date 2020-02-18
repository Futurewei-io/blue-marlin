package com.bluemarlin.ims.imsservice.dao.tbr;

import com.bluemarlin.ims.imsservice.dao.booking.TestBookingDaoESImp;
import com.bluemarlin.ims.imsservice.esclient.ESClient;
import com.bluemarlin.ims.imsservice.esclient.ESRichClientImp;
import com.bluemarlin.ims.imsservice.exceptions.ESConnectionException;
import com.bluemarlin.ims.imsservice.model.TargetingChannel;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class TestTBRDaoESImp {

    static InputStream DEF_INPUT = TestBookingDaoESImp.class.getClassLoader().getResourceAsStream("db-test.properties");
    static Properties DEF_PROP;
    static RestHighLevelClient DEF_RHLCLI;
    static ESClient DEF_ESCLI;
    static TargetingChannel DEF_TC;

    static {
        DEF_PROP = new Properties();
        try {
            DEF_PROP.load(DEF_INPUT);
        } catch (IOException e) {
            e.printStackTrace();
        }

        DEF_RHLCLI = new RestHighLevelClient(RestClient.builder(
                new HttpHost(DEF_PROP.getProperty("db.host.urls"),
                        Integer.parseInt(DEF_PROP.getProperty("db.host.ports")), "http")));

        DEF_ESCLI = new ESRichClientImp(DEF_RHLCLI);

        DEF_TC = new TargetingChannel();
        DEF_TC.setG(Arrays.asList("g_m"));
        DEF_TC.setA(Arrays.asList("2"));
    }


    @Test
    public void getTBRRatio() throws ESConnectionException {
        TBRDaoESImp tbrDaoImp = new TBRDaoESImp(DEF_PROP);
        tbrDaoImp.setESClient(DEF_ESCLI);
        double res = tbrDaoImp.getTBRRatio(DEF_TC);
        assertEquals(res, 1D, 0);
        TargetingChannel tc = new TargetingChannel();
        tc.setAus(Arrays.asList("123"));
        res = tbrDaoImp.getTBRRatio(tc);
        assertEquals(res, 0D, 0);
    }

    @Test
    public void getTBRRatio_PI_BNs_DOT_qBAR() throws IOException {
        TBRDaoESImp tbrDaoImp = new TBRDaoESImp(DEF_PROP);
        tbrDaoImp.setESClient(DEF_ESCLI);
        List<TargetingChannel> bns = new ArrayList();
        TargetingChannel bn1 = new TargetingChannel(), bn2 = new TargetingChannel(), bn3 = new TargetingChannel();
        bn1.setA(Arrays.asList("3"));
        bn2.setG(Arrays.asList("g_f"));
        bn3.setR(Arrays.asList("456"));
        bns.add(bn1);
        bns.add(bn2);
        bns.add(bn3);
        double res = tbrDaoImp.getTBRRatio_PI_BNs_DOT_qBAR(bns, DEF_TC);
        assertEquals(1D, res, 0);
    }

    @Test
    public void getTBRRatio_PI_TCs() throws IOException {
        TBRDaoESImp tbrDaoImp = new TBRDaoESImp(DEF_PROP);
        tbrDaoImp.setESClient(DEF_ESCLI);
        List<TargetingChannel> tcs = new ArrayList();
        TargetingChannel tc1 = new TargetingChannel(), tc2 = new TargetingChannel(), tc3 = new TargetingChannel();
        tc1.setA(Arrays.asList("2"));
        tc1.setG(Arrays.asList("g_m"));
        tc2.setA(Arrays.asList("3"));
        tc3.setR(Arrays.asList("456"));
        tcs.add(tc1);
        tcs.add(tc2);
        tcs.add(tc3);
        double res = tbrDaoImp.getTBRRatio_PI_TCs(tcs);
        assertEquals(0D, res, 0);
        res = tbrDaoImp.getTBRRatio_PI_TCs(Arrays.asList(new TargetingChannel()));
        assertEquals(1D, res, 0);
        res = tbrDaoImp.getTBRRatio_PI_TCs(new ArrayList());
        assertEquals(0D, res, 0);
    }
}