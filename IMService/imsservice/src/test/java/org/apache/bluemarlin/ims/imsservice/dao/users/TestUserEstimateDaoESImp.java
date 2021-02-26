package org.apache.bluemarlin.ims.imsservice.dao.users;

import org.apache.bluemarlin.ims.imsservice.dao.booking.TestBookingDaoESImp;
import org.apache.bluemarlin.ims.imsservice.esclient.ESClient;
import org.apache.bluemarlin.ims.imsservice.esclient.ESRichClientImp;
import org.apache.bluemarlin.ims.imsservice.model.TargetingChannel;
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

import static org.junit.Assert.assertNotNull;

public class TestUserEstimateDaoESImp {

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
    /**
     * Coverage purpose only. Data not verified.
     */
    public void getUserCount() throws IOException {
        UserEstimateDaoESImp usrEstDaoImp = new UserEstimateDaoESImp(DEF_PROP);
        usrEstDaoImp.setESClient(DEF_ESCLI);
        List<String> days = new ArrayList();
        days.add("2018-11-07");
        days.add("2018-01-05");

        long res = usrEstDaoImp.getUserCount(DEF_TC, days, 1.0);
        assertNotNull(res);

        res = usrEstDaoImp.getUserCount(new TargetingChannel(), new ArrayList(), 1D);
        assertNotNull(res);
    }
}