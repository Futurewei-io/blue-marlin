package com.bluemarlin.ims.imsservice.esclient;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchResponseSections;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.profile.SearchProfileShardResults;
import org.elasticsearch.search.suggest.Suggest;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;

public class TestESResponse {

    static SearchHit[] DEF_HIT_ARR;
    static SearchHits DEF_HIT;
    static Aggregations DEF_AGGR;
    static List<Suggest.Suggestion<? extends Suggest.Suggestion.Entry<? extends Suggest.Suggestion.Entry.Option>>> DEF_SUG_LIST;
    static Suggest DEF_SUG;
    static SearchProfileShardResults DEF_PROSHRES;
    static SearchResponseSections DEF_RESSEC;
    static ShardSearchFailure[] DEF_SHFAIL;
    static SearchResponse.Clusters DEF_CLUSTER;

    static {
        DEF_HIT_ARR = new SearchHit[6];
        DEF_HIT_ARR[0] = new SearchHit(0);
        DEF_HIT_ARR[1] = new SearchHit(1);
        DEF_HIT_ARR[2] = new SearchHit(2);
        DEF_HIT_ARR[3] = new SearchHit(3);
        DEF_HIT_ARR[4] = new SearchHit(4);
        DEF_HIT_ARR[5] = new SearchHit(5);
        DEF_HIT = new SearchHits(DEF_HIT_ARR, 30000, 450);

        DEF_AGGR = new Aggregations(new ArrayList());

        DEF_SUG_LIST = new ArrayList();
        DEF_SUG_LIST.add(new Suggest.Suggestion("sug1", 1));
        DEF_SUG_LIST.add(new Suggest.Suggestion("sug2", 2));
        DEF_SUG_LIST.add(new Suggest.Suggestion("sug3", 30));
        DEF_SUG_LIST.add(new Suggest.Suggestion("sug4", 400));
        DEF_SUG_LIST.add(new Suggest.Suggestion("sug5", 5));
        DEF_SUG = new Suggest(DEF_SUG_LIST);

        DEF_PROSHRES = new SearchProfileShardResults(Collections.emptyMap());

        DEF_RESSEC = new SearchResponseSections(DEF_HIT, DEF_AGGR, DEF_SUG, false, false, DEF_PROSHRES,  0);
        DEF_SHFAIL = new ShardSearchFailure[0];
        DEF_CLUSTER = SearchResponse.Clusters.EMPTY;
    }

    @Test
    public void getSourceMap() {
        SearchResponse srchResp = new SearchResponse(DEF_RESSEC, "id1", 1, 1, 1, 1, DEF_SHFAIL, DEF_CLUSTER);
        ESResponse esResp = new ESResponse(srchResp, Collections.emptyMap());
        Map<String, Object> res = esResp.getSourceMap();
        assertTrue(res.isEmpty());

        Map<String, Object> srMap = new HashMap();
        srMap.put("srMap_k1", "srMap_v1");
        List<Double> list1 = new ArrayList();
        list1.add(1.0);
        list1.add(0.0);
        srMap.put("srMap_k2", list1);
        esResp = new ESResponse(srchResp, srMap);
        res = esResp.getSourceMap();

        assertEquals(2, res.size());
        assertEquals("srMap_v1", res.get("srMap_k1"));
        assertEquals(list1.toString(), res.get("srMap_k2").toString());
        assertNull(res.get("srMap_k3"));
    }

    @Test
    public void getSearchHits() {
        SearchResponse srchResp = new SearchResponse(DEF_RESSEC, "id1", 1, 1, 1, 1, DEF_SHFAIL, DEF_CLUSTER);
        ESResponse esResp = new ESResponse(srchResp, Collections.emptyMap());
        SearchHits res = esResp.getSearchHits();
        assertNotNull(res);
        assertEquals(30000, res.getTotalHits());
    }

    @Test
    public void getSearchResponse() {
        SearchResponse srchResp = new SearchResponse(DEF_RESSEC, "id1", 1, 1, 1, 1, DEF_SHFAIL, DEF_CLUSTER);
        ESResponse esResp = new ESResponse(srchResp, Collections.emptyMap());
        SearchResponse res = esResp.getSearchResponse();
        assertNotNull(res);
        assertEquals(30000, res.getHits().getTotalHits());
    }

    @Test
    /**
     * Coverage purpose only. Data not verified.
     */
    public void getUpdateResponse() {
        SearchResponse srchResp = new SearchResponse(DEF_RESSEC, "id1", 1, 1, 1, 1, DEF_SHFAIL, DEF_CLUSTER);
        ESResponse esResp = new ESResponse(srchResp, Collections.emptyMap());
        UpdateResponse res = esResp.getUpdateResponse();
        assertNull(res);
    }

    @Test
    /**
     * Coverage purpose only. Data not verified.
     */
    public void testUpdateResponse() {
        Index idx = new Index("idxName", "idxUuid");
        ShardId shardId = new ShardId(idx, 0);
        UpdateResponse updRes = new UpdateResponse(shardId, "doc", "id", 1L, DocWriteResponse.Result.NOT_FOUND);
        ESResponse esResp = new ESResponse(updRes);
        assertNotNull(esResp);
    }
}