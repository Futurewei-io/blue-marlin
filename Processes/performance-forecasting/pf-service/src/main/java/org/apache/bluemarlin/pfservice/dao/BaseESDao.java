/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0.html
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.bluemarlin.pfservice.dao;

import org.apache.bluemarlin.pfservice.model.Traffic;
import org.apache.bluemarlin.pfservice.util.BMLogger;
import org.apache.bluemarlin.pfservice.util.CommonUtil;
import org.apache.commons.codec.Charsets;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchPhraseQueryBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;

@Component
public class BaseESDao
{
    public static final String ES_TRAFFIC_DOC_PREFIX = "traffic.";
    public static final String ES_KEYWORD_DOC_PREFIX = "kws";
    public static final String ES_KEYWORD_B_PREFIX = "b";

    protected static final BMLogger LOGGER = BMLogger.instance();

    @Autowired
    ResourceLoader resourceLoader;

    protected String getResourceContent(String resourcePath) throws IOException
    {
        File _file = resourceLoader.getResource(resourcePath).getFile();
        String fileContents = "";

        List<String> file = java.nio.file.Files.readAllLines(Paths.get(_file.getAbsolutePath()), Charsets.UTF_8);
        for (String s : file)
        {
            fileContents += s.trim();
        }

        return fileContents;
    }

    protected BoolQueryBuilder createBaseQueryForSingleValues(Traffic traffic)
    {
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();

        boolQueryBuilder.must(new MatchAllQueryBuilder());

        append(boolQueryBuilder, traffic.getT(), ES_TRAFFIC_DOC_PREFIX + "t");
        append(boolQueryBuilder, traffic.getSi(), ES_TRAFFIC_DOC_PREFIX + "si");
        append(boolQueryBuilder, traffic.getM(), ES_TRAFFIC_DOC_PREFIX + "m");
        append(boolQueryBuilder, traffic.getA(), ES_TRAFFIC_DOC_PREFIX + "a");
        append(boolQueryBuilder, traffic.getG(), ES_TRAFFIC_DOC_PREFIX + "g");
        return boolQueryBuilder;
    }

    /**
     * This method adds an attribute with its possible values to a query as Must operand.
     *
     * @param boolQueryBuilder : query that new attribute appends to
     * @param list             : list of values
     * @param field            : attribute name
     */
    protected void append(BoolQueryBuilder boolQueryBuilder, List<String> list, String field)
    {
        appendBase(boolQueryBuilder, list, null, field);
    }


    /**
     * This method adds an attribute with its possible values to a query as Must or notMust operand.
     * <p>
     * If the values are in List then the operand is Must.
     * If the values are in noList then the operand is notMust.
     *
     * @param boolQueryBuilder
     * @param list
     * @param notList
     * @param field
     */
    protected void append(BoolQueryBuilder boolQueryBuilder, java.util.List<String> list, List<String> notList, String field)
    {
        appendBase(boolQueryBuilder, list, notList, field);
    }

    /**
     * This method adds an attribute with its possible values to a query as Must or notMust operand.
     * <p>
     * If the values are in List then the operand is Must.
     * If the values are in noList then the operand is notMust.
     *
     * @param boolQueryBuilder
     * @param list
     * @param notList
     * @param field
     */
    protected void appendBase(BoolQueryBuilder boolQueryBuilder, List<String> list, List<String> notList, String field)
    {
        //single choice in inapp: pdas or exclude_pdas, installed or uninstalled
        if (!CommonUtil.isEmpty(list) || !CommonUtil.isEmpty(notList))
        {
            BoolQueryBuilder query = new BoolQueryBuilder();
            if (list != null) //deal with pdas/installed
            {
                for (String item : list)
                {
                    MatchPhraseQueryBuilder _query = new MatchPhraseQueryBuilder(field, item);
                    query = query.should(_query);
                }
                boolQueryBuilder.must(query);
            }

            if (notList != null) //deal with exclude_pdas/uninstalled
            {
                for (String item : notList)
                {
                    MatchPhraseQueryBuilder _query = new MatchPhraseQueryBuilder(field, item);
                    query = query.should(_query);
                }
                boolQueryBuilder.mustNot(query);
            }
        }
    }
}
