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

package org.apache.bluemarlin.pfservice.config;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;

@Configuration
@PropertySource("file:src/main/resources/application.properties")
public class SystemConfig
{
    private static int ZK_CONNECTION_TIMEOUT = 60000;
    private final CountDownLatch connectedSignal = new CountDownLatch(1);
    private ZooKeeper zk;

    @Value("${zk_host}")
    private String zk_host;

    @Value("${zk_gucdoc_es_host}")
    private String zk_gucdoc_es_host;

    @Value("${zk_gucdoc_es_port}")
    private String zk_gucdoc_es_port;

    @Value("${zk_gucdoc_es_index}")
    private String zk_gucdoc_es_index;

    @Value("${zk_pf_services_graph_size}")
    private String zk_pf_services_graph_size;

    @Value("${thread_pool_size}")
    private int thread_pool_size;

    private String din_ucdoc_es_host;
    private String din_ucdoc_es_port;
    private String din_ucdoc_es_index;
    private int pf_services_graph_size;

    public String getDinUCDocESHost()
    {
        return din_ucdoc_es_host;
    }

    public String getDinUCDocESPort()
    {
        return din_ucdoc_es_port;
    }

    public String getDinUCDocESIndex()
    {
        return din_ucdoc_es_index;
    }

    public int getGraphSize()
    {
        return pf_services_graph_size;
    }

    public int getThreadPoolSize()
    {
        return thread_pool_size;
    }

    @Bean
    @Qualifier("SystemConfig")
    public SystemConfig build() throws IOException, KeeperException, InterruptedException
    {

        this.zk = new ZooKeeper(zk_host, ZK_CONNECTION_TIMEOUT, new Watcher()
        {
            public void process(WatchedEvent we)
            {
                if (we.getState() == Event.KeeperState.SyncConnected)
                {
                    connectedSignal.countDown();
                }
            }
        });

        connectedSignal.await();

        this.din_ucdoc_es_host = getZkData(zk_gucdoc_es_host);
        this.din_ucdoc_es_port = getZkData(zk_gucdoc_es_port);
        this.din_ucdoc_es_index = getZkData(zk_gucdoc_es_index);
        this.pf_services_graph_size = Integer.valueOf(getZkData(zk_pf_services_graph_size));

        this.zk.close();

        return this;
    }

    private String getZkData(String zNode) throws KeeperException, InterruptedException
    {
        byte[] data = zk.getData(zNode, false, zk.exists(zNode, false));
        return new String(data, StandardCharsets.UTF_8);
    }

}
