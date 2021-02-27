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

package com.bluemarlin.pfservice.controllers;

import com.bluemarlin.pfservice.model.GraphRequest;
import com.bluemarlin.pfservice.model.GraphResponse;
import com.bluemarlin.pfservice.services.GraphService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class PFController
{
    @Autowired
    private GraphService graphService;

    @GetMapping("/")
    public String ping()
    {
        return "Pong";
    }

    @PostMapping("/graph/click-ctr")
    public GraphResponse getGraphs(@RequestBody GraphRequest graphRequest) throws Exception
    {
        return graphService.buildGraph(graphRequest);
    }
}
