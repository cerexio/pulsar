/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pulsar.io.mqtt;

import org.apache.pulsar.io.core.PushSource;
import org.apache.pulsar.io.core.SourceContext;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;


@Connector(
        name = "mqtt",
        type = IOType.SOURCE,
        help = "A simple connector to move messages from a mqtt topic to a Pulsar topic",
        configClass = MqttSourceConfig.class)

public class MqttSource extends PushSource<byte[]> {

    private static final Logger LOG = LoggerFactory.getLogger(MqttSource.class);
    private MqttSourceConfig mqttSourceConfig;

    private MqttConnector mqttConnector;
    private Thread runnerThread;
    private volatile boolean running = false;
    private String driverId;
    @Override
    public void open(Map<String, Object> config, SourceContext sourceContext) throws Exception {
        LOG.info("opening mqtt source connection");
        mqttSourceConfig = MqttSourceConfig.load(config);
        mqttSourceConfig.validate();
        this.driverId = mqttSourceConfig.getDriverId();
        mqttConnector = new MqttConnector(mqttSourceConfig);
        MessageProcessor processor = new MessageProcessor(mqttConnector, this,sourceContext,mqttSourceConfig);

        mqttConnector.subscribe(processor);
        LOG.info("adding subscriber is done");

        this.start();
        running = true;
    }

    public void start() {
        runnerThread = new Thread(() -> {
            LOG.info("Starting mqtt source connector thread");
            try {
                mqttConnector.connect();


            } catch (Exception e) {

                e.printStackTrace();
            }
            LOG.info("mqtt source started.");

            while (running) {
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }
        });
        runnerThread.setUncaughtExceptionHandler((t, e) -> LOG.error("[{}] Error while consuming records", t.getName(), e));
        runnerThread.setName("mqtt Source Thread");
        runnerThread.start();
    }

    @Override
    public void close() throws InterruptedException {
        LOG.info("Stopping mqtt source");
        //TODO we will have to close the mqtt source connection from here
    }


}

