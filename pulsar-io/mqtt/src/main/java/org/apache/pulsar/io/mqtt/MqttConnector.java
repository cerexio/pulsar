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


import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.UUID;

public class MqttConnector implements MqttCallback {
    private static final Logger log = LoggerFactory.getLogger(MqttConnector.class);

    private final String mqttTopic;
    private int qosLevel;
    private String clientId;
    private final String broker;
    private boolean cleanSession;
    private int maxInFlight;
    private boolean automaticReconnect;
    private int connectionTimeout;
    private final MqttConnectOptions connectOptions;
    private final LinkedList<IMqttMessageHandler> handlers = new LinkedList<>();




    private MqttClient mqttClient;
    private MqttSourceConfig mqttSourceConfig;
    private static final Logger LOG = LoggerFactory.getLogger(MqttConnector.class);
    public MqttConnector(MqttSourceConfig mqttSourceConfig) {
        mqttTopic = mqttSourceConfig.getTopic();
        qosLevel = mqttSourceConfig.getQos();
        broker = mqttSourceConfig.getMqttBrokerURI();
        clientId = mqttSourceConfig.getClientId();
        clientId += "-" + UUID.randomUUID().toString().substring(0, 8);


        LOG.info("client id {}, mqtt topic {}, qos level {} broker {}",clientId,mqttTopic, qosLevel, broker);

        cleanSession = mqttSourceConfig.isCleanSession();
        maxInFlight  = mqttSourceConfig.getMaxInflightMessages();
        automaticReconnect = mqttSourceConfig.isAutoReconnect();
        connectionTimeout= mqttSourceConfig.getConnectionTimeout();
        connectOptions = new MqttConnectOptions();
        connectOptions.setCleanSession(cleanSession); //This should be false for persistent subscription
        connectOptions.setMaxInflight(maxInFlight);
        connectOptions.setAutomaticReconnect(automaticReconnect); //Let's abort on connection errors
        connectOptions.setConnectionTimeout(connectionTimeout);

        LOG.info("clean session {}, max inflight messages {}, automatic reconnect {} connection timeout {}",cleanSession,maxInFlight,automaticReconnect,connectionTimeout);
    }



    public void subscribe(IMqttMessageHandler handler) {
        //let's not subscribe to the actual client. we have our own observables here
        //since we want to provide the disconnected-event via the same interface.
        LOG.info("Adding subscription");
        handlers.add(handler);
    }

    public void connect() throws Exception {
        MqttClient client = null;
        try {
            //Let's use memory persistance to optimize throughput.
            MemoryPersistence memoryPersistence = new MemoryPersistence();

            client = new MqttClient(broker, clientId, memoryPersistence);
            client.setCallback(this); //Let's add the callback before connecting so we won't lose any messages

            LOG.info(String.format("Connecting to mqtt broker %s", broker));
            IMqttToken token = client.connectWithResult(connectOptions);

            LOG.info("Connection to MQTT completed? {}", token.isComplete());
            if (token.getException() != null) {
                throw token.getException();
            }
        }
        catch (Exception e) {
            LOG.error("Error connecting to MQTT broker", e);
            if (client != null) {
                //Paho doesn't close the connection threads unless we force-close it.
                client.close(true);
            }
            throw e;
        }

        try {
            LOG.info("Subscribing to topic {} with QoS {}", mqttTopic, qosLevel);
            mqttClient = client;
            mqttClient.subscribe(mqttTopic, qosLevel);
        }
        catch (Exception e) {
            LOG.error("Error subscribing to MQTT broker", e);
            closeMqttConnection();
            throw e;
        }
    }

    @Override
    public void connectionLost(Throwable cause) {
        LOG.error("Connection to mqtt broker lost, notifying clients", cause);
        for (IMqttMessageHandler handler: handlers) {
            handler.connectionLost(cause);
        }
        closeMqttConnection();
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) throws Exception {
        for (IMqttMessageHandler handler: handlers) {
            handler.handleMessage(topic, message);
        }
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {}

    public void closeMqttConnection() {
        if (mqttClient == null) {
            LOG.warn("Cannot close mqtt connection since it's null");
            return;
        }
        try {
            LOG.info("Closing MqttConnector resources");
            //Paho doesn't close the connection threads unless we first disconnect and then force-close it.
            mqttClient.disconnectForcibly(1000L, 1000L);
            mqttClient.close(true);
            mqttClient = null;
        }
        catch (Exception e) {
            LOG.error("Failed to close MQTT client connection", e);
        }
    }

    public boolean isMqttConnected() {
        if (mqttClient != null) {
            return mqttClient.isConnected();
        }
        return false;
    }
}