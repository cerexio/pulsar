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



import lombok.Data;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.SourceContext;
import org.eclipse.paho.client.mqttv3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.Optional;

public class MessageProcessor implements IMqttMessageHandler {

    private static final Logger log = LoggerFactory.getLogger(MessageProcessor.class);
    final MqttConnector connector;
    private boolean shutdownInProgress = false;
    public static final String METRICS_MQTT_TOTAL_INCOMING = "mqtt_incoming_messages";
    public static final String METRICS_MQTT_TOTAL_INCOMING_BYTES = "mqtt_incoming_bytes";
    private MqttSource source;
    private SourceContext sourceContext;
    private long mqttReceiveMsgCount;
    private long mqttReceiveMsgSize;
    private String driverId;


private static final Logger LOG = LoggerFactory.getLogger(MessageProcessor.class);
    public MessageProcessor(MqttConnector connector,MqttSource mqttSource,SourceContext srcContext,MqttSourceConfig mqttSourceConfig) {
        this.mqttReceiveMsgCount =0;
        this.mqttReceiveMsgSize =0;
        this.source = mqttSource;
        this.sourceContext=  srcContext;
        this.connector = connector;
        this.driverId = mqttSourceConfig.getDriverId();
    }

    @Override
    public void handleMessage(String topic, MqttMessage message) throws Exception {
        try {
            // This method is invoked synchronously by the MQTT client (via our connector), so all events arrive in the same thread
            // https://www.eclipse.org/paho/files/javadoc/org/eclipse/paho/client/mqttv3/MqttCallback.html

            // Optimally we would like to send the event to Pulsar synchronously and validate that it was a success,
            // and only after that acknowledge the message to mqtt by returning gracefully from this function.
            // This works if the rate of incoming messages is low enough to complete the Pulsar transaction.
            // This would allow us to deliver messages once and only once, in insertion order

            // If we want to improve our performance and lose guarantees of once-and-only-once,
            // we can optimize the pipeline by sending messages asynchronously to Pulsar.
            // This means that data will be lost on insertion errors.
            // Using a single producer however should guarantee insertion-order guarantee between two consecutive messages.

            // Current implementation uses the latter approach
            byte[] payload = message.getPayload();

            LOG.info("Received message from MQTT source : {}", message.toString());

            if (payload != null) {
                LOG.info("Received the message  in bytes : {}", payload);
                source.consume(new MqttRecord(Optional.ofNullable(topic), payload));
                ++mqttReceiveMsgCount;
                sourceContext.recordMetric(METRICS_MQTT_TOTAL_INCOMING,mqttReceiveMsgCount );
                mqttReceiveMsgSize +=payload.length;
                sourceContext.recordMetric(METRICS_MQTT_TOTAL_INCOMING_BYTES,mqttReceiveMsgSize );
            }
            else {
                log.warn("Cannot forward Message to Pulsar because (mapped) content is null");
            }

        }
        catch (Exception e) {
            log.error("Error while handling the message", e);
            // Let's close everything and restart.
            // Closing the MQTT connection should enable us to receive the same message again.
            close(true);
        }

    }

    @Override
    public void connectionLost(Throwable cause) {
        log.info("MQTT connection lost");
        close(false);
    }

    public void close(boolean closeMqtt) {
        if (shutdownInProgress) {
            return;
        }
        shutdownInProgress = true;

        log.warn("Closing MessageProcessor resources");
        //Let's first close the MQTT to stop the event stream.
        if (closeMqtt) {
            connector.closeMqttConnection();
            log.info("MQTT connection closed");
        }


    }

    public boolean isMqttConnected() {
        if (connector != null) {
            boolean mqttConnected = connector.isMqttConnected();
            if (mqttConnected == false) {
                log.error("Health check: MQTT is not connected");
            }
            return mqttConnected;
        }
        return false;
    }


    @Data
    static private class MqttRecord implements Record<byte[]> {
        private final Optional<String> key;
        private final byte[] value;
    }
}