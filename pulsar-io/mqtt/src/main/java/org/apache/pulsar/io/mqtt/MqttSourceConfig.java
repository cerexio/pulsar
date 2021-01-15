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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.pulsar.io.core.annotations.FieldDoc;
import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;


@Data
@Accessors(chain = true)
public class MqttSourceConfig implements Serializable {

    private static final long serialVersionUID = 1L;


    @FieldDoc(
            required = true,
            defaultValue = "",
            help = "The Mqtt topic name from which messages should be read from or written to")
    private String topic;

    @FieldDoc(
            required = true,
            defaultValue = "",
            help = "The Mqtt Driver Id ")
    private String driverId;

    @FieldDoc(
            required = true,
            defaultValue = "",
            help = "The Mqtt broker source ip")
    private String mqttBrokerURI;

    @FieldDoc(
            required = false,
            defaultValue = "0",
            help = "This controls how many messages can be in-flight simultaneously.")
    private int maxInflightMessages = 10;

    @FieldDoc(
            required = false,
            defaultValue = "false",
            help = "This is a boolean value. The clean session setting controls the behaviour of both the client and the server at connection and disconnection time.The client and server both maintain session state information. Thisinformation is used to ensure \"at least once\" and \"exactly once\"delivery, and \"exactly once\" receipt of messages. Session state alsoincludes subscriptions created by an MQTT client. You can choose tomaintain or discard state information between sessions.When cleansession is true, the state information is discarded atconnect and disconnect. Setting cleansession to false keeps the stateinformation. When you connect an MQTT client application withMQTTAsync_connect(), the client identifies the connection using theclient identifier and the address of the server. The server checkswhether session information for this clienthas been saved from a previous connection to the server. If a previoussession still exists, and cleansession=true, then the previous sessioninformation at the client and server is cleared. If cleansession=false,the previous session is resumed. If no previous session exists, a newsession is started")
    private boolean cleanSession = false;

    @FieldDoc(
            required=false,
            defaultValue = "60",
            help = "The keep alive interval, measured in seconds, defines the maximum timethat should pass without communication between the client and the server The client will ensure that at least one message travels across the network within each keep alive period.  In the absence of a data-rela message during the time period, the client sends a very small MQTT \"ping\" message, which the server will acknowledge. The keep alive interval enables the client to detect when the server is no lon available without having to wait for the long TCP/IP timeo Set to 0 if you do not want any keep alive processing.")
    private int keepAliveInterval = 60;

    @FieldDoc(
            required=false,
            defaultValue = "",
            help = "MQTT servers that support the MQTT v3.1 protocol provide authentication and authorisation by user name and password. This is the user name parameter.")
    private String username = "";

    @FieldDoc(
            required=false,
            defaultValue = "",
            help = "MQTT servers that support the MQTT v3.1 protocol provide authentication and authorisation by user name and password. This is the password parameter.")
    private String password = "";

    @FieldDoc(
            required = false,
            defaultValue = "0",
            help = "The time interval in seconds to allow a connect to complete")
    private int connectionTimeout = 30;

    @FieldDoc(
            required = false,
            defaultValue = "0",
            help = "MQTT version")
    private int mqttVersion = 0;

    @FieldDoc(
            required = false,
            defaultValue = "0",
            help = "Topic QoS level")
    private int qos = 0;


    @FieldDoc(
            required=false,
            defaultValue = "",
            help = "The mqtt client id")
    private String clientId = "";

    @FieldDoc(
            required = false,
            defaultValue = "false",
            help = "Enable automatic reconnection")
    private boolean autoReconnect = false;



    public static MqttSourceConfig load(String yamlFile) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(new File(yamlFile), MqttSourceConfig.class);
    }

    public static MqttSourceConfig load(Map<String, Object> map) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(new ObjectMapper().writeValueAsString(map), MqttSourceConfig.class);
    }

    public void validate() {
        Preconditions.checkArgument(topic != "", "mqtt topic name property not set.");
        Preconditions.checkArgument(maxInflightMessages >= 0, "maximum inflight message must be non-negative.");
        Preconditions.checkArgument(clientId != "", "mqtt client id is not set, this is required value");
    }
}