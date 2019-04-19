/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

 

package com.dtstack.flink.sql.source.kafka;


import com.dtstack.flink.sql.source.AbsDeserialization;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

/**
 * json string parsing custom
 * Date: 2018/09/18
 * Company: www.dtstack.com
 * @author sishu.yss
 */

public class CustomerJsonDeserialization extends AbsDeserialization<Row> {

    private static final Logger logger = LoggerFactory.getLogger(CustomerJsonDeserialization.class);

    private final ObjectMapper objectMapper = new ObjectMapper();

    /** Type information describing the result type. */
    private final TypeInformation<Row> typeInfo;

    /** Field names to parse. Indices match fieldTypes indices. */
    private final String[] fieldNames;

    /** Types to parse fields as. Indices match fieldNames indices. */
    private final TypeInformation<?>[] fieldTypes;

    /** Flag indicating whether to fail on a missing field. */
    private boolean failOnMissingField;

    public CustomerJsonDeserialization(TypeInformation<Row> typeInfo){
        this.typeInfo = typeInfo;

        this.fieldNames = ((RowTypeInfo) typeInfo).getFieldNames();

        this.fieldTypes = ((RowTypeInfo) typeInfo).getFieldTypes();
    }

    @Override
    public Row deserialize(byte[] message) throws IOException {
        try {
            numInRecord.inc();
            numInBytes.inc(message.length);

            JsonNode root = objectMapper.readTree(message);
            Row row = new Row(fieldNames.length);
            for (int i = 0; i < fieldNames.length; i++) {
                JsonNode node = getIgnoreCase(root, fieldNames[i]);

                if (node == null) {
                    if (failOnMissingField) {
                        throw new IllegalStateException("Failed to find field with name '"
                                + fieldNames[i] + "'.");
                    } else {
                        row.setField(i, null);
                    }
                } else {
                    // Read the value as specified type
                    if(node.isValueNode()){
                        Object value = objectMapper.treeToValue(node, fieldTypes[i].getTypeClass());
                        row.setField(i, value);
                    }else{
                        row.setField(i, node.toString());
                    }
                }
            }

            numInResolveRecord.inc();
            return row;
        } catch (Throwable t) {
            logger.error(t.getMessage());
            dirtyDataCounter.inc();
            //return new Row(fieldNames.length);
            return null;
        }
    }

    public void setFailOnMissingField(boolean failOnMissingField) {
        this.failOnMissingField = failOnMissingField;
    }

    public JsonNode getIgnoreCase(JsonNode jsonNode, String key) {
        if(!key.isEmpty() && key.indexOf("_")==0){
            key = key.substring(1);
        }
        Iterator<String> iter = jsonNode.fieldNames();
        while (iter.hasNext()) {
            String key1 = iter.next();
            if (key1.equalsIgnoreCase(key)) {
                return jsonNode.get(key1);
            }else if(key.indexOf("_")>0 && key1.equalsIgnoreCase(key.substring(0,key.indexOf("_")))){
                Iterator<String> subIter = jsonNode.get(key1).fieldNames();
                while (subIter.hasNext()) {
                    String key2 = subIter.next();
                    if (key2.equalsIgnoreCase(key.substring(key.indexOf("_") + 1))) {
                        return jsonNode.get(key1).get(key2);
                    }
                }
            }
        }

        return null;

    }
}
