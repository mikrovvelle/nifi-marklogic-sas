/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.sas7ToJSON;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.marklogic.sastools.SasFileReaderJson;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;

import java.util.*;

@Tags({"SAS", "SAS7BDAT",})
@CapabilityDescription("" +
        "Converts SAS7BDAT files into a single JSON array of objects, " +
        "each with the same set of keys representing the columns from the table. " +
        "(one object per row)")
//@SeeAlso({})
//@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
//@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class Sas7BDatToSingleJsonArray extends AbstractProcessor {

    static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("File was converted to JSON")
            .build();

    static final Relationship FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Conversion went poorly")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    private ObjectMapper mapper = new ObjectMapper();

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(SUCCESS);
        relationships.add(FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();

        final SasFileReaderJson[] sasFileReaderJson = new SasFileReaderJson[1];
        final ArrayNode[] arrayNode = new ArrayNode[1];
        FlowFile finalFlowFile = flowFile;
        final Boolean[] success = {false};

        session.read(flowFile, in -> {
            try (in) {
                if (in.available() > 0) {
                    sasFileReaderJson[0] = new SasFileReaderJson(in);
                    arrayNode[0] = sasFileReaderJson[0].readDataSetToArrayNode();
                    success[0] = true;
                } else {
                    getLogger().warn("Empty flowFile: " +finalFlowFile.getId());
                }
            } catch (Exception ex) {
                getLogger().error("Failed to read SAS File: " + finalFlowFile.getAttribute("filename"));
                ex.printStackTrace();
            }
        });

        if (success[0]) {
            flowFile = session.write(flowFile, outputStream -> mapper.writeValue(outputStream, arrayNode[0]));
            session.transfer(flowFile, SUCCESS);
        } else {
            session.transfer(flowFile, FAILURE);
        }
        session.commit();
    }
}
