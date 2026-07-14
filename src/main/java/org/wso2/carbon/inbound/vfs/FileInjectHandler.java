/*
 *  Copyright (c) 2025, WSO2 LLC. (https://www.wso2.com).
 *
 *  WSO2 LLC. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.wso2.carbon.inbound.vfs;

import com.google.gson.JsonObject;
import java.util.HashMap;
import org.apache.axiom.om.OMAbstractFactory;
import org.apache.axiom.om.OMElement;
import org.apache.axiom.om.util.UUIDGenerator;
import org.apache.axiom.soap.SOAPEnvelope;
import org.apache.axis2.AxisFault;
import org.apache.axis2.Constants.Configuration;
import org.apache.axis2.builder.Builder;
import org.apache.axis2.builder.BuilderUtil;
import org.apache.axis2.builder.SOAPBuilder;
import org.apache.axis2.context.MessageContext;
import org.apache.axis2.format.DataSourceMessageBuilder;
import org.apache.axis2.format.ManagedDataSource;
import org.apache.axis2.format.ManagedDataSourceFactory;
import org.apache.axis2.transport.TransportUtils;
import org.apache.commons.io.input.AutoCloseInputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.SynapseConstants;
import org.apache.synapse.commons.vfs.VFSConstants;
import org.apache.synapse.core.SynapseEnvironment;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.apache.synapse.inbound.InboundEndpoint;
import org.apache.synapse.mediators.base.SequenceMediator;
import org.apache.synapse.transport.customlogsetter.CustomLogSetter;
import org.wso2.carbon.inbound.vfs.streaming.StreamChunk;
import org.wso2.carbon.inbound.vfs.streaming.StreamRecord;
import org.wso2.carbon.inbound.vfs.streaming.StreamingConstants;
import org.wso2.carbon.inbound.vfs.streaming.StreamingException;
import org.wso2.carbon.inbound.vfs.streaming.StreamingProcessor;
import org.wso2.carbon.inbound.vfs.streaming.StreamingProcessorFactory;
import org.wso2.org.apache.commons.vfs2.FileObject;

import javax.mail.internet.ContentType;
import javax.mail.internet.ParseException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.axis2.context.MessageContext.TRANSPORT_HEADERS;

public class FileInjectHandler {

    private static final Log log = LogFactory.getLog(FileInjectHandler.class);

    private String injectingSeq;
    private String onErrorSeq;
    private boolean sequential;
    private VFSConfig vfsProperties;
    private SynapseEnvironment synapseEnvironment;
    private Map<String, Object> transportHeaders;
    private String fileURI;

    public FileInjectHandler(String injectingSeq, String onErrorSeq, boolean sequential,
                             SynapseEnvironment synapseEnvironment, VFSConfig vfsProperties) {
        this.injectingSeq = injectingSeq;
        this.onErrorSeq = onErrorSeq;
        this.sequential = sequential;
        this.synapseEnvironment = synapseEnvironment;
        this.vfsProperties = vfsProperties;
    }

    /**
     * Entry point for processing a file. Dispatches to the appropriate handler based on the
     * configured streaming mode:
     * <ul>
     *     <li>ENTIRE_FILE (or streaming disabled) - inject the whole file as one message
     *         (previous behaviour, backwards compatible).</li>
     *     <li>CHUNK - stream the file in batches of records (ChunkIterator), one message per chunk.</li>
     *     <li>RECORD - stream the file record by record (RowIterator), one message per record.</li>
     * </ul>
     */
    public boolean invoke(Object object, String name) {
        FileObject file = (FileObject) object;

        if (vfsProperties.isStreaming()) {
            String mode = vfsProperties.getStreamingMode();
            if (org.wso2.carbon.inbound.vfs.VFSConstants.STREAMING_MODE_CHUNK.equalsIgnoreCase(mode)) {
                return invokeStreaming(file, name, true);
            } else if (org.wso2.carbon.inbound.vfs.VFSConstants.STREAMING_MODE_RECORD.equalsIgnoreCase(mode)) {
                return invokeStreaming(file, name, false);
            }
            // ENTIRE_FILE (default) falls through to the whole-file behaviour below.
        }
        return invokeEntireFile(file, name);
    }

    /**
     * Inject the whole file as a single message to the sequence.
     */
    private boolean invokeEntireFile(FileObject file, String name) {

        ManagedDataSource dataSource = null;

        InputStream in = null;
        try {
            org.apache.synapse.MessageContext msgCtx = createMessageContext();
            msgCtx.setProperty(SynapseConstants.INBOUND_ENDPOINT_NAME, name);
            msgCtx.setProperty(SynapseConstants.ARTIFACT_NAME, SynapseConstants.FAIL_SAFE_MODE_INBOUND_ENDPOINT + name);
            msgCtx.setProperty(SynapseConstants.IS_INBOUND, true);

            InboundEndpoint inboundEndpoint = msgCtx.getConfiguration().getInboundEndpoint(name);
            CustomLogSetter.getInstance().setLogAppender(inboundEndpoint.getArtifactContainerName());
            String contentType = vfsProperties.getContentType();
            if (contentType == null || contentType.trim().equals("")) {
                if (file.getName().getExtension().toLowerCase().endsWith("xml")) {
                    contentType = "text/xml";
                } else if (file.getName().getExtension().toLowerCase().endsWith("txt")) {
                    contentType = "text/plain";
                }
            } else {
                // Extract the charset encoding from the configured content type and
                // set the CHARACTER_SET_ENCODING property as e.g. SOAPBuilder relies on this.
                String charSetEnc = null;
                try {
                    charSetEnc = new ContentType(contentType).getParameter("charset");
                } catch (ParseException ex) {
                    // ignore
                }
                msgCtx.setProperty(Configuration.CHARACTER_SET_ENCODING, charSetEnc);
            }
            if (log.isDebugEnabled()) {
                log.debug("Processed file : " + file + " of Content-type : " + contentType);
            }
            MessageContext axis2MsgCtx = ((Axis2MessageContext) msgCtx)
                    .getAxis2MessageContext();
            // Determine the message builder to use
            Builder builder;
            if (contentType == null) {
                log.debug("No content type specified. Using SOAP builder.");
                builder = new SOAPBuilder();
            } else {
                int index = contentType.indexOf(';');
                String type = index > 0 ? contentType.substring(0, index) : contentType;
                builder = BuilderUtil.getBuilderFromSelector(type, axis2MsgCtx);
                if (builder == null) {
                    if (log.isDebugEnabled()) {
                        log.debug("No message builder found for type '" + type + "'. Falling back to SOAP.");
                    }
                    builder = new SOAPBuilder();
                }
            }

            // set the message payload to the message context
            boolean isStreaming = vfsProperties.isStreaming();

            if (builder instanceof DataSourceMessageBuilder && isStreaming) {
                dataSource = ManagedDataSourceFactory.create(new FileObjectDataSource(file, contentType));
            } else {
                in = new AutoCloseInputStream(file.getContent().getInputStream());
            }

            //Inject the message to the sequence.

            OMElement documentElement;
            if (in != null) {
                documentElement = builder.processDocument(in, contentType, axis2MsgCtx);
            } else {
                documentElement = ((DataSourceMessageBuilder) builder)
                        .processDocument(dataSource, contentType, axis2MsgCtx);
            }

            if (vfsProperties.isBuild()) {
                documentElement.build();
            }
            msgCtx.setEnvelope(TransportUtils.createSOAPEnvelope(documentElement));

            if (injectingSeq == null || injectingSeq.equals("")) {
                log.error("Sequence name not specified. Sequence : " + injectingSeq);
            }
            SequenceMediator seq = (SequenceMediator) synapseEnvironment.getSynapseConfiguration()
                    .getSequence(injectingSeq);
            if (seq != null) {
                if (log.isDebugEnabled()) {
                    log.debug("injecting message to sequence : " + injectingSeq);
                }
                if (!seq.isInitialized()) {
                    seq.init(synapseEnvironment);
                }
                seq.setErrorHandler(onErrorSeq);

                // >>> APPEND MODE: ensure any VFS write/Respond appends instead of overwriting
                // This sets the Axis2 property the VFS sender/transport checks.
                if (vfsProperties.isAppend()) {
                    axis2MsgCtx.setProperty("transport.vfs.Append", "true");
                }
                // Optional (config-driven): if you later add a boolean to VFSConfig, do:
                // axis2MsgCtx.setProperty("transport.vfs.Append",
                //     String.valueOf(vfsProperties.isAppendEnabled()));
                // <<< APPEND MODE

                synapseEnvironment.injectInbound(msgCtx, seq, sequential);

                Map<String, Object> transportHeaders =
                        (Map<String, Object>) axis2MsgCtx.getProperty(TRANSPORT_HEADERS);
                String errorCode = (transportHeaders != null)
                        ? (String) transportHeaders.get(VFSConstants.ERROR_CODE) : null;
                if (StringUtils.isNotEmpty(errorCode)) {
                    return false;
                }
                /// set rollback property check = -1
                /// write body of message.
            } else {
                log.error("Sequence: " + injectingSeq + " not found");
            }
        } catch (Exception e) {
            log.error("Error while processing the file/folder", e);
            return false;
        } finally {
            if (dataSource != null) {
                dataSource.destroy();
            }
            try {
                if (in != null) {
                    in.close();
                }
            } catch (IOException e) {
                log.error("Error while closing the input stream", e);
            }
        }
        return true;
    }

    /**
     * Stream a file to the sequence, injecting one message per chunk or per record.
     *
     * @param file      the file being processed
     * @param name      the inbound endpoint name
     * @param chunkMode true for CHUNK mode (ChunkIterator), false for RECORD mode (RowIterator)
     * @return true if every chunk/record was injected without error
     */
    private boolean invokeStreaming(FileObject file, String name, boolean chunkMode) {
        // In streaming modes the content type is fixed by the input format (not the user-configured
        // transport.vfs.ContentType), and drives both the reader charset and the message builder.
        String contentType = vfsProperties.getStreamingContentType();
        String inputFormat = vfsProperties.getStreamingInputFormat();
        StreamingProcessor processor = StreamingProcessorFactory.getProcessor(inputFormat, vfsProperties);
        if (processor == null) {
            log.error("No streaming processor found for input format '" + inputFormat
                    + "'. Cannot stream file: " + file.getName().getBaseName());
            return false;
        }

        boolean addOutputToVariable = vfsProperties.isStreamingAddOutputToVariable();

        try (InputStream in = file.getContent().getInputStream()) {
            if (chunkMode) {
                Iterator<StreamChunk> iterator =
                        processor.getChunkIterator(in, contentType, vfsProperties.getStreamingChunkSize());
                while (iterator.hasNext()) {
                    try {
                        StreamChunk chunk = iterator.next();
                        byte[] body = addOutputToVariable ? null : buildChunkBody(chunk);
                        Map<String, Object> variableOutputMap = null;
                        if (addOutputToVariable) {
                            variableOutputMap = new HashMap<>();
                            JsonObject attributes = new JsonObject();
                            attributes.addProperty(StreamingConstants.FIRST_RECORD_IN_CHUNK,
                                chunk.getFirstRecordNumber());
                            attributes.addProperty(StreamingConstants.LAST_RECORD_IN_CHUNK,
                                chunk.getLastRecordNumber());
                            attributes.addProperty(StreamingConstants.CHUNK_SIZE,
                                chunk.getRecordCount());
                            attributes.addProperty(StreamingConstants.CHUNK_NUMBER,
                                chunk.getChunkNumber());
                            variableOutputMap.put(StreamingConstants.ATTRIBUTES, attributes);
                            variableOutputMap.put(StreamingConstants.PAYLOAD,
                                chunk.getJSONPayload());
                        }
                        if (!injectStreamingMessage(name, contentType, body, variableOutputMap)) {
                            return false;
                        }
                    } catch (StreamingException ex) {
                        if (ex.isRecoverable()) {
                            log.warn("Recoverable streaming error at row " + ex.getRowNumber()
                                + ". Continuing with next chunk.", ex);
                        } else {
                            log.error("Unrecoverable streaming error at row " + ex.getRowNumber()
                                + ". Aborting processing the file : " + file.getName(), ex);
                            return false;
                        }
                    }
                }
            } else {
                Iterator<StreamRecord> iterator = processor.getRecordIterator(in, contentType);
                while (iterator.hasNext()) {
                    try {
                        StreamRecord record = iterator.next();
                        Map<String, Object> variableOutputMap = null;
                        if (addOutputToVariable) {
                            variableOutputMap = new HashMap<>();
                            JsonObject attributes = new JsonObject();
                            attributes.addProperty(StreamingConstants.RECORD_NUMBER,
                                record.getRecordNumber());
                            variableOutputMap.put(StreamingConstants.ATTRIBUTES, attributes);
                            variableOutputMap.put(StreamingConstants.PAYLOAD,
                                record.getJSONPayload());
                        }
                        byte[] body = addOutputToVariable ? null : record.getContent();
                        if (!injectStreamingMessage(name, contentType, body, variableOutputMap)) {
                            return false;
                        }
                    } catch (StreamingException ex) {
                        if (ex.isRecoverable()) {
                            log.warn("Recoverable streaming error at row " + ex.getRowNumber()
                                    + ". Continuing with next record.", ex);
                        } else {
                            log.error("Unrecoverable streaming error at row " + ex.getRowNumber()
                                    + ". Aborting processing the file : " + file.getName(), ex);
                            return false;
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error("Error while streaming the file/folder : " + file.getName(), e);
            return false;
        }
        return true;
    }

    /**
     * Build the message body for a chunk in raw-content mode by joining each record's raw
     * content with the platform line separator.
     */
    private byte[] buildChunkBody(StreamChunk chunk) {
        Charset charset = chunk.getEncoding();
        StringBuilder sb = new StringBuilder();
        List<StreamRecord> records = chunk.getRecords();
        for (int i = 0; i < records.size(); i++) {
            byte[] content = records.get(i).getContent();
            if (content == null) {
                continue;
            }
            if (sb.length() > 0) {
                sb.append(System.lineSeparator());
            }
            sb.append(new String(content, charset));
        }
        return sb.toString().getBytes(charset);
    }

    /**
     * Create a message context for a single chunk/record and inject it to the sequence.
     * When {@code body} is non-null it becomes the message payload; when
     * {@code variableOutput} is non-null it is exposed as a message property.
     *
     * @return true if the injection completed without an error code
     */
    private boolean injectStreamingMessage(String name, String contentType, byte[] body,
        Map<String, Object> variableOutput) throws Exception {
        org.apache.synapse.MessageContext msgCtx = createMessageContext();
        msgCtx.setProperty(SynapseConstants.INBOUND_ENDPOINT_NAME, name);
        msgCtx.setProperty(SynapseConstants.ARTIFACT_NAME,
                SynapseConstants.FAIL_SAFE_MODE_INBOUND_ENDPOINT + name);
        msgCtx.setProperty(SynapseConstants.IS_INBOUND, true);

        InboundEndpoint inboundEndpoint = msgCtx.getConfiguration().getInboundEndpoint(name);
        CustomLogSetter.getInstance().setLogAppender(inboundEndpoint.getArtifactContainerName());

        MessageContext axis2MsgCtx = ((Axis2MessageContext) msgCtx).getAxis2MessageContext();

        if (vfsProperties.isStreamingAddOutputToVariable()) {
            msgCtx.setVariable(vfsProperties.getStreamingOutputVariable(), variableOutput);
            // add empty SOAP envelope.
            SOAPEnvelope envelope = OMAbstractFactory.getSOAP12Factory().getDefaultEnvelope();
            msgCtx.setEnvelope(envelope);
        } else {
            // Raw record/chunk content becomes the message body.
            Builder builder = resolveBuilder(contentType, axis2MsgCtx);
            OMElement documentElement = builder.processDocument(
                    new ByteArrayInputStream(body), contentType, axis2MsgCtx);
            if (vfsProperties.isBuild()) {
                documentElement.build();
            }
            msgCtx.setEnvelope(TransportUtils.createSOAPEnvelope(documentElement));
        }

        if (injectingSeq == null || injectingSeq.equals("")) {
            log.error("Sequence name not specified. Sequence : " + injectingSeq);
            return false;
        }
        SequenceMediator seq = (SequenceMediator) synapseEnvironment.getSynapseConfiguration()
                .getSequence(injectingSeq);
        if (seq == null) {
            log.error("Sequence: " + injectingSeq + " not found");
            return false;
        }
        if (!seq.isInitialized()) {
            seq.init(synapseEnvironment);
        }
        seq.setErrorHandler(onErrorSeq);
        if (vfsProperties.isAppend()) {
            axis2MsgCtx.setProperty("transport.vfs.Append", "true");
        }

        synapseEnvironment.injectInbound(msgCtx, seq, sequential);

        Map<String, Object> responseHeaders =
                (Map<String, Object>) axis2MsgCtx.getProperty(TRANSPORT_HEADERS);
        String errorCode = (responseHeaders != null)
                ? (String) responseHeaders.get(VFSConstants.ERROR_CODE) : null;
        return StringUtils.isEmpty(errorCode);
    }

    /**
     * Select the Axis2 message builder for the given content type, falling back to SOAP.
     */
    private Builder resolveBuilder(String contentType, MessageContext axis2MsgCtx)
            throws AxisFault {
        if (contentType == null) {
            return new SOAPBuilder();
        }
        int index = contentType.indexOf(';');
        String type = index > 0 ? contentType.substring(0, index) : contentType;
        Builder builder = BuilderUtil.getBuilderFromSelector(type, axis2MsgCtx);
        if (builder == null) {
            builder = new SOAPBuilder();
        }
        return builder;
    }

    /**
     * @param transportHeaders the transportHeaders to set
     */
    public void setTransportHeaders(Map<String, Object> transportHeaders) {
        this.transportHeaders = transportHeaders;
    }

    public void setFileURI(String fileURI) {
        this.fileURI = fileURI;
    }

    /**
     * Create the initial message context for the file
     */
    private org.apache.synapse.MessageContext createMessageContext() {
        org.apache.synapse.MessageContext msgCtx = synapseEnvironment.createMessageContext();
        MessageContext axis2MsgCtx = ((Axis2MessageContext) msgCtx)
                .getAxis2MessageContext();
        axis2MsgCtx.setServerSide(true);
        axis2MsgCtx.setMessageID(UUIDGenerator.getUUID());
        axis2MsgCtx.setProperty(TRANSPORT_HEADERS, transportHeaders);
        msgCtx.setProperty(MessageContext.CLIENT_API_NON_BLOCKING, true);
        return msgCtx;
    }
}
