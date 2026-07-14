/*
 *  Copyright (c) 2026, WSO2 LLC. (https://www.wso2.com).
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
import org.apache.axiom.om.OMAbstractFactory;
import org.apache.axiom.om.OMElement;
import org.apache.axiom.soap.SOAPEnvelope;
import org.apache.axis2.builder.Builder;
import org.apache.axis2.context.MessageContext;
import org.apache.axis2.transport.TransportUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.core.SynapseEnvironment;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.wso2.carbon.inbound.vfs.streaming.FailedRecordWriter;
import org.wso2.carbon.inbound.vfs.streaming.StreamChunk;
import org.wso2.carbon.inbound.vfs.streaming.StreamRecord;
import org.wso2.carbon.inbound.vfs.streaming.StreamingConstants;
import org.wso2.carbon.inbound.vfs.streaming.StreamingException;
import org.wso2.carbon.inbound.vfs.streaming.StreamingProcessor;
import org.wso2.carbon.inbound.vfs.streaming.StreamingProcessorFactory;
import org.wso2.org.apache.commons.vfs2.FileObject;
import org.wso2.org.apache.commons.vfs2.FileSystemManager;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Streams an inbound VFS file to a Synapse sequence, injecting one message per chunk (CHUNK mode)
 * or per record (RECORD mode) instead of loading the whole file into a single message.
 * <p>
 * Recoverable, per-record parse failures (e.g. a malformed JSONL line) are handled without failing
 * the whole file: the raw bytes of each failed record are siphoned to a sidecar in the configured
 * failed-records folder (see {@link FailedRecordWriter}) and processing continues. If the number of
 * failed records reaches the configured maximum, or the siphon is disabled for the format, the file
 * is treated as a complete failure - the partial sidecar is discarded and the caller applies the
 * configured action-after-failure (move to fault folder / delete) to the whole file.
 * <p>
 * A non-recoverable {@link StreamingException} (structural/IO error) also fails the whole file.
 */
public class StreamInjectHandler extends AbstractInjectHandler {

    private static final Log log = LogFactory.getLog(StreamInjectHandler.class);

    private final FileSystemManager fsManager;

    public StreamInjectHandler(String injectingSeq, String onErrorSeq, boolean sequential,
        SynapseEnvironment synapseEnvironment, VFSConfig vfsProperties,
        FileSystemManager fsManager) {
        super(injectingSeq, onErrorSeq, sequential, synapseEnvironment, vfsProperties);
        this.fsManager = fsManager;
    }

    /**
     * Stream a file to the sequence, injecting one message per chunk or per record.
     *
     * @param file      the file being processed
     * @param name      the inbound endpoint name
     * @param chunkMode true for CHUNK mode (ChunkIterator), false for RECORD mode (RowIterator)
     * @return true if the file was processed successfully (bad records may have been siphoned);
     * false if the file must be treated as a complete failure
     */
    public boolean stream(FileObject file, String name, boolean chunkMode) {
        // In streaming modes the content type is fixed by the input format (not the user-configured
        // transport.vfs.ContentType), and drives both the reader charset and the message builder.
        String contentType = vfsProperties.getStreamingContentType();
        String inputFormat = vfsProperties.getStreamingInputFormat();
        StreamingProcessor processor = StreamingProcessorFactory.getProcessor(inputFormat,
            vfsProperties);
        if (processor == null) {
            log.error("No streaming processor found for input format '" + inputFormat
                + "'. Cannot stream file: " + file.getName().getBaseName());
            return false;
        }

        boolean addOutputToVariable = vfsProperties.isStreamingAddOutputToVariable();
        FailedRecordCollector failed = new FailedRecordCollector(file);

        try (InputStream in = file.getContent().getInputStream()) {
            if (chunkMode) {
                Iterator<StreamChunk> iterator =
                    processor.getChunkIterator(in, contentType,
                        vfsProperties.getStreamingChunkSize());
                while (iterator.hasNext()) {
                    try {
                        StreamChunk chunk = iterator.next();
                        if (!handleChunk(name, contentType, chunk, addOutputToVariable, failed,
                            processor)) {
                            return false;
                        }
                    } catch (StreamingException ex) {
                        if (!handleStreamingException(ex, file, failed, "chunk")) {
                            return false;
                        }
                    }
                }
            } else {
                Iterator<StreamRecord> iterator = processor.getRecordIterator(in, contentType);
                while (iterator.hasNext()) {
                    try {
                        StreamRecord record = iterator.next();
                        if (!handleRecord(name, contentType, record, addOutputToVariable, failed)) {
                            return false;
                        }
                    } catch (StreamingException ex) {
                        if (!handleStreamingException(ex, file, failed, "record")) {
                            return false;
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error("Error while streaming the file/folder : " + file.getName(), e);
            failed.discard();
            return false;
        } finally {
            failed.close();
        }
        return true;
    }

    /**
     * Process a single record: siphon it if invalid, otherwise inject it.
     *
     * @return false if the whole file must be treated as a complete failure
     */
    private boolean handleRecord(String name, String contentType, StreamRecord record,
        boolean addOutputToVariable, FailedRecordCollector failed) throws Exception {
        if (!record.isValid()) {
            return failed.recordFailure(record);
        }
        Map<String, Object> variableOutputMap = null;
        if (addOutputToVariable) {
            variableOutputMap = new HashMap<>();
            JsonObject attributes = new JsonObject();
            attributes.addProperty(StreamingConstants.RECORD_NUMBER, record.getRecordNumber());
            variableOutputMap.put(StreamingConstants.ATTRIBUTES, attributes);
            variableOutputMap.put(StreamingConstants.PAYLOAD, record.getJSONPayload());
        }
        byte[] body = addOutputToVariable ? null : record.getContent();
        return injectStreamingMessage(name, contentType, body, variableOutputMap);
    }

    /**
     * Process a single chunk: siphon its invalid records, then inject the valid ones.
     *
     * @return false if the whole file must be treated as a complete failure
     */
    private boolean handleChunk(String name, String contentType, StreamChunk chunk,
        boolean addOutputToVariable, FailedRecordCollector failed,
        StreamingProcessor processor) throws Exception {
        int validCount = 0;
        for (StreamRecord record : chunk.getRecords()) {
            if (record.isValid()) {
                validCount++;
            } else if (!failed.recordFailure(record)) {
                return false;
            }
        }
        // Nothing left to inject once the invalid records have been siphoned.
        if (validCount == 0) {
            return true;
        }

        // The processor knows how to combine its own records (newline-joined text, JSON array, ...).
        byte[] body = addOutputToVariable ? null : processor.buildChunkBody(chunk);
        Map<String, Object> variableOutputMap = null;
        if (addOutputToVariable) {
            variableOutputMap = new HashMap<>();
            JsonObject attributes = new JsonObject();
            attributes.addProperty(StreamingConstants.FIRST_RECORD_IN_CHUNK,
                chunk.getFirstRecordNumber());
            attributes.addProperty(StreamingConstants.LAST_RECORD_IN_CHUNK,
                chunk.getLastRecordNumber());
            attributes.addProperty(StreamingConstants.CHUNK_SIZE, validCount);
            attributes.addProperty(StreamingConstants.CHUNK_NUMBER, chunk.getChunkNumber());
            variableOutputMap.put(StreamingConstants.ATTRIBUTES, attributes);
            variableOutputMap.put(StreamingConstants.PAYLOAD, chunk.getJSONPayload());
        }
        return injectStreamingMessage(name, contentType, body, variableOutputMap);
    }

    /**
     * Handle a StreamingException thrown mid-iteration. Recoverable errors are logged and skipped;
     * non-recoverable errors fail the whole file (discarding any partial sidecar).
     *
     * @return true to continue processing, false to abort the file
     */
    private boolean handleStreamingException(StreamingException ex, FileObject file,
        FailedRecordCollector failed, String unit) {
        if (ex.isRecoverable()) {
            log.warn("Recoverable streaming error at row " + ex.getRowNumber()
                + ". Continuing with next " + unit + ".", ex);
            return true;
        }
        log.error("Unrecoverable streaming error at row " + ex.getRowNumber()
            + ". Aborting processing the file : " + file.getName(), ex);
        failed.discard();
        return false;
    }

    /**
     * Create a message context for a single chunk/record and inject it to the sequence. When
     * {@code body} is non-null it becomes the message payload; when {@code variableOutput} is
     * non-null it is exposed as a message-context variable and the body is left empty.
     *
     * @return true if the injection completed without an error code
     */
    private boolean injectStreamingMessage(String name, String contentType, byte[] body,
        Map<String, Object> variableOutput) throws Exception {
        org.apache.synapse.MessageContext msgCtx = createMessageContext();
        seedInboundProperties(msgCtx, name);
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
        return injectToSequence(name, msgCtx, axis2MsgCtx);
    }

    /**
     * Tracks failed records for a single source file: counts them, enforces the max-failed-records
     * threshold, and (when the siphon is enabled and a destination is available) appends their raw
     * bytes to the failed-records sidecar.
     */
    private final class FailedRecordCollector {

        private final FileObject sourceFile;
        private final boolean skip;
        private final int maxFailed;
        private final FailedRecordWriter writer;
        private long failedCount;

        FailedRecordCollector(FileObject sourceFile) {
            this.sourceFile = sourceFile;
            this.skip = vfsProperties.isStreamingSkipFailedRecords();
            this.maxFailed = vfsProperties.getStreamingMaxFailedRecords();
            this.writer = (skip && fsManager != null) ? buildWriter(sourceFile) : null;
        }

        private FailedRecordWriter buildWriter(FileObject sourceFile) {
            String folder = vfsProperties.getStreamingFailedRecordsFolder();
            if (folder == null || folder.trim().isEmpty()) {
                folder = vfsProperties.getMoveAfterFailure();
            }
            if (folder == null || folder.trim().isEmpty()) {
                log.warn("Streaming failed-record siphon is enabled but no failed-records folder "
                    + "(or fault folder) is configured; failed records will be logged only.");
                return null;
            }
            return new FailedRecordWriter(fsManager, vfsProperties, folder,
                sourceFile.getName().getBaseName());
        }

        /**
         * Record an invalid record. Siphons and skips it when tolerated; otherwise signals a
         * whole-file failure.
         *
         * @return true to continue processing, false if the file must be treated as a complete
         * failure
         */
        boolean recordFailure(StreamRecord record) {
            failedCount++;
            log.warn("Invalid record at row " + record.getRecordNumber() + " in file "
                + sourceFile.getName().getBaseName()
                + (record.getParseError() != null ? " : " + record.getParseError() : ""));

            // Tolerated only when siphoning is enabled and we are still below the configured cap.
            // maxFailed < 0 means unlimited; otherwise the file fails once the count reaches it.
            boolean tolerate = skip && (maxFailed < 0 || failedCount < maxFailed);
            if (!tolerate) {
                if (!skip) {
                    log.error("Encountered an invalid record at row " + record.getRecordNumber()
                        + " and failed-record siphoning is disabled. Treating the file as a "
                        + "complete failure: " + sourceFile.getName().getBaseName());
                } else {
                    log.error("Failed-record count reached the configured maximum (" + maxFailed
                        + "). Treating the file as a complete failure: "
                        + sourceFile.getName().getBaseName());
                }
                discard();
                return false;
            }
            if (writer != null) {
                writer.append(record.getContent());
            }
            return true;
        }

        void discard() {
            if (writer != null) {
                writer.discard();
            }
        }

        void close() {
            if (writer != null) {
                if (failedCount > 0) {
                    log.info("Siphoned " + writer.getWrittenCount() + " failed record(s) from "
                        + sourceFile.getName().getBaseName() + " to the failed-records folder.");
                }
                writer.close();
            }
        }
    }
}
