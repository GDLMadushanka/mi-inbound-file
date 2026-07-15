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

import com.google.gson.JsonElement;
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
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
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

    public StreamInjectHandler(String injectingSeq, String onErrorSeq,
        SynapseEnvironment synapseEnvironment, VFSConfig vfsProperties,
        FileSystemManager fsManager) {
        // Streaming always injects sequentially. Per-record/per-chunk mediation errors can only be
        // detected synchronously - the ERROR_CODE transport header is set while the sequence runs -
        // which requires injectInbound to mediate inline rather than hand off asynchronously.
        super(injectingSeq, onErrorSeq, true, synapseEnvironment, vfsProperties);
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
            failed.parseFailure(record);
            return true;
        }
        byte[] body = addOutputToVariable ? null : record.getContent();
        Map<String, Object> variableOutputMap = addOutputToVariable ? recordVariableMap(record) : null;
        if (!tryInject(name, contentType, body, variableOutputMap,
                "record " + record.getRecordNumber())) {
            // Mediation failed: apply the mediation-error action to this record's content (raw
            // content, or its payload in variable mode).
            log.warn("Record " + record.getRecordNumber() + " failed mediation; "
                + failed.mediationActionLabel() + " the record.");
            byte[] failBytes = addOutputToVariable
                    ? payloadBytes(record.getJSONPayload(), record.getEncoding())
                    : record.getContent();
            failed.mediationFailure(failBytes, 1);
        }
        return true;
    }

    /**
     * Process a single chunk: apply the parse-error action to its invalid records, then inject the
     * valid ones as one batch.
     * <p>
     * If the batch fails mediation we do <em>not</em> retry it record-by-record: in chunk mode the
     * sequence is written against the batch shape (e.g. expressions over the JSON array), so a lone
     * record would not mediate correctly. Instead the whole chunk is handled by the mediation-error
     * action in the same shape it was sent (a JSON array for JSON/JSONL, newline-joined lines for
     * text/CSV).
     */
    private boolean handleChunk(String name, String contentType, StreamChunk chunk,
        boolean addOutputToVariable, FailedRecordCollector failed,
        StreamingProcessor processor) throws Exception {
        List<StreamRecord> validRecords = new ArrayList<>();
        for (StreamRecord record : chunk.getRecords()) {
            if (record.isValid()) {
                validRecords.add(record);
            } else {
                failed.parseFailure(record);
            }
        }
        // Nothing left to inject once the invalid records have been handled.
        if (validRecords.isEmpty()) {
            return true;
        }

        // The processor knows how to combine its own records (newline-joined text, JSON array, ...).
        byte[] body = addOutputToVariable ? null : processor.buildChunkBody(chunk);
        Map<String, Object> variableOutputMap =
            addOutputToVariable ? chunkVariableMap(chunk, validRecords.size()) : null;
        if (!tryInject(name, contentType, body, variableOutputMap,
                describe(validRecords) + " (chunk " + chunk.getChunkNumber() + ")")) {
            // The chunk failed mediation as a unit; hand the whole chunk (in the shape it was sent)
            // to the mediation-error action.
            log.warn("Chunk " + chunk.getChunkNumber() + " failed mediation; "
                + failed.mediationActionLabel() + " its " + validRecords.size() + " record(s).");
            byte[] failBytes = addOutputToVariable
                    ? payloadBytes(chunk.getJSONPayload(), chunk.getEncoding())
                    : body;
            failed.mediationFailure(failBytes, validRecords.size());
        }
        return true;
    }

    /** Serialize a JSON payload to bytes, or null if there is no payload. */
    private static byte[] payloadBytes(JsonElement payload, Charset charset) {
        return payload != null ? payload.toString().getBytes(charset) : null;
    }

    private Map<String, Object> recordVariableMap(StreamRecord record) {
        Map<String, Object> map = new HashMap<>();
        JsonObject attributes = new JsonObject();
        attributes.addProperty(StreamingConstants.RECORD_NUMBER, record.getRecordNumber());
        map.put(StreamingConstants.ATTRIBUTES, attributes);
        map.put(StreamingConstants.PAYLOAD, record.getJSONPayload());
        return map;
    }

    private Map<String, Object> chunkVariableMap(StreamChunk chunk, int validCount) {
        Map<String, Object> map = new HashMap<>();
        JsonObject attributes = new JsonObject();
        attributes.addProperty(StreamingConstants.FIRST_RECORD_IN_CHUNK, chunk.getFirstRecordNumber());
        attributes.addProperty(StreamingConstants.LAST_RECORD_IN_CHUNK, chunk.getLastRecordNumber());
        attributes.addProperty(StreamingConstants.CHUNK_SIZE, validCount);
        attributes.addProperty(StreamingConstants.CHUNK_NUMBER, chunk.getChunkNumber());
        map.put(StreamingConstants.ATTRIBUTES, attributes);
        map.put(StreamingConstants.PAYLOAD, chunk.getJSONPayload());
        return map;
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
     * Attempt to inject one message (a single record, or a chunk batch). A mediation failure is the
     * injection returning an error code or throwing; unlike a parse failure the data itself is fine
     * (the downstream sequence failed). This method only reports success/failure - the caller
     * decides what to siphon - so a failed chunk can be retried record-by-record.
     *
     * @param what a short description of the unit, for logging
     * @return true if mediation succeeded, false on a mediation failure
     */
    private boolean tryInject(String name, String contentType, byte[] body,
                              Map<String, Object> variableOutput, String what) {
        try {
            if (injectStreamingMessage(name, contentType, body, variableOutput)) {
                return true;
            }
            log.warn("Mediation reported an error for " + what + ".");
            return false;
        } catch (Exception e) {
            log.warn("Mediation error while injecting " + what + ".", e);
            return false;
        }
    }

    private static String describe(List<StreamRecord> records) {
        if (records.isEmpty()) {
            return "no records";
        }
        long first = records.get(0).getRecordNumber();
        long last = records.get(records.size() - 1).getRecordNumber();
        return first == last ? ("record " + first) : ("records " + first + "-" + last);
    }

    /**
     * Applies the configured per-record error actions for a single source file across two
     * independent kinds of failure:
     * <ul>
     *     <li><b>parse errors</b> - bad data (only JSONL reaches this per-record). The
     *     {@code StreamingParseErrorAction} decides MOVE (append to the {@code .parse.fail} sidecar)
     *     or DROP (discard); either way processing continues.</li>
     *     <li><b>mediation errors</b> - downstream failures. The {@code StreamingMediationErrorAction}
     *     decides MOVE (append to the {@code .mediation.fail} sidecar) or DROP; either way processing
     *     continues.</li>
     * </ul>
     * Neither kind fails the whole file - that only happens on a non-recoverable structural/IO error.
     */
    private final class FailedRecordCollector {

        private final FileObject sourceFile;
        private final boolean parseMove;
        private final boolean mediationMove;
        private final FailedRecordWriter parseWriter;
        private final FailedRecordWriter mediationWriter;
        private long parseFailedCount;
        private long mediationFailedCount;

        FailedRecordCollector(FileObject sourceFile) {
            this.sourceFile = sourceFile;
            this.parseMove = StreamingConstants.STREAMING_ERROR_ACTION_MOVE
                    .equalsIgnoreCase(vfsProperties.getStreamingParseErrorAction());
            this.mediationMove = StreamingConstants.STREAMING_ERROR_ACTION_MOVE
                    .equalsIgnoreCase(vfsProperties.getStreamingMediationErrorAction());
            // Parse errors: dedicated folder, then the fault folder.
            this.parseWriter = (parseMove && fsManager != null)
                    ? buildWriter(sourceFile, "parse.fail", "parse-error",
                        vfsProperties.getStreamingParseErrorFolder(),
                        vfsProperties.getMoveAfterFailure())
                    : null;
            // Mediation errors: dedicated folder, then the parse folder, then the fault folder.
            this.mediationWriter = (mediationMove && fsManager != null)
                    ? buildWriter(sourceFile, "mediation.fail", "mediation-error",
                        vfsProperties.getStreamingMediationErrorFolder(),
                        vfsProperties.getStreamingParseErrorFolder(),
                        vfsProperties.getMoveAfterFailure())
                    : null;
        }

        /** Build a writer targeting the first non-empty folder in {@code candidates}, or null. */
        private FailedRecordWriter buildWriter(FileObject sourceFile, String marker, String label,
                                               String... candidates) {
            String folder = firstNonEmpty(candidates);
            if (folder == null) {
                log.warn("Streaming " + label + " action is MOVE but no destination folder (or "
                    + "fault folder) is configured; " + label + " records will be logged only.");
                return null;
            }
            return new FailedRecordWriter(fsManager, vfsProperties, folder,
                sourceFile.getName().getBaseName(), marker);
        }

        private String firstNonEmpty(String... values) {
            for (String v : values) {
                if (v != null && !v.trim().isEmpty()) {
                    return v;
                }
            }
            return null;
        }

        /**
         * Apply the parse-error action to an invalid (unparseable) record: MOVE appends it to the
         * parse-error sidecar, DROP discards it. Processing always continues.
         */
        void parseFailure(StreamRecord record) {
            parseFailedCount++;
            log.warn("Invalid record at row " + record.getRecordNumber() + " in file "
                + sourceFile.getName().getBaseName()
                + (record.getParseError() != null ? " : " + record.getParseError() : ""));
            if (parseWriter != null) {
                parseWriter.append(record.getContent());
            }
        }

        /**
         * Apply the mediation-error action to one injection unit (a single record, or a whole
         * chunk): MOVE appends the pre-formatted {@code content} (in the shape it was sent) to the
         * mediation-error sidecar, DROP discards it. Processing always continues.
         *
         * @param content     the bytes to move, in the shape the unit was mediated
         * @param recordCount the number of records the unit represents (for reporting)
         */
        void mediationFailure(byte[] content, long recordCount) {
            mediationFailedCount += recordCount;
            if (mediationWriter != null && content != null) {
                mediationWriter.append(content);
            }
        }

        /** "moving" or "dropping", for log messages. */
        String mediationActionLabel() {
            return mediationMove ? "moving" : "dropping";
        }

        void discard() {
            if (parseWriter != null) {
                parseWriter.discard();
            }
            if (mediationWriter != null) {
                mediationWriter.discard();
            }
        }

        void close() {
            if (parseWriter != null) {
                if (parseFailedCount > 0) {
                    log.info("Moved " + parseWriter.getWrittenCount() + " parse-error record(s) "
                        + "from " + sourceFile.getName().getBaseName() + " to the parse-error "
                        + "folder.");
                }
                parseWriter.close();
            }
            if (mediationWriter != null) {
                if (mediationFailedCount > 0) {
                    log.info("Moved " + mediationWriter.getWrittenCount() + " mediation-error "
                        + "record(s) from " + sourceFile.getName().getBaseName() + " to the "
                        + "mediation-error folder.");
                }
                mediationWriter.close();
            }
        }
    }
}
