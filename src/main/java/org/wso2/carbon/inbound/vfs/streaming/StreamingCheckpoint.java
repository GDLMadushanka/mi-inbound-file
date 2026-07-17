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

package org.wso2.carbon.inbound.vfs.streaming;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

/**
 * The persisted state that lets a streamed file resume after a restart. Serialized as compact JSON
 * to the registry (see {@link StreamingCheckpointManager}).
 * <p>
 * {@link #recordsConsumed} is the authoritative resume pointer - the record number of the last
 * fully-handled record/chunk. On resume the processor skips that many records. The three counts are
 * metrics, with the invariant {@code processedRecords + parseFailedRecords + mediationFailedRecords
 * == recordsConsumed}.
 */
public class StreamingCheckpoint {

    private static final Gson GSON = new Gson();

    private int schemaVersion;
    private String inboundName;
    private String fileUri;
    private Fingerprint fileFingerprint;
    private String streamingMode;
    private String inputFormat;
    private String configHash;
    private long recordsConsumed;
    private long processedRecords;
    private long parseFailedRecords;
    private long mediationFailedRecords;
    private long updatedAt;

    /** A cheap file identity: size + last-modified + a hash of the leading bytes. */
    public static class Fingerprint {
        private long size;
        private long lastModified;
        private int hashedBytes;
        private String algorithm;
        private String hash;

        public Fingerprint() {
        }

        public Fingerprint(long size, long lastModified, int hashedBytes, String algorithm, String hash) {
            this.size = size;
            this.lastModified = lastModified;
            this.hashedBytes = hashedBytes;
            this.algorithm = algorithm;
            this.hash = hash;
        }

        public long getSize() {
            return size;
        }

        public long getLastModified() {
            return lastModified;
        }

        public int getHashedBytes() {
            return hashedBytes;
        }

        public String getAlgorithm() {
            return algorithm;
        }

        public String getHash() {
            return hash;
        }

        /** True when both fingerprints refer to the same file content (size, mtime and sample hash). */
        public boolean matches(Fingerprint other) {
            return other != null
                    && size == other.size
                    && lastModified == other.lastModified
                    && (hash == null ? other.hash == null : hash.equals(other.hash));
        }
    }

    public StreamingCheckpoint() {
    }

    /** @return the compact JSON form to persist. */
    public String toJson() {
        return GSON.toJson(this);
    }

    /** Parse a checkpoint from its JSON form, or {@code null} if the text is not a valid checkpoint. */
    public static StreamingCheckpoint fromJson(String json) {
        if (json == null || json.trim().isEmpty()) {
            return null;
        }
        try {
            return GSON.fromJson(json, StreamingCheckpoint.class);
        } catch (JsonSyntaxException e) {
            return null;
        }
    }

    /**
     * True when this stored checkpoint can be safely resumed for the given file/config: same
     * fingerprint, same config, same mode and format.
     */
    public boolean isResumableFor(Fingerprint current, String configHash, String streamingMode,
                                  String inputFormat) {
        return fileFingerprint != null
                && fileFingerprint.matches(current)
                && this.configHash != null && this.configHash.equals(configHash)
                && this.streamingMode != null && this.streamingMode.equalsIgnoreCase(streamingMode)
                && this.inputFormat != null && this.inputFormat.equalsIgnoreCase(inputFormat);
    }

    public int getSchemaVersion() {
        return schemaVersion;
    }

    public void setSchemaVersion(int schemaVersion) {
        this.schemaVersion = schemaVersion;
    }

    public String getInboundName() {
        return inboundName;
    }

    public void setInboundName(String inboundName) {
        this.inboundName = inboundName;
    }

    public String getFileUri() {
        return fileUri;
    }

    public void setFileUri(String fileUri) {
        this.fileUri = fileUri;
    }

    public Fingerprint getFileFingerprint() {
        return fileFingerprint;
    }

    public void setFileFingerprint(Fingerprint fileFingerprint) {
        this.fileFingerprint = fileFingerprint;
    }

    public String getStreamingMode() {
        return streamingMode;
    }

    public void setStreamingMode(String streamingMode) {
        this.streamingMode = streamingMode;
    }

    public String getInputFormat() {
        return inputFormat;
    }

    public void setInputFormat(String inputFormat) {
        this.inputFormat = inputFormat;
    }

    public String getConfigHash() {
        return configHash;
    }

    public void setConfigHash(String configHash) {
        this.configHash = configHash;
    }

    public long getRecordsConsumed() {
        return recordsConsumed;
    }

    public void setRecordsConsumed(long recordsConsumed) {
        this.recordsConsumed = recordsConsumed;
    }

    public long getProcessedRecords() {
        return processedRecords;
    }

    public void setProcessedRecords(long processedRecords) {
        this.processedRecords = processedRecords;
    }

    public long getParseFailedRecords() {
        return parseFailedRecords;
    }

    public void setParseFailedRecords(long parseFailedRecords) {
        this.parseFailedRecords = parseFailedRecords;
    }

    public long getMediationFailedRecords() {
        return mediationFailedRecords;
    }

    public void setMediationFailedRecords(long mediationFailedRecords) {
        this.mediationFailedRecords = mediationFailedRecords;
    }

    public long getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(long updatedAt) {
        this.updatedAt = updatedAt;
    }
}
