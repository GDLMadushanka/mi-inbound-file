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

package org.wso2.carbon.inbound.vfs.streaming;

import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

public abstract class ChunkedDataProcessor implements StreamingProcessor {

    protected int bufferSize = 8192; // Default buffer size in BufferedReader class is 8192 bytes (8 KB)
    protected Charset defaultCharset = StandardCharsets.UTF_8;

    public ChunkedDataProcessor() {
    }

    public ChunkedDataProcessor(int bufferSize) {
        this.bufferSize = Math.max(512, bufferSize);
    }

    /**
     * Detect charset from content type header or default to UTF-8.
     *
     * @param contentType the Content-Type header value
     * @return detected Charset, or UTF-8 as default
     */
    protected Charset detectCharset(String contentType) {
        if (contentType == null || contentType.isEmpty()) {
            return defaultCharset;
        }

        String lower = contentType.toLowerCase();

        if (lower.contains("charset=")) {
            try {
                String[] parts = contentType.split("charset=");
                if (parts.length > 1) {
                    String charsetName = parts[1].trim();
                    if (charsetName.contains(";")) {
                        charsetName = charsetName.split(";")[0].trim();
                    }
                    charsetName = charsetName.replaceAll("\"", "");
                    return Charset.forName(charsetName);
                }
            } catch (Exception e) {
                // Fall through to default
            }
        }

        if (lower.contains("utf-8") || lower.contains("utf8")) {
            return StandardCharsets.UTF_8;
        } else if (lower.contains("iso-8859-1") || lower.contains("latin-1")) {
            return StandardCharsets.ISO_8859_1;
        } else if (lower.contains("utf-16")) {
            return StandardCharsets.UTF_16;
        }

        return defaultCharset;
    }

    /**
     * Get the buffer size for reading.
     *
     * @return buffer size in bytes
     */
    public int getBufferSize() {
        return bufferSize;
    }

    /**
     * Set the buffer size for reading.
     *
     * @param bufferSize size in bytes (minimum 512)
     */
    public void setBufferSize(int bufferSize) {
        this.bufferSize = Math.max(512, bufferSize);
    }

    public Charset getDefaultCharset() {
        return defaultCharset;
    }

    public void setDefaultCharset(Charset charset) {
        this.defaultCharset = charset != null ? charset : StandardCharsets.UTF_8;
    }

    @Override
    public abstract Iterator<StreamChunk> getChunkIterator(InputStream input, String contentType)
            throws StreamingException;

}
