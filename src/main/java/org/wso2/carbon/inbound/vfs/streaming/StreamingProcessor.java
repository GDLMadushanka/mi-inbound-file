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
import java.util.Iterator;

public interface StreamingProcessor {

    /**
     * Get an iterator of chunks (batches of records) from the input stream.
     * Use this for batch processing (multiple rows per chunk).
     * <p>
     * Guarantees:
     * - Each StreamChunk contains multiple complete, valid records
     * - No partial records or split fields
     * - Multi-byte characters are never split
     * - Record boundaries are preserved
     * <p>
     * Structure:
     * - Headers stored on StreamChunk (always)
     * - Individual StreamRecords in chunk (no headers)
     * - Metadata on StreamRecords (if configured)
     *
     * @param input the input stream to read from
     * @param contentType the MIME type
     * @return an Iterator of StreamChunk (batches of records)
     * @throws StreamingException if initialization fails
     */
    Iterator<StreamChunk> getChunkIterator(InputStream input, String contentType, int chunkSize)
        throws StreamingException;

    /**
     * Get an iterator of individual records from the input stream.
     * Use this for row-by-row processing (one record per iteration).
     * <p>
     * Guarantees:
     * - Each StreamRecord is a complete, valid record
     * - No partial records or split fields
     * - Multi-byte characters are never split
     * - Record boundaries are preserved
     * <p>
     * Structure:
     * - Headers on StreamRecord (if configured)
     * - Metadata on StreamRecord (if configured)
     *
     * @param input the input stream to read from
     * @param contentType the MIME type
     * @return an Iterator of StreamRecord (individual records)
     * @throws StreamingException if initialization fails
     */
    Iterator<StreamRecord> getRecordIterator(InputStream input, String contentType) throws StreamingException;

}
