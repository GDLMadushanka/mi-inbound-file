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

import org.wso2.carbon.inbound.vfs.VFSConfig;
import org.wso2.carbon.inbound.vfs.streaming.csv.CSVStreamingProcessor;

/**
 * Selects a {@link StreamingProcessor} for a file based on its content type.
 * <p>
 * Only CSV is supported today, but the selection is content-type driven so that
 * JSON/XML processors can be registered here later without touching callers.
 */
public class StreamingProcessorFactory {

    private StreamingProcessorFactory() {
    }

    /**
     * Return a processor able to handle the given content type, configured from the
     * supplied VFS configuration, or {@code null} if no registered processor matches.
     *
     * @param contentType the resolved MIME type of the file
     * @param config      the inbound VFS configuration carrying streaming parameters
     * @return a matching {@link StreamingProcessor}, or {@code null} if unsupported
     */
    public static StreamingProcessor getProcessor(String contentType, VFSConfig config) {
        CSVStreamingProcessor csvProcessor = new CSVStreamingProcessor(
                config.getStreamingBufferSize(),
                config.getStreamingCsvDelimiter(),
                config.getStreamingCsvQuote(),
                config.isStreamingCsvHasHeader(),
                config.isStreamingAddOutputToVariable(),
                config.isStreamingAddHeadersToEachResult());
        if (csvProcessor.canProcess(contentType)) {
            return csvProcessor;
        }

        // Future formats (JSON, XML, ...) can be checked here.
        return null;
    }
}
