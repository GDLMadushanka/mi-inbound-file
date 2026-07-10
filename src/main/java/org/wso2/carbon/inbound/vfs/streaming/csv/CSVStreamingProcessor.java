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

package org.wso2.carbon.inbound.vfs.streaming.csv;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.inbound.vfs.streaming.ChunkedDataProcessor;
import org.wso2.carbon.inbound.vfs.streaming.StreamChunk;
import org.wso2.carbon.inbound.vfs.streaming.StreamRecord;
import org.wso2.carbon.inbound.vfs.streaming.StreamingException;

public class CSVStreamingProcessor extends ChunkedDataProcessor {

    private static final Log log = LogFactory.getLog(CSVStreamingProcessor.class);

    private char delimiter = ',';
    private char quoteChar = '"';
    private boolean hasHeader = true;
    private int chunkSize = 1;  // Number of rows per chunk in batch mode.
    private boolean addOutputToVariable = false;  // Add output to variable (metadata) of body
    private boolean addHeadersToEachResult = true; // Add header to each result row or chunk

    public CSVStreamingProcessor(int bufferSize, char delimiter, char quoteChar, boolean hasHeader,
        boolean addOutputToVariable, boolean addHeadersToEachResult) {
        super(bufferSize);
        this.delimiter = delimiter;
        this.quoteChar = quoteChar;
        this.hasHeader = hasHeader;
        this.addOutputToVariable = addOutputToVariable;
        this.addHeadersToEachResult = addHeadersToEachResult;
    }

    @Override
    public Iterator<StreamChunk> getChunkIterator(InputStream input, String contentType, int chunkSize)
        throws StreamingException {
        this.chunkSize = Math.max(1, chunkSize);
        try {
            Charset charset = detectCharset(contentType);
            BufferedReader reader = new BufferedReader(
                new InputStreamReader(input, charset),
                bufferSize
            );

            return new ChunkIterator(reader, charset);

        } catch (Exception e) {
            throw new StreamingException("Failed to initialize CSV processor", e, 0, false);
        }
    }

    @Override
    public Iterator<StreamRecord> getRecordIterator(InputStream input, String contentType)
        throws StreamingException {
        try {
            Charset charset = detectCharset(contentType);
            BufferedReader reader = new BufferedReader(
                new InputStreamReader(input, charset),
                bufferSize
            );

            return new RowIterator(reader, charset);

        } catch (Exception e) {
            throw new StreamingException("Failed to initialize CSV processor", e, 0, false);
        }
    }

    /**
     * Build CSVFormat with configured delimiter, quote character, and header handling.
     */
    private CSVFormat buildCSVFormat() {
        CSVFormat.Builder builder = CSVFormat.DEFAULT.builder()
            .setDelimiter(delimiter)
            .setQuote(quoteChar)
            .setIgnoreEmptyLines(true)
            .setTrim(false);

        if (hasHeader) {
            builder = builder
                .setHeader()
                .setSkipHeaderRecord(true);
        }

        return builder.get();
    }

    /**
     * Build record content as a delimiter-separated string.
     * Fields are re-quoted per RFC 4180 so the reconstruction is a faithful, non-lossy
     * round-trip: a field is wrapped in quotes if it contains the delimiter, the quote
     * character, or a line break, and any embedded quote characters are doubled.
     */
    private String buildRecordContent(CSVRecord record) {
        if (record.size() == 0) {
            return "";
        }
        StringBuilder sb = new StringBuilder(quoteField(record.get(0)));
        for (int i = 1; i < record.size(); i++) {
            sb.append(delimiter).append(quoteField(record.get(i)));
        }
        return sb.toString();
    }

    /**
     * Quote a single CSV field per RFC 4180 if it contains the delimiter, the quote
     * character, or a line break. Embedded quote characters are escaped by doubling.
     */
    private String quoteField(String field) {
        boolean needsQuoting = field.indexOf(delimiter) >= 0
            || field.indexOf(quoteChar) >= 0
            || field.indexOf('\n') >= 0
            || field.indexOf('\r') >= 0;

        if (!needsQuoting) {
            return field;
        }

        String escaped = field.replace(String.valueOf(quoteChar),
            String.valueOf(quoteChar) + quoteChar);
        return quoteChar + escaped + quoteChar;
    }

    /**
     * Iterator for batch mode: yields chunks containing multiple records. Guarantees complete,
     * valid records without split fields or partial rows. Each chunk contains up to chunkSize
     * rows.
     */
    private class ChunkIterator implements Iterator<StreamChunk> {

        private final Iterator<CSVRecord> csvIterator;
        private String[] headers;
        private long recordCount = 0;
        private boolean eof = false;
        private CSVRecord nextRecord;
        private final Charset charset;

        ChunkIterator(BufferedReader reader, Charset charset) throws IOException, StreamingException {
            this.charset = charset;
            try {
                CSVFormat csvFormat = buildCSVFormat();
                CSVParser csvParser = csvFormat.parse(reader);
                this.csvIterator = csvParser.iterator();

                if (hasHeader) {
                    this.headers = csvParser.getHeaderNames().toArray(new String[0]);
                    if (log.isDebugEnabled()) {
                        log.debug("CSV headers parsed: " + csvParser.getHeaderNames().size()
                            + " columns");
                    }
                }

                // Try to read first record
                advanceToNextRecord();

            } catch (IOException e) {
                throw new StreamingException("Failed to parse CSV headers", e, 0, false);
            }
        }

        private void advanceToNextRecord() {
            if (csvIterator.hasNext()) {
                nextRecord = csvIterator.next();
            } else {
                nextRecord = null;
                eof = true;
            }
        }

        @Override
        public boolean hasNext() {
            return !eof && nextRecord != null;
        }

        @Override
        public StreamChunk next() {
            if (!hasNext()) {
                throw new NoSuchElementException("No more records to iterate");
            }
            try {
                return getNextBatchChunk();
            } catch (Exception e) {
                StreamChunk errorChunk = new StreamChunk(chunkSize);
                errorChunk.setValid(false);
                errorChunk.setParseError("CSV processing error: " + e.getMessage());
                if (log.isWarnEnabled()) {
                    log.warn("CSV processing error at record " + recordCount, e);
                }
                return errorChunk;
            }
        }

        /**
         * Get next chunk in batch mode. Each StreamChunk contains up to chunkSize
         * rows. Headers are stored on the chunk, not on individual records (unless configured).
         */
        private StreamChunk getNextBatchChunk() {
            StreamChunk chunk = new StreamChunk(chunkSize);
            chunk.setEncoding(charset);

            if (addOutputToVariable) {
                if (addHeadersToEachResult && headers != null && headers.length > 0) {
                    // [{"ID":"1","Name":"John"},{"ID":"2","Name":"Jane"}]
                    chunk.putMetadata("payload", new ArrayList<HashMap<String, Object>>());
                } else {
                    // [["1","John"],["2","Jane"]]
                    chunk.putMetadata("payload", new ArrayList<ArrayList<String>>());
                }
            }

            int rowsInChunk = 0;
            while (rowsInChunk < chunkSize && hasNext()) {
                recordCount++;
                CSVRecord record = nextRecord;
                advanceToNextRecord();
                rowsInChunk++;

                // Create StreamRecord for this row
                StreamRecord streamRecord = new StreamRecord(recordCount + (hasHeader ? 1 : 0));
                streamRecord.setEncoding(charset);
                streamRecord.setValid(true);

                if (addOutputToVariable) {
                    if (addHeadersToEachResult && headers != null && headers.length > 0) {
                        @SuppressWarnings("unchecked")
                        ArrayList<HashMap<String, Object>> payload =
                            (ArrayList<HashMap<String, Object>>)chunk.getMetadata().get("payload");
                        HashMap<String, Object> recordMetadata = new HashMap<>();
                        for (int i = 0; i < headers.length && i < record.size(); i++) {
                            recordMetadata.put(headers[i], record.get(i));
                        }
                        payload.add(rowsInChunk-1, recordMetadata);
                    } else {
                        // If no headers, store the row as a list of values
                        @SuppressWarnings("unchecked")
                        ArrayList<ArrayList<String>> payload =
                            (ArrayList<ArrayList<String>>)chunk.getMetadata().get("payload");
                        ArrayList<String> rowData = new ArrayList<>();
                        for (int i = 0; i < record.size(); i++) {
                            rowData.add(record.get(i));
                        }
                        payload.add(rowsInChunk -1, rowData);
                    }
                } else {
                    String recordContent = buildRecordContent(record);
                    streamRecord.setContent(recordContent.getBytes(charset));
                }

                chunk.addRecord(streamRecord);
            }

            chunk.setRecordCount(rowsInChunk);
            chunk.setLastChunk(!hasNext());

            if (log.isDebugEnabled()) {
                log.debug("Batch mode: Chunk with " + rowsInChunk + " rows (rows " +
                    chunk.getFirstRecordNumber() + "-" + chunk.getLastRecordNumber() + ")");
            }

            return chunk;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Remove not supported");
        }
    }

    /**
     * Iterator for row-by-row mode: yields individual StreamRecords. Guarantees complete, valid
     * records without split fields or partial rows. Each iteration returns a single StreamRecord.
     */
    private class RowIterator implements Iterator<StreamRecord> {

        private final Charset charset;
        private final Iterator<CSVRecord> csvIterator;
        private String[] headers;
        private long recordCount = 0;
        private boolean eof = false;
        private CSVRecord nextRecord;

        RowIterator(BufferedReader reader, Charset charset) throws IOException, StreamingException {
            this.charset = charset;

            try {
                CSVFormat csvFormat = buildCSVFormat();
                CSVParser csvParser = csvFormat.parse(reader);
                this.csvIterator = csvParser.iterator();

                if (hasHeader) {
                    this.headers = csvParser.getHeaderNames().toArray(new String[0]);
                    if (log.isDebugEnabled()) {
                        log.debug("CSV headers parsed: " + csvParser.getHeaderNames().size()
                            + " columns");
                    }
                }

                advanceToNextRecord();

            } catch (IOException e) {
                throw new StreamingException("Failed to parse CSV headers", e, 0, false);
            }
        }

        private void advanceToNextRecord() {
            if (csvIterator.hasNext()) {
                nextRecord = csvIterator.next();
            } else {
                nextRecord = null;
                eof = true;
            }
        }

        @Override
        public boolean hasNext() {
            return !eof && nextRecord != null;
        }

        @Override
        public StreamRecord next() {
            if (!hasNext()) {
                throw new NoSuchElementException("No more records to iterate");
            }

            recordCount++;
            CSVRecord record = nextRecord;
            advanceToNextRecord();

            try {
                StreamRecord streamRecord = new StreamRecord(recordCount + (hasHeader ? 1 : 0));
                streamRecord.setEncoding(charset);
                streamRecord.setValid(true);

                if (addOutputToVariable) {
                    if (headers != null && headers.length > 0) {
                        Map<String, String> payload = new HashMap<>();
                        for (int i = 0; i < headers.length && i < record.size(); i++) {
                            payload.put(headers[i], record.get(i));
                        }
                        // {"ID":"1","Name":"John"}
                        streamRecord.putMetadata("payload", payload);
                    } else {
                        // If no headers, store the row as a list of values
                        List<String> payload = new ArrayList<>();
                        for (int i = 0; i < record.size(); i++) {
                            payload.add(record.get(i));
                        }
                        // ["1","John"]
                        streamRecord.putMetadata("payload", payload);
                    }
                } else {
                    String recordContent = buildRecordContent(record);
                    if (addHeadersToEachResult && headers != null && headers.length > 0) {
                        StringBuilder sb = new StringBuilder();
                        for (int i = 0; i < headers.length; i++) {
                            sb.append(headers[i]);
                            if (i < headers.length - 1) {
                                sb.append(delimiter);
                            }
                        }
                        sb.append(System.lineSeparator());
                        sb.append(recordContent);
                        recordContent = sb.toString();
                    }
                    streamRecord.setContent(recordContent.getBytes(charset));
                }

                if (log.isDebugEnabled()) {
                    log.debug(
                        "Row-by-row mode: Record " + streamRecord.getRecordNumber() + " parsed");
                }
                return streamRecord;

            } catch (Exception e) {
                StreamRecord errorRecord = new StreamRecord(recordCount + (hasHeader ? 1 : 0));
                errorRecord.setValid(false);
                errorRecord.setParseError("CSV parsing error: " + e.getMessage());

                if (log.isWarnEnabled()) {
                    log.warn("CSV parsing error at record " + errorRecord.getRecordNumber(), e);
                }
                return errorRecord;
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Remove not supported");
        }
    }
}
