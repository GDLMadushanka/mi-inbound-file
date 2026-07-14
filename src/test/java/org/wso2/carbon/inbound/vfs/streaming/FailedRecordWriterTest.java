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

import org.junit.Assert;
import org.junit.Test;

public class FailedRecordWriterTest {

    @Test
    public void testFailedRecordsFileNaming() {
        // A '.fail' marker is inserted before the last extension.
        Assert.assertEquals("input.fail.jsonl",
                FailedRecordWriter.toFailedRecordsFileName("input.jsonl"));
        Assert.assertEquals("data.fail.csv",
                FailedRecordWriter.toFailedRecordsFileName("data.csv"));
        // Multiple dots: only the last extension is preserved after the marker.
        Assert.assertEquals("archive.2026.fail.json",
                FailedRecordWriter.toFailedRecordsFileName("archive.2026.json"));
        // No extension: the marker is appended.
        Assert.assertEquals("records.fail",
                FailedRecordWriter.toFailedRecordsFileName("records"));
        // Leading-dot (hidden) file: treated as a name, not an extension.
        Assert.assertEquals(".gitignore.fail",
                FailedRecordWriter.toFailedRecordsFileName(".gitignore"));
    }
}
