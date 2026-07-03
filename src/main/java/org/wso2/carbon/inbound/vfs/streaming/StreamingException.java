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

public class StreamingException extends Exception {

    private long rowNumber;
    private boolean isRecoverable;

    public StreamingException(String message) {
        super(message);
        this.rowNumber = -1;
        this.isRecoverable = true;
    }

    public StreamingException(String message, Throwable cause) {
        super(message, cause);
        this.rowNumber = -1;
        this.isRecoverable = true;
    }

    public StreamingException(String message, Throwable cause, long rowNumber, boolean isRecoverable) {
        super(message, cause);
        this.rowNumber = rowNumber;
        this.isRecoverable = isRecoverable;
    }

    public long getRowNumber() {
        return rowNumber;
    }

    public boolean isRecoverable() {
        return isRecoverable;
    }

    @Override
    public String toString() {
        return "StreamingException{" +
                "message='" + getMessage() + '\'' +
                ", rowNumber=" + rowNumber +
                ", isRecoverable=" + isRecoverable +
                '}';
    }
}
