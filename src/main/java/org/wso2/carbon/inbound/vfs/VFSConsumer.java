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

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.core.SynapseEnvironment;
import org.wso2.carbon.inbound.endpoint.protocol.generic.GenericPollingConsumer;
import org.wso2.carbon.inbound.vfs.filter.FileSelector;
import org.wso2.carbon.inbound.vfs.lock.LockManager;
import org.wso2.carbon.inbound.vfs.processor.MoveAction;
import org.wso2.carbon.inbound.vfs.processor.PostProcessingHandler;
import org.wso2.carbon.inbound.vfs.processor.PreProcessingHandler;
import org.wso2.org.apache.commons.vfs2.FileContent;
import org.wso2.org.apache.commons.vfs2.FileObject;
import org.wso2.org.apache.commons.vfs2.FileSystemException;
import org.wso2.org.apache.commons.vfs2.FileSystemManager;
import org.wso2.org.apache.commons.vfs2.FileSystemOptions;
import org.wso2.org.apache.commons.vfs2.FileType;
import org.wso2.org.apache.commons.vfs2.cache.NullFilesCache;
import org.wso2.org.apache.commons.vfs2.impl.StandardFileSystemManager;
import org.wso2.org.apache.commons.vfs2.provider.UriParser;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.wso2.carbon.inbound.vfs.Utils.maskURLPassword;
import static org.wso2.carbon.inbound.vfs.Utils.stripVfsSchemeIfPresent;

public class VFSConsumer extends GenericPollingConsumer {

    private static final Log log = LogFactory.getLog(VFSConsumer.class);
    private static final String EMPTY_MD5 = "d41d8cd98f00b204e9800998ecf8427e";
    private final VFSConfig vfsConfig;
    private final FileSelector fileSelector;
    private final PreProcessingHandler preProcessingHandler;
    private final PostProcessingHandler postProcessingHandler;
    private final FileInjectHandler fileInjectHandler;
    private final String name;
    private FileSystemManager fsManager = null;
    private ScheduledExecutorService retryScheduler;
    private boolean fileLock = true;
    private FileSystemOptions fso;
    private boolean readSubDirectories = false;
    private boolean isClosed;
    private String replyFileURI;
    private String replyFileName;
    private boolean append = false;
    private boolean resolveHostsDynamically = false;
    private Long failedRecordNextRetryDuration = 30000L; // Default 30 seconds
    private boolean isMounted = false;

    private String fileURI;

    public VFSConsumer(Properties properties,
                       String name,
                       SynapseEnvironment synapseEnvironment,
                       long scanInterval,
                       String injectingSeq,
                       String onErrorSeq,
                       boolean coordination,
                       boolean sequential) {
        super(properties, name, synapseEnvironment, scanInterval, injectingSeq, onErrorSeq, coordination, sequential);

        this.vfsConfig = new VFSConfig(properties);

        try {
            StandardFileSystemManager mgr = new StandardFileSystemManager();
            mgr.setClassLoader(getClass().getClassLoader());
            // we don't need to cache files in the consumer side
            mgr.setFilesCache(new NullFilesCache());
            mgr.init();
            this.fsManager = mgr;
        } catch (FileSystemException e) {
            VFSTransportErrorHandler.handleException(log, "Error initializing VFS FileSystemManager", e);
        }

        // Build FSO once per poll (protocol options; auth; SFTP opts; etc.)
        this.fso = null;

        // Initialize new features
        initializeProperties();

        // Initialize retry scheduler for failed records
        this.retryScheduler = Executors.newScheduledThreadPool(1);

        // Handlers (wire your concrete actions here)
        this.fileInjectHandler = new FileInjectHandler(injectingSeq, onErrorSeq, sequential, synapseEnvironment,
                vfsConfig, fsManager);
        this.preProcessingHandler = new PreProcessingHandler();
        this.postProcessingHandler = new PostProcessingHandler();
        int actionAfterProcess = vfsConfig.getActionAfterProcess();
        this.postProcessingHandler.setOnSuccessAction(Utils.getActionAfterProcess(vfsConfig, actionAfterProcess, vfsConfig.getMoveAfterProcess(), fsManager));
        this.postProcessingHandler.setOnFailAction(Utils.getActionAfterProcess(vfsConfig, vfsConfig.getActionAfterFailure(), vfsConfig.getMoveAfterFailure(), fsManager));

        this.fileSelector = new FileSelector(vfsConfig, fsManager);
        this.name = name;

        // Resolve input URI and subdirectory setting from config (supports /* or \*)
        ResolvedFileUri inFileUri = extractFileUri(VFSConstants.TRANSPORT_FILE_FILE_URI);
        if (inFileUri == null || StringUtils.isBlank(inFileUri.resolvedUri)) {
            VFSTransportErrorHandler.handleException(log, "Invalid FileURI. Check configuration. URI: " + maskURLPassword(vfsConfig.getFileURI()));
        }
        readSubDirectories = inFileUri.supportSubDirectories;
        fileURI = stripVfsSchemeIfPresent(inFileUri.resolvedUri);

        // Resolve hosts dynamically if enabled
        if (resolveHostsDynamically) {
            fileURI = resolveHostDynamically(fileURI);
        }
        //Setup SFTP Options
        try {
            fso = Utils.attachFileSystemOptions(Utils.parseSchemeFileOptions(fileURI, properties), fsManager);
        } catch (Exception e) {
            log.warn("Unable to set the sftp Options", e);
            fso = null;
        }
    }

    public VFSConsumer(Properties properties,
                       String name,
                       SynapseEnvironment synapseEnvironment,
                       String cronExpression,
                       String injectingSeq,
                       String onErrorSeq,
                       boolean coordination,
                       boolean sequential) {
        super(properties, name, synapseEnvironment, cronExpression, injectingSeq, onErrorSeq, coordination, sequential);

        this.vfsConfig = new VFSConfig(properties);

        try {
            StandardFileSystemManager mgr = new StandardFileSystemManager();
            mgr.setClassLoader(getClass().getClassLoader());
            // we don't need to cache files in the consumer side
            mgr.setFilesCache(new NullFilesCache());
            mgr.init();
            this.fsManager = mgr;
        } catch (FileSystemException e) {
            VFSTransportErrorHandler.handleException(log, "Error initializing VFS FileSystemManager", e);
        }

        this.fso = null;

        // Initialize new features
        initializeProperties();

        // Initialize retry scheduler for failed records
        this.retryScheduler = Executors.newScheduledThreadPool(1);

        // Handlers (wire your concrete actions here)
        this.fileInjectHandler = new FileInjectHandler(injectingSeq, onErrorSeq, sequential, synapseEnvironment,
                vfsConfig, fsManager);
        this.preProcessingHandler = new PreProcessingHandler();
        this.postProcessingHandler = new PostProcessingHandler();
        int actionAfterProcess = vfsConfig.getActionAfterProcess();
        this.postProcessingHandler.setOnSuccessAction(Utils.getActionAfterProcess(vfsConfig, actionAfterProcess,
                vfsConfig.getMoveAfterProcess(), fsManager));
        this.postProcessingHandler.setOnFailAction(Utils.getActionAfterProcess(vfsConfig,
                vfsConfig.getActionAfterFailure(), vfsConfig.getMoveAfterFailure(), fsManager));

        this.fileSelector = new FileSelector(vfsConfig, fsManager);
        this.name = name;
        // Resolve input URI and subdirectory setting from config (supports /* or \*)
        ResolvedFileUri inFileUri = extractFileUri(VFSConstants.TRANSPORT_FILE_FILE_URI);
        if (inFileUri == null || StringUtils.isBlank(inFileUri.resolvedUri)) {
            VFSTransportErrorHandler.handleException(log, "Invalid FileURI. Check configuration. URI: " + maskURLPassword(vfsConfig.getFileURI()));
        }
        readSubDirectories = inFileUri.supportSubDirectories;
        fileURI = stripVfsSchemeIfPresent(inFileUri.resolvedUri);

        // Resolve hosts dynamically if enabled
        if (resolveHostsDynamically) {
            fileURI = resolveHostDynamically(fileURI);
        }
        //Setup SFTP Options
        try {
            fso = Utils.attachFileSystemOptions(Utils.parseSchemeFileOptions(fileURI, properties), fsManager);
        } catch (Exception e) {
            log.warn("Unable to set the sftp Options", e);
            fso = null;
        }
    }

    /**
     * Initialize new VFS features
     */
    private void initializeProperties() {

        // Reply file configuration
        this.replyFileURI = vfsConfig.getReplyFileURI();
        this.replyFileName = vfsConfig.getReplayFileName();

        // Resolve hosts dynamically
        this.resolveHostsDynamically = vfsConfig.isResolveHostsDynamically();

        // Failed record retry duration
        this.failedRecordNextRetryDuration = vfsConfig.getFailedRecordNextRetryDuration();

        if (log.isDebugEnabled()) {
            log.debug("VFS Consumer initialized with features");
        }
    }

    @Override
    public Object poll() {
        if (isClosed) {
            return null;
        }

        if (log.isDebugEnabled()) {
            log.debug("Polling VFS location: " + maskURLPassword(fileURI) +
                    " (recursive=" + readSubDirectories + ")");
        }

        FileObject root = initFileCheck(fileURI);

        if (root == null) {
            log.error("Resolved FileObject is null for: " + maskURLPassword(fileURI));
            return null;
        }

        try {
            if (root.getType() == FileType.FILE) {
                // Single-file mode
                processFile(root);
            } else if (root.getType() == FileType.FOLDER) {
                // Directory mode (optional recursion)
                processDirectory(root);
            } else {
                // TODO: to handle FileType FILE_OR_FOLDER;
                if (log.isDebugEnabled()) {
                    log.debug("Ignoring non-file, non-folder: " + maskURLPassword(root.toString()));
                }
            }
        } catch (FileSystemException e) {
            log.error("Error during poll for: " + maskURLPassword(fileURI), e);
        } finally {
            safeClose(root);
        }

        return null;
    }

    /* =========================
       Directory & File Handling
       ========================= */

    private void processDirectory(FileObject dir) throws FileSystemException {
        if (isClosed) {
            return;
        }
        FileObject[] children = null;
        int processCount = 0;
        try {
            children = dir.getChildren();
        } catch (FileSystemException e) {
            log.error("Unable to list children of: " + maskURLPassword(dir.toString()), e);
        }

        if (children == null || children.length == 0) {
            if (log.isDebugEnabled()) {
                log.debug("Empty directory: " + maskURLPassword(dir.toString()));
            }
            return;
        }
        // 1. sort to ensure files are processed in order
        String fileSortParam = vfsConfig.getFileSortParam();
        if (StringUtils.isNotEmpty(fileSortParam)) {
            children = Utils.sortFileObjects(children, fileSortParam, vfsConfig);
        }
        for (FileObject child : children) {
            if (isClosed) {
                return;
            }
            try {
                // Skip lock/fail markers
                String base = child.getName().getBaseName();
                if (base.endsWith(".lock") || base.endsWith(".fail")) {
                    continue;
                }

                // Check if this is a failed record
                boolean isFailedRecord = Utils.isFailRecord(fsManager, child, fso) || Utils.isFailedRecordInFailedList(child, vfsConfig);
                if (isFailedRecord) {
                    // Handle failed record with retry mechanism
                    scheduleFailedRecordRetry(child);
                    continue;
                }

                if (child.getType() == FileType.FOLDER) {
                    if (readSubDirectories) {
                        // Recurse only if enabled
                        processDirectory(child);
                    } else if (log.isDebugEnabled()) {
                        log.debug("Skipping subdirectory (recursive disabled): " +
                                maskURLPassword(child.toString()));
                    }
                } else if (child.getType() == FileType.FILE) {
                    processCount++;
                    if (vfsConfig.getFileProcessingInterval() != null && vfsConfig.getFileProcessingInterval() != 0) {
                        // Throttle file processing if configured
                        try {
                            processFile(child);
                            Thread.sleep(vfsConfig.getFileProcessingInterval());
                        } catch (InterruptedException ignore) {
                            if (log.isDebugEnabled()) {
                                log.debug("File processing sleep interrupted");
                            }
                            Thread.currentThread().interrupt();
                        }
                    } else if (vfsConfig.getFileProcessingCount() != null && processCount <= vfsConfig.getFileProcessingCount()) {
                        if (log.isDebugEnabled()) {
                            log.debug("Processing file (count limit " + vfsConfig.getFileProcessingCount() +
                                    "): " + maskURLPassword(child.toString()));
                        }
                        processFile(child);
                    } else if (vfsConfig.getFileProcessingCount() != null && processCount > vfsConfig.getFileProcessingCount()) {
                        if (log.isDebugEnabled()) {
                            log.debug("Skipping file (count limit " + vfsConfig.getFileProcessingCount() +
                                    " reached): " + maskURLPassword(child.toString()));
                        }
                        break;
                    } else {
                        processFile(child);
                    }

                } else if (log.isDebugEnabled()) {
                    log.debug("Ignoring item (not file/folder): " +
                            maskURLPassword(child.toString()));
                }
            } catch (Exception e) {
                // never block remaining files on a single failure
                log.error("Error processing child: " + maskURLPassword(child.toString()), e);
            } finally {
                safeClose(child);
            }
        }
    }

    private void processFile(FileObject file) throws FileSystemException {
        if (isClosed) {
            return;
        }
        file.setIsMounted(vfsConfig.isFileLocking());
        // Delegate readiness, age, size, pattern, etc. to FileSelector
        if (!fileSelector.isValidFile(file)) {
            if (log.isDebugEnabled()) {
                log.debug("File not eligible: " + maskURLPassword(file.toString()));
            }
            return;
        }

        // Acquire lock if file locking is enabled
        fileLock = vfsConfig.isFileLocking();
        LockManager lockManager = new LockManager(fileLock, vfsConfig,
                fsManager, fso);

        if (fileLock && !lockManager.acquireLock(file)) {
            log.error("Couldn't get the lock for processing the file: " +
                    maskURLPassword(file.getName().toString()));
            return;
        }

        try {
            // Pre-processing hook (e.g., acquire lock, tmp rename, etc.)
            preProcessingHandler.handle(file);

            // Build transport headers
            try (FileContent content = file.getContent()) {
                String fileName = file.getName().getBaseName();
                String filePath = file.getName().getPath();
                String fileURI = file.getName().getURI();

                Map<String, Object> headers = new HashMap<>();
                headers.put(VFSConstants.FILE_NAME, fileName);
                headers.put(VFSConstants.FILE_PATH, filePath);
                headers.put(VFSConstants.FILE_URI, fileURI);

                // Add reply file information if configured
                if (StringUtils.isNotEmpty(replyFileURI)) {
                    headers.put(VFSConstants.REPLY_FILE_URI, replyFileURI);
                }
                if (StringUtils.isNotEmpty(replyFileName)) {
                    headers.put(VFSConstants.REPLY_FILE_NAME, replyFileName);
                }

                // Add append mode flag
                headers.put(VFSConstants.APPEND, String.valueOf(append));

                try {
                    headers.put(org.apache.synapse.commons.vfs.VFSConstants.FILE_LENGTH, content.getSize());
                    headers.put(org.apache.synapse.commons.vfs.VFSConstants.LAST_MODIFIED, content.getLastModifiedTime());
                } catch (FileSystemException ignore) {
                    // length/mtime are best-effort
                }

                fileInjectHandler.setTransportHeaders(headers);
                fileInjectHandler.setFileURI(fileURI);

                boolean ok = fileInjectHandler.invoke(file, name);
                if (ok) {
                    if (log.isDebugEnabled()) {
                        log.debug("File processed successfully: " + maskURLPassword(file.toString()));
                    }
                    postProcessingHandler.onSuccess(file);
                } else {
                    // handle the failed records here too
                    if (log.isDebugEnabled()) {
                        log.debug("File processing failed: " + maskURLPassword(file.toString()));
                    }
                    postProcessingHandler.onFail(file);
                }
            }
        } catch (Exception e) {
            log.error("Error processing file: " + maskURLPassword(file.toString()), e);
            try {
                if (vfsConfig.getMoveAfterMoveFailure() != null) {
                    String timeStamp =
                            Utils.getSystemTime(vfsConfig.getFailedRecordTimestampFormat());
                    Utils.addFailedRecord(vfsConfig, file, timeStamp, fsManager);
                } else {
                    Utils.markFailRecord(fsManager, file, fso);
                }
            } catch (Exception failHandlingError) {
                log.error("Error in fail handling for file: " + maskURLPassword(file.toString()), failHandlingError);
                // Mark as failed record if we couldn't handle the failure
                Utils.markFailRecord(fsManager, file, fso);
            }
        } finally {
            // Release lock if file locking is enabled and we shouldn't skip
            if (fileLock) {
                lockManager.releaseLock(file);
                if (log.isDebugEnabled()) {
                    log.debug("Released the lock for file: " + maskURLPassword(file.toString()));
                }
            }
        }
    }

    /**
     * Schedule retry for failed record processing
     */
    private void scheduleFailedRecordRetry(FileObject file) {
        if (failedRecordNextRetryDuration > 0) {
            retryScheduler.schedule(new FailedRecordRetryTask(file),
                    failedRecordNextRetryDuration, TimeUnit.MILLISECONDS);
            if (log.isDebugEnabled()) {
                log.debug("Scheduled retry for failed record: " + maskURLPassword(file.toString()) +
                        " after " + failedRecordNextRetryDuration + "ms");
            }
        }
    }

    /**
     * Remove file from failed records
     */
    private void removeFromFailedRecords(FileObject file) {
        // Implementation to remove from failed records tracking
        if (log.isDebugEnabled()) {
            log.debug("Removed file from failed records: " + maskURLPassword(file.toString()));
        }
        try {
            if (vfsConfig.getMoveAfterMoveFailure() != null) {
                MoveAction moveAction = new MoveAction(vfsConfig.getMoveAfterMoveFailure(), vfsConfig, fsManager);
                moveAction.execute(file);
            }
            //TODO: remove failed record from the list file.
        } catch (FileSystemException e) {
            VFSTransportErrorHandler.handleException(log, "Error moving file during failed record removal: " +
                    maskURLPassword(file.toString()), e);
        }
    }

    /**
     * Resolve hostname dynamically if enabled
     */
    private String resolveHostDynamically(String uri) {
        if (!resolveHostsDynamically) {
            return uri;
        }

        try {
            // Extract hostname from URI and resolve it
            // This is a simplified implementation
            if (uri.contains("://")) {
                String[] parts = uri.split("://");
                if (parts.length > 1) {
                    String[] hostParts = parts[1].split("/");
                    if (hostParts.length > 0) {
                        String host = hostParts[0];
                        if (host.contains("@")) {
                            host = host.substring(host.lastIndexOf("@") + 1);
                        }
                        if (host.contains(":")) {
                            host = host.substring(0, host.indexOf(":"));
                        }

                        // Resolve the hostname
                        InetAddress addr = InetAddress.getByName(host);
                        String resolvedIP = addr.getHostAddress();

                        if (log.isDebugEnabled()) {
                            log.debug("Resolved host " + host + " to " + resolvedIP);
                        }

                        // Replace hostname with resolved IP
                        return uri.replace(host, resolvedIP);
                    }
                }
            }
        } catch (UnknownHostException e) {
            log.warn("Could not resolve host dynamically for URI: " + maskURLPassword(uri), e);
        }

        return uri;
    }

    private void safeClose(FileObject fo) {
        if (fo != null) {
            try {
                fo.close();
            } catch (Exception ignore) {
            }
        }
    }

    /* =========================
                Helpers
       ========================= */

    private ResolvedFileUri extractFileUri(String propertyForUri) {
        String definedFileUri;
        switch (propertyForUri) {
            case VFSConstants.TRANSPORT_FILE_FILE_URI:
                definedFileUri = vfsConfig.getFileURI();
                break;
            default:
                definedFileUri = null;
        }
        if (StringUtils.isNotEmpty(definedFileUri)) {
            if (Utils.supportsSubDirectoryToken(definedFileUri)) {
                return new ResolvedFileUri(Utils.sanitizeFileUriWithSub(definedFileUri), true);
            } else {
                return new ResolvedFileUri(definedFileUri, false);
            }
        }
        return null;
    }

    /**
     * Check if the file/folder exists before proceeding and retrying
     */
    private FileObject initFileCheck(String fileURI) {
        boolean wasError = true;
        int retryCount = 0;

        FileObject fileObject = null;
        while (wasError) {
            try {
                if (isClosed) {
                    return null;
                }
                retryCount++;
                fileObject = fsManager.resolveFile(fileURI, fso);
                if (fileObject == null) {
                    log.error("fileObject is null");
                    throw new FileSystemException("fileObject is null");
                }
                Map<String, String> queryParams = UriParser.extractQueryParams(fileURI);
                isMounted = Boolean.parseBoolean(queryParams.get(VFSConstants.IS_MOUNTED));
                fileObject.setIsMounted(isMounted);
                wasError = false;
            } catch (FileSystemException e) {
                if (retryCount >= vfsConfig.getMaxRetryCount()) {
                    log.error("Repeatedly failed to resolve the file URI: " + maskURLPassword(fileURI), e);
                    return null;
                } else {
                    log.warn("Failed to resolve the file URI: " + maskURLPassword(fileURI) + ", in attempt "
                            + retryCount + ", " + e.getMessage() + " Retrying in " + vfsConfig.getReconnectTimeout()
                            + " milliseconds.");
                }
            }
            if (wasError) {
                try {
                    Thread.sleep(vfsConfig.getReconnectTimeout());
                } catch (InterruptedException e2) {
                    Thread.currentThread().interrupt();
                    log.error("Thread was interrupted while waiting to reconnect.", e2);
                }
            }
        }
        return fileObject;
    }

    public void close() {
        isClosed = true;
        fsManager.close();
        if (retryScheduler != null && !retryScheduler.isShutdown()) {
            retryScheduler.shutdown();
            try {
                if (!retryScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    retryScheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                retryScheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    public void start() {
        isClosed = false;
    }

    public void destroy() {
        this.close();
    }

    @Override
    public void resume() {
        try {
            ((StandardFileSystemManager) fsManager).init();
            retryScheduler = Executors.newScheduledThreadPool(1);
            isClosed = false;
        } catch (FileSystemException e) {
            log.error("Error re-initializing VFS FileSystemManager on resume", e);
        }
    }

    @Override
    public void pause() {
        isClosed = true;
        this.close();
    }

    private static class ResolvedFileUri {
        final String resolvedUri;
        final boolean supportSubDirectories;

        ResolvedFileUri(String uri, boolean subDirs) {
            this.resolvedUri = uri;
            this.supportSubDirectories = subDirs;
        }
    }

    /**
     * Task to retry processing of failed records
     */
    private class FailedRecordRetryTask implements Runnable {
        private final FileObject file;

        public FailedRecordRetryTask(FileObject file) {
            this.file = file;
        }

        @Override
        public void run() {
            try {
                if (log.isDebugEnabled()) {
                    log.debug("Retrying failed record: " + maskURLPassword(file.toString()));
                }

                // Check if file still exists
                if (file.exists()) {
                    // Remove from failed records and retry processing
                    removeFromFailedRecords(file);
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("Failed record file no longer exists: " + maskURLPassword(file.toString()));
                    }
                }
            } catch (Exception e) {
                log.error("Error during failed record retry: " + maskURLPassword(file.toString()), e);
                // Schedule another retry if configured
                scheduleFailedRecordRetry(file);
            }
        }
    }

}

