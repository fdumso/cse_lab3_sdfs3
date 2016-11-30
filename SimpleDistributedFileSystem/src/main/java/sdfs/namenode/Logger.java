package sdfs.namenode;

import sdfs.namenode.log.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class Logger {
    private ObjectOutputStream oos;
    private ReentrantLock logLock = new ReentrantLock();
    private AtomicInteger id = new AtomicInteger(0);
    private AtomicInteger onActionCounter = new AtomicInteger(0);
    private ReentrantLock flushLock = new ReentrantLock();

    Logger(String logPath, NameNode nameNode) {
        File logFile = new File(logPath);
        if (logFile.exists()) {
            // re-construct name node context from previous log file
            try {
                FileInputStream fileInputStream = new FileInputStream(logFile);
                ObjectInputStream ois = new ObjectInputStream(fileInputStream);
                ArrayList<Log> logList = new ArrayList<>();
                // load all the log into a list
                while (true) {
                    try {
                        logList.add((Log) ois.readObject());
                    } catch (EOFException e) {
                        break;
                    }
                }

                // find the last time when CHECK POINT log appears
                int lastCheckPointIndex = 0;
                for (int i = logList.size()-1; i >= 0; i--) {
                    if (logList.get(i).getType()== Log.Type.CHECK_POINT) {
                        lastCheckPointIndex = i;
                        break;
                    }
                }

                // for the log before check point

                Map<Integer, Log> possibleLogMap = new HashMap<>();
                List<Log> committedLog = new ArrayList<>();
                for (int i = 0; i < lastCheckPointIndex; i++) {
                    Log currentLog = logList.get(i);
                    switch (currentLog.getType()) {
                        case START: {
                            break;
                        }
                        case ABORT: {
                            possibleLogMap.remove(currentLog.getId());
                            break;
                        }
                        case COMMIT: {
                            committedLog.add(possibleLogMap.get(currentLog.getId()));
                            possibleLogMap.remove(currentLog.getId());
                            break;
                        }
                        case CHECK_POINT: {
                            break;
                        }
                        default: {
                            possibleLogMap.put(currentLog.getId(), currentLog);
                            break;
                        }
                    }
                }

                assert possibleLogMap.isEmpty();

                // extract out all unclosed file and its logs
                Map<UUID, OpenReadLog> openedReadMap = new HashMap<>();
                Map<UUID, List<Log>> openedWriteMap = new HashMap<>();
                for (Log currentLog : committedLog) {
                    switch (currentLog.getType()) {
                        case OPEN_READ: {
                            openedReadMap.put(((OpenReadLog) currentLog).getToken(), (OpenReadLog) currentLog);
                            break;
                        }
                        case OPEN_WRITE: {
                            List<Log> writeList = new ArrayList<>();
                            writeList.add(currentLog);
                            openedWriteMap.put(((OpenWriteLog) currentLog).getToken(), writeList);
                            break;
                        }
                        case CREATE: {
                            List<Log> writeList = new ArrayList<>();
                            writeList.add(currentLog);
                            openedWriteMap.put(((CreateFileLog) currentLog).getToken(), writeList);
                            break;
                        }
                        case COPY_ON_WRITE_BLOCK: {
                            List<Log> logs = openedWriteMap.get(((CopyOnWriteBlockLog) currentLog).getToken());
                            logs.add(currentLog);
                            break;
                        }
                        case ADD_BLOCKS: {
                            List<Log> logs = openedWriteMap.get(((AddBlocksLog) currentLog).getToken());
                            logs.add(currentLog);
                            break;
                        }
                        case REMOVE_BLOCKS: {
                            List<Log> logs = openedWriteMap.get(((RemoveBlocksLog) currentLog).getToken());
                            logs.add(currentLog);
                            break;
                        }
                        case CLOSE_WRITE: {
                            openedWriteMap.remove(((CloseWriteLog) currentLog).getToken());
                            break;
                        }
                        case CLOSE_READ: {
                            openedReadMap.remove(((CloseReadLog) currentLog).getToken());
                            break;
                        }
                        default: break;
                    }
                }


                // re-open read-only file
                if (openedReadMap.values().size() > 0) {
                    for (OpenReadLog l : openedReadMap.values()) {
                        nameNode.redoOpenReadonly(l.getFileUri(), l.getToken());
                    }
                }

                // re-open read-write file and redo the action
                if (openedWriteMap.values().size() > 0) {
                    for (List<Log> list : openedWriteMap.values()) {
                        for (Log l : list) {
                            switch (l.getType()) {
                                case OPEN_WRITE: {
                                    nameNode.redoOpenReadwrite(((OpenWriteLog) l).getFileUri(), ((OpenWriteLog) l).getToken());
                                    break;
                                }
                                case CREATE: {
                                    nameNode.redoOpenReadwrite(((CreateFileLog) l).getFileUri(), ((CreateFileLog) l).getToken());
                                    break;
                                }
                                case REMOVE_BLOCKS: {
                                    nameNode.redoRemoveBlocks(((RemoveBlocksLog) l).getToken(), ((RemoveBlocksLog) l).getBlockAmount());
                                    break;
                                }
                                case ADD_BLOCKS: {
                                    nameNode.redoAddBlocks(((AddBlocksLog) l).getToken(), ((AddBlocksLog) l).getNewBlockNumberList());
                                    break;
                                }
                                case COPY_ON_WRITE_BLOCK: {
                                    nameNode.redoNewCopyOnWriteBlock(((CopyOnWriteBlockLog) l).getToken(), ((CopyOnWriteBlockLog) l).getFileBlockNumber(), ((CopyOnWriteBlockLog) l).getNewBlockNumber());
                                }
                            }
                        }
                    }
                }

                // for the log after check point

                // first extract all committed transaction
                possibleLogMap = new HashMap<>();
                committedLog = new ArrayList<>();
                for (int i = lastCheckPointIndex+1; i < logList.size(); i++) {
                    Log currentLog = logList.get(i);
                    switch (currentLog.getType()) {
                        case START: {
                            break;
                        }
                        case ABORT: {
                            possibleLogMap.remove(currentLog.getId());
                            break;
                        }
                        case COMMIT: {
                            committedLog.add(possibleLogMap.get(currentLog.getId()));
                            break;
                        }
                        default: {
                            possibleLogMap.put(currentLog.getId(), currentLog);
                            break;
                        }
                    }
                }

                // redo all the committed logs
                for (Log currentLog : committedLog) {
                    switch (currentLog.getType()) {
                        case OPEN_READ: {
                            nameNode.redoOpenReadonly(((OpenReadLog) currentLog).getFileUri(), ((OpenReadLog) currentLog).getToken());
                            break;
                        }
                        case OPEN_WRITE: {
                            nameNode.redoOpenReadwrite(((OpenWriteLog) currentLog).getFileUri(), ((OpenWriteLog) currentLog).getToken());
                            break;
                        }
                        case CREATE: {
                            nameNode.redoCreate(((CreateFileLog) currentLog).getFileUri(), ((CreateFileLog) currentLog).getToken());
                            break;
                        }
                        case COPY_ON_WRITE_BLOCK: {
                            nameNode.redoNewCopyOnWriteBlock(((CopyOnWriteBlockLog) currentLog).getToken(), ((CopyOnWriteBlockLog) currentLog).getFileBlockNumber(), ((CopyOnWriteBlockLog) currentLog).getNewBlockNumber());
                            break;
                        }
                        case ADD_BLOCKS: {
                            nameNode.redoAddBlocks(((AddBlocksLog) currentLog).getToken(), ((AddBlocksLog) currentLog).getNewBlockNumberList());
                            break;
                        }
                        case REMOVE_BLOCKS: {
                            nameNode.redoRemoveBlocks(((RemoveBlocksLog) currentLog).getToken(), ((RemoveBlocksLog) currentLog).getBlockAmount());
                            break;
                        }
                        case CLOSE_WRITE: {
                            nameNode.redoCloseReadwrite(((CloseWriteLog) currentLog).getToken(), ((CloseWriteLog) currentLog).getNewFileSize());
                            break;
                        }
                        case CLOSE_READ: {
                            nameNode.redoCloseReadonly(((CloseReadLog) currentLog).getToken());
                            break;
                        }
                        case MK_DIR: {
                            nameNode.redoMkdir(((MkdirLog) currentLog).getFileUri());
                            break;
                        }
                        default:
                            break;
                    }
                }

            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            }
        } else {
            try {
                logFile.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        try {
            FileOutputStream fileOutputStream = new FileOutputStream(logFile, true);
            oos = new ObjectOutputStream(fileOutputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void writeLog(Log log) {
        logLock.lock();
        try {
            oos.writeObject(log);
            oos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
        logLock.unlock();
    }


    int start() {
        flushLock.lock();
        onActionCounter.incrementAndGet();
        int newID = id.incrementAndGet();
        StartLog startLog = new StartLog(newID);
        writeLog(startLog);
        flushLock.unlock();
        return newID;
    }

    void abort(int logID) {
        AbortLog abortLog = new AbortLog(logID);
        writeLog(abortLog);
        onActionCounter.decrementAndGet();
    }

    void commit(int logID) {
        CommitLog commitLog = new CommitLog(logID);
        writeLog(commitLog);
        onActionCounter.decrementAndGet();
    }

    void prepToFlush() {
        flushLock.lock();
        while (onActionCounter.get() != 0) {
            // wait until all other action is finished
        }
    }

    void checkPoint() {
        int newID = id.incrementAndGet();
        CheckPointLog checkPointLog = new CheckPointLog(newID);
        writeLog(checkPointLog);
        flushLock.unlock();
    }

    void openRead(int logID, String fileUri, UUID token) {
        OpenReadLog openReadLog = new OpenReadLog(logID, fileUri, token);
        writeLog(openReadLog);
    }

    void openWrite(int logID, String fileUri, UUID token) {
        OpenWriteLog openWriteLog = new OpenWriteLog(logID, fileUri, token);
        writeLog(openWriteLog);
    }

    void create(int logID, String fileUri, UUID token) {
        CreateFileLog createFileLog = new CreateFileLog(logID, fileUri, token);
        writeLog(createFileLog);
    }

    void mkdir(int logID, String fileUri) {
        MkdirLog mkdirLog = new MkdirLog(logID, fileUri);
        writeLog(mkdirLog);
    }

    void closeRead(int logID, UUID token) {
        CloseReadLog closeReadLog = new CloseReadLog(logID, token);
        writeLog(closeReadLog);
    }

    void closeWrite(int logID, UUID token, long newFileSize) {
        CloseWriteLog closeWriteLog = new CloseWriteLog(logID, token, newFileSize);
        writeLog(closeWriteLog);
    }

    void addBlocks(int logID, UUID token, List<Integer> newBlockNumberList) {
        AddBlocksLog addBlocksLog = new AddBlocksLog(logID, token, newBlockNumberList);
        writeLog(addBlocksLog);
    }

    void removeBlocks(int logID, UUID token, int blockAmount) {
        RemoveBlocksLog removeBlocksLog = new RemoveBlocksLog(logID, token, blockAmount);
        writeLog(removeBlocksLog);
    }

    void copyOnWriteBlock(int logID, UUID token, int fileBlockNumber, int newBlockNumber) {
        CopyOnWriteBlockLog copyOnWriteBlockLog = new CopyOnWriteBlockLog(logID, token, fileBlockNumber, newBlockNumber);
        writeLog(copyOnWriteBlockLog);
    }
}
