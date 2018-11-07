package sdfs.namenode

import sdfs.datanode.DataNode
import sdfs.exception.SDFSFileAlreadyExistsException
import sdfs.protocol.INameNodeProtocol
import sdfs.protocol.SDFSConfiguration
import spock.lang.Specification

import java.nio.file.Files
import java.nio.file.Paths

import static sdfs.Util.generateFilename
import static sdfs.Util.generatePort

class LogTest extends Specification {
    def dir1 = File.createTempDir().absolutePath
    def dir2 = File.createTempDir().absolutePath
    def dir3 = File.createTempDir().absolutePath
    def dir4 = File.createTempDir().absolutePath

    def "Log"() {
        System.setProperty("sdfs.namenode.dir", dir1)
        SDFSConfiguration configuration = new SDFSConfiguration(InetAddress.getLocalHost(), generatePort(), InetAddress.getLocalHost(), generatePort())
        INameNodeProtocol nameNode = new NameNode(configuration, 2)
        sleep(3000)
        Files.copy(Paths.get(dir1, "root.node"), Paths.get(dir2, "root.node"))
        def parentDir = generateFilename()
        nameNode.mkdir(parentDir)
        for (int i = 0; i < 10; i++)
            nameNode.mkdir(parentDir += "/" + generateFilename())
        def dirName = generateFilename()
        def filename = generateFilename()
        def filename2 = generateFilename()
        nameNode.mkdir("$parentDir/$dirName")
        def accessToken = nameNode.create("$parentDir/$filename").token
        def locatedBlock = nameNode.addBlocks(accessToken, 1)[0]
        def accessToken2 = nameNode.create("$parentDir/$filename2").token
        def locatedBlock2 = nameNode.addBlocks(accessToken2, 3)[0]
        nameNode.removeLastBlocks(accessToken2, 1)
        def copyOnWriteBlock2 = nameNode.newCopyOnWriteBlock(accessToken2, 1)
        sleep(3000)
        nameNode.closeReadwriteFile(accessToken, 1)
        nameNode.closeReadwriteFile(accessToken2, DataNode.BLOCK_SIZE * 2)
        Files.copy(Paths.get(dir1, "root.node"), Paths.get(dir3, "root.node"))
        Files.copy(Paths.get(dir1, "namenode.log"), Paths.get(dir3, "namenode.log"))
        sleep(3000)
        Files.copy(Paths.get(dir1, "root.node"), Paths.get(dir4, "root.node"))
        System.setProperty("sdfs.namenode.dir", dir2)
        def nameNode2 = new NameNode(configuration, 3)
        System.setProperty("sdfs.namenode.dir", dir3)
        def nameNode3 = new NameNode(configuration, 3)
        System.setProperty("sdfs.namenode.dir", dir4)
        def nameNode4 = new NameNode(configuration, 3)

        when:
        nameNode2.mkdir("$parentDir/$dirName")

        then:
        thrown(FileNotFoundException)

        when:
        nameNode2.create("$parentDir/$filename")

        then:
        thrown(FileNotFoundException)

        when:
        nameNode3.mkdir("$parentDir/$dirName")

        then:
        thrown(SDFSFileAlreadyExistsException)

        when:
        nameNode3.create("$parentDir/$filename")

        then:
        thrown(SDFSFileAlreadyExistsException)

        when:
        def fileInfo = nameNode3.openReadonly("$parentDir/$filename").fileInfo

        then:
        fileInfo.fileSize == 1
        fileInfo.blockInfoList[0][0] == locatedBlock

        when:
        def fileInfo2 = nameNode3.openReadonly("$parentDir/$filename2").fileInfo

        then:
        fileInfo2.fileSize == DataNode.BLOCK_SIZE * 2
        fileInfo2.blockInfoList[0][0] == locatedBlock2
        fileInfo2.blockInfoList[1][0] == copyOnWriteBlock2

        when:
        nameNode4.mkdir("$parentDir/$dirName")

        then:
        thrown(SDFSFileAlreadyExistsException)

        when:
        nameNode4.create("$parentDir/$filename")

        then:
        thrown(SDFSFileAlreadyExistsException)

        when:
        def fileInfo3 = nameNode4.openReadonly("$parentDir/$filename").fileInfo

        then:
        fileInfo3.fileSize == 1
        fileInfo3.blockInfoList[0][0] == locatedBlock

        when:
        def fileInfo4 = nameNode4.openReadonly("$parentDir/$filename2").fileInfo

        then:
        fileInfo4.fileSize == DataNode.BLOCK_SIZE * 2
        fileInfo4.blockInfoList[0][0] == locatedBlock2
        fileInfo4.blockInfoList[1][0] == copyOnWriteBlock2
    }
}

