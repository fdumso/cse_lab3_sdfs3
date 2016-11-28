package sdfs.client

import sdfs.datanode.DataNode
import sdfs.datanode.DataNodeServer
import sdfs.namenode.NameNodeServer
import sdfs.protocol.SDFSConfiguration
import spock.lang.Shared
import spock.lang.Specification

import java.nio.ByteBuffer
import java.nio.channels.OverlappingFileLockException

import static sdfs.Util.*

class SDFSClientTest extends Specification {
    @Shared
    NameNodeServer nameNodeServer
    @Shared
    DataNodeServer dataNodeServer
    @Shared
    SDFSClient client
    @Shared
    def FILE_SIZE = 2 * DataNode.BLOCK_SIZE + 2
    @Shared
    def dataBuffer = ByteBuffer.allocate(FILE_SIZE)
    @Shared
    def buffer = ByteBuffer.allocate(FILE_SIZE)
    def parentDir = generateFilename()
    def filename = parentDir + "/" + generateFilename()

    def setupSpec() {
        System.setProperty("sdfs.namenode.dir", File.createTempDir().absolutePath)
        System.setProperty("sdfs.datanode.dir", File.createTempDir().absolutePath)
        SDFSConfiguration configuration = new SDFSConfiguration(InetAddress.getLocalHost(), generatePort(), InetAddress.getLocalHost(), generatePort())
        nameNodeServer = new NameNodeServer(configuration, 10)
        dataNodeServer = new DataNodeServer(configuration)
        new Thread(nameNodeServer).start()
        new Thread(dataNodeServer).start()
        client = new SDFSClient(configuration, 3)
        for (int i = 0; i < FILE_SIZE; i++)
            dataBuffer.put(i.byteValue())
    }

    def setup() {
        client.mkdir(parentDir)
    }

    private def writeData() {
        def fc = client.create(filename)
        dataBuffer.position(0)
        fc.write(dataBuffer)
        fc.close()
    }

    def "Test multi client basic"() {
        def fileSize = FILE_SIZE
        // 创建一个新文件，将dataBuffer中的数据写入
        when:
        def fc = client.create("$filename")
        dataBuffer.position(0)

        then:
        fc.write(dataBuffer) == fileSize
        fc.size() == fileSize
        fc.fileInfo.blockAmount == getBlockAmount(fileSize)
        fc.position() == fileSize
        fc.write(dataBuffer) == 0
        // 不能同时有两个写锁
        when:
        client.openReadWrite(filename)

        then:
        thrown(OverlappingFileLockException)
        // 打开一个只读的FileChannel，由于打开时上面的channel还未关闭，所以读到的依旧是旧文件
        when:
        def readonlyFileChannel = client.openReadonly(filename)

        then:
        readonlyFileChannel.size() == 0
        readonlyFileChannel.fileInfo.blockAmount == getBlockAmount(0)
        readonlyFileChannel.position() == 0
        // 即使之前channel关闭，之前打开的channel读到的依旧是旧文件
        when:
        fc.close()

        then:
        readonlyFileChannel.size() == 0
        readonlyFileChannel.fileInfo.blockAmount == getBlockAmount(0)
        readonlyFileChannel.position() == 0
        // 之前的可读写channel关闭后，新打开的channel可以读到新的内容
        when:
        fc = client.openReadonly(filename)
        buffer.position(0)

        then:
        fc.size() == fileSize
        fc.fileInfo.blockAmount == getBlockAmount(fileSize)
        fc.position() == 0
        fc.read(buffer) == fileSize
        buffer == dataBuffer
        // 关闭只读channel
        when:
        fc.close()

        then:
        noExceptionThrown()
        // 打开可读写channel，可读到之前写入的内容
        when:
        fc = client.openReadWrite(filename)
        buffer.position(0)

        then:
        fc.size() == fileSize
        fc.fileInfo.blockAmount == getBlockAmount(fileSize)
        fc.position() == 0
        fc.read(buffer) == fileSize
        buffer == dataBuffer
        fc.close()
        // 关闭只读channel
        when:
        readonlyFileChannel.close()

        then:
        noExceptionThrown()
    }

    def "Test truncate"() {
        def fileSize = FILE_SIZE
        writeData()
        // 打开可读写channel
        when:
        def fc = client.openReadWrite(filename)
        buffer.position(0)

        then:
        fc.size() == fileSize
        fc.fileInfo.blockAmount == getBlockAmount(fileSize)
        fc.position() == 0
        fc.read(buffer) == fileSize
        buffer == dataBuffer
        // 截断后的长度参数超出文件长度，不报错，但不产生任何效果
        when:
        fc.truncate(fileSize + 1)

        then:
        fc.position() == fileSize
        fc.size() == fileSize
        // 截断后的长度参数为0，文件被清空
        when:
        fc.truncate(0)
        buffer.position(0)

        then:
        fc.size() == 0
        fc.fileInfo.blockAmount == 0
        fc.position() == 0
        fc.read(buffer) == -1
        // 新开一个只读channel，由于打开时上面的channel未关闭，所以依旧读到旧的内容
        when:
        def readonlyFileChannel = client.openReadonly(filename)

        then:
        readonlyFileChannel.size() == fileSize
        readonlyFileChannel.fileInfo.blockAmount == getBlockAmount(fileSize)
        readonlyFileChannel.position() == 0
        // 及时这时候可读写channel关闭，只读channel依旧读到的是旧的内容
        when:
        fc.close()

        then:
        readonlyFileChannel.size() == fileSize
        readonlyFileChannel.fileInfo.blockAmount == getBlockAmount(fileSize)
        readonlyFileChannel.position() == 0
        // 之后新开的channel将会读到新的内容
        when:
        fc = client.openReadWrite(filename)
        dataBuffer.position(0)

        then:
        fc.size() == 0
        fc.fileInfo.blockAmount == 0
        fc.position() == 0
        fc.read(buffer) == -1
        fc.write(dataBuffer) == fileSize
        // 关闭channel
        when:
        fc.close()
        readonlyFileChannel.close()

        then:
        noExceptionThrown()
    }

    def "Test append data"() {
        def fileSize = FILE_SIZE
        def secondPosition = 3 * DataNode.BLOCK_SIZE - 1
        // secondPosition大小超过FILE_SIZE，FILE_SIZE = 2 * DataNodeServer.BLOCK_SIZE + 2
        writeData()
        // 打开读写channel
        when:
        def fc = client.openReadWrite(filename)
        buffer.position(0)

        then:
        fc.size() == fileSize
        fc.fileInfo.blockAmount == getBlockAmount(fileSize)
        fc.position() == 0
        fc.read(buffer) == fileSize
        buffer == dataBuffer
        // 移动pos到secondPosition位置，此时文件大小和block数量不会发生变化，在该处都不会获得任何数据
        when:
        fc.position(secondPosition)
        buffer.position(0)

        then:
        fc.size() == fileSize
        fc.fileInfo.blockAmount == getBlockAmount(fileSize)
        fc.position() == secondPosition
        fc.read(buffer) == -1
        fc.write(dataBuffer) == 0
        // 从secondPosition处写如dataBuffer中的数据，此时文件大小和block数发生改变
        when:
        dataBuffer.position(0)

        then:
        fc.write(dataBuffer) == fileSize
        fc.size() == secondPosition + fileSize
        fc.fileInfo.blockAmount == getBlockAmount(secondPosition + fileSize)
        fc.position() == secondPosition + fileSize
        fc.read(buffer) == -1
        // 将pos置回0，此时文件大小为secondPosition+fileSize
        when:
        fc.position(0)

        then:
        fc.read(buffer) == fileSize
        fc.position() == fileSize
        fc.size() == secondPosition + fileSize
        buffer == dataBuffer
        fc.read(buffer) == 0
        fc.size() == secondPosition + fileSize
        fc.fileInfo.blockAmount == getBlockAmount(secondPosition + fileSize)
        fc.position() == fileSize
        // 截断的参数超过文件大小，不产生任何效果
        when:
        fc.truncate(secondPosition + fileSize + 1)

        then:
        fc.size() == secondPosition + fileSize
        fc.fileInfo.blockAmount == getBlockAmount(secondPosition + fileSize)
        fc.position() == fileSize
        // 从fileSize位置开始，读取buffer大小的
        when:
        buffer.position(0)

        then:
        fc.read(buffer) == fileSize
        // FILE_SIZE，FILE_SIZE = 2 * IDataNodeProtocol.BLOCK_SIZE + 2
        // 之前写入的位置是3 * DataNodeServer.BLOCK_SIZE - 1，所以中间有DataNodeServer.BLOCK_SIZE - 3的空间是用0填充的
        when:
        buffer.position(0)

        then:
        for (int i = 0; i < DataNode.BLOCK_SIZE - 3; i++)
            buffer.get() == 0.byteValue()
        buffer.get() == 0.byteValue()
        buffer.get() == 1.byteValue()
        buffer.get() == 2.byteValue()
        buffer.get() == 3.byteValue()
        buffer.get() == 4.byteValue()
        // 将文件截断会fileSize大小（恢复原状）
        when:
        fc.truncate(fileSize)

        then:
        fc.size() == fileSize
        fc.fileInfo.blockAmount == getBlockAmount(fileSize)
        fc.position() == fileSize
        // 关闭文件，重新打开一个只读channel，此时文件大小应该变回fileSize
        when:
        fc.close()
        fc = client.openReadWrite(filename)
        buffer.position(0)

        then:
        fc.size() == fileSize
        fc.fileInfo.blockAmount == getBlockAmount(fileSize)
        fc.position() == 0
        fc.read(buffer) == fileSize
        buffer == dataBuffer
        // 关闭channel
        when:
        fc.close()

        then:
        noExceptionThrown()
    }
}