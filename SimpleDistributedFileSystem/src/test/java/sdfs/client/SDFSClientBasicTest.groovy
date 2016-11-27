package sdfs.client

import sdfs.datanode.DataNodeServer
import sdfs.exception.SDFSFileAlreadyExistsException
import sdfs.namenode.NameNodeServer
import sdfs.protocol.SDFSConfiguration
import spock.lang.Shared
import spock.lang.Specification

import java.nio.ByteBuffer
import java.nio.channels.ClosedChannelException
import java.nio.channels.NonWritableChannelException

import static sdfs.Util.generateFilename
import static sdfs.Util.generatePort

class SDFSClientBasicTest extends Specification {
    @Shared
    NameNodeServer nameNodeServer
    @Shared
    DataNodeServer dataNodeServer
    @Shared
    ISDFSClient client

    def setupSpec() {
        System.setProperty("sdfs.namenode.dir", File.createTempDir().absolutePath)
        System.setProperty("sdfs.datanode.dir", File.createTempDir().absolutePath)
        SDFSConfiguration configuration = new SDFSConfiguration(InetAddress.getLocalHost(), generatePort(), InetAddress.getLocalHost(), generatePort())
        nameNodeServer = new NameNodeServer(configuration, 10)
        dataNodeServer = new DataNodeServer(configuration)
        client = new SDFSClient(configuration, 3)
        new Thread(nameNodeServer).start()
        new Thread(dataNodeServer).start()
    }

    def "Test file tree"() {
        def parentDir = generateFilename()
        client.mkdir(parentDir)
        for (int i = 0; i < 255; i++)
            client.mkdir(parentDir += "/" + generateFilename())
        def dirName = generateFilename()
        def filename = generateFilename()
        client.mkdir("$parentDir/$dirName")
        client.create("$parentDir/$filename").close()
        // 不能重复创建目录
        when:
        client.mkdir("$parentDir/$dirName")

        then:
        thrown(SDFSFileAlreadyExistsException)
        // 同目录下不能有同名的文件和文件夹
        when:
        client.mkdir("$parentDir/$filename")

        then:
        thrown(SDFSFileAlreadyExistsException)
        // 同目录下不能有同名的文件和文件夹
        when:
        client.create("$parentDir/$dirName")

        then:
        thrown(SDFSFileAlreadyExistsException)
        // 不能重复创建文件
        when:
        client.create("$parentDir/$filename")

        then:
        thrown(SDFSFileAlreadyExistsException)
        // 文件不存在
        when:
        client.openReadonly("$parentDir/${generateFilename()}")

        then:
        thrown(FileNotFoundException)
        // 文件不存在
        when:
        client.openReadWrite("$parentDir/${generateFilename()}")

        then:
        thrown(FileNotFoundException)
        // 目录不存在
        when:
        client.openReadonly("${generateFilename()}/$filename")

        then:
        thrown(FileNotFoundException)
        // 目录不存在
        when:
        client.openReadWrite("${generateFilename()}/$filename")

        then:
        thrown(FileNotFoundException)
        // 目录不存在
        when:
        client.create("${generateFilename()}/$filename")

        then:
        thrown(FileNotFoundException)
    }

    def "Test create empty"() {
        def parentDir = generateFilename()
        client.mkdir(parentDir)
        def filename = parentDir + "/" + generateFilename()
        // 创建一个文件，大小为0，创建后默认为读写模式打开
        when:
        def fc = client.create(filename)

        then:
        fc.size() == 0
        fc.fileNode.blockAmount == 0
        fc.isOpen()
        fc.position() == 0
        fc.read(ByteBuffer.allocate(1)) == -1
        // 参数错误，必须大于零
        when:
        fc.position(-1)

        then:
        thrown(IllegalArgumentException)
        // pos移动到1
        when:
        fc.position(1)

        then:
        fc.size() == 0
        fc.fileNode.blockAmount == 0
        fc.isOpen()
        fc.position() == 1
        fc.read(ByteBuffer.allocate(1)) == -1
        // 文件关闭
        when:
        fc.close()

        then:
        !fc.isOpen()
        // FileChannel已经关闭，不能再进行操作
        when:
        fc.position()

        then:
        thrown(ClosedChannelException)
        // FileChannel已经关闭，不能再进行操作
        when:
        fc.position(0)

        then:
        thrown(ClosedChannelException)
        // FileChannel已经关闭，不能再进行操作       
        when:
        fc.read(ByteBuffer.allocate(1))

        then:
        thrown(ClosedChannelException)
        // FileChannel已经关闭，不能再进行操作
        when:
        fc.write(ByteBuffer.allocate(1))

        then:
        thrown(ClosedChannelException)
        // FileChannel已经关闭，不能再进行操作
        when:
        fc.size()

        then:
        thrown(ClosedChannelException)
        // FileChannel已经关闭，不能再进行操作
        when:
        fc.flush()

        then:
        thrown(ClosedChannelException)
        // FileChannel已经关闭，不能再进行操作
        when:
        fc.truncate(0)

        then:
        thrown(ClosedChannelException)
        // 对已关闭的FileChannel进行close操作，不产生影响，直接退出
        when:
        fc.close()

        then:
        noExceptionThrown()
        // 只读模式打开文件
        when:
        fc = client.openReadonly(filename)

        then:
        fc.size() == 0
        fc.fileNode.blockAmount == 0
        fc.isOpen()
        fc.position() == 0
        // 只读模式打开的，不可写
        when:
        fc.write(ByteBuffer.allocate(1))

        then:
        thrown(NonWritableChannelException)
        // 只读模式打开的，不可截断
        when:
        fc.truncate(0)

        then:
        thrown(NonWritableChannelException)
        // 关闭channel
        when:
        fc.close()

        then:
        !fc.isOpen()
        // 对已关闭的FileChannel进行close操作，不产生影响，直接退出
        when:
        fc.close()

        then:
        noExceptionThrown()
    }
}