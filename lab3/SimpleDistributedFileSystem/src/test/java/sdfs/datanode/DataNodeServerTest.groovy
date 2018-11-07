package sdfs.datanode

import sdfs.exception.IllegalAccessTokenException
import sdfs.namenode.NameNode
import sdfs.namenode.NameNodeServer
import sdfs.protocol.SDFSConfiguration
import spock.lang.Shared
import spock.lang.Specification

import static sdfs.Util.generateFilename
import static sdfs.Util.generatePort

class DataNodeServerTest extends Specification {
    public static final int POSITION = DataNode.BLOCK_SIZE >> 2
    public static final int BUFFER_SIZE = DataNode.BLOCK_SIZE >> 1
    @Shared
    NameNode nameNode
    @Shared
    DataNode dataNode
    @Shared
    def dataBuffer = new byte[BUFFER_SIZE]
    def parentDir = generateFilename()
    def filename = parentDir + "/" + generateFilename()
    int blockNumber

    def setupSpec() {
        System.setProperty("sdfs.namenode.dir", File.createTempDir().absolutePath)
        System.setProperty("sdfs.datanode.dir", File.createTempDir().absolutePath)
        SDFSConfiguration configuration = new SDFSConfiguration(InetAddress.getLocalHost(), generatePort(), InetAddress.getLocalHost(), generatePort())
        NameNodeServer nameNodeServer = new NameNodeServer(configuration, 10)
        DataNodeServer dataNodeServer = new DataNodeServer(configuration)
        nameNode = nameNodeServer.nameNode
        dataNode = dataNodeServer.dataNode
        new Thread(nameNodeServer).start()
        new Thread(dataNodeServer).start()
        for (int i = 0; i < BUFFER_SIZE; i++)
            dataBuffer[i] = i
    }

    def setup() {
        nameNode.mkdir(parentDir)
    }

    private def writeData() {
        def accessToken = nameNode.create(filename).token
        blockNumber = nameNode.addBlocks(accessToken, 1)[0].id
        dataNode.write(accessToken, blockNumber, POSITION, dataBuffer)
        nameNode.closeReadwriteFile(accessToken, POSITION + BUFFER_SIZE)
    }

    def "Read"() {
        writeData()
        def readonlyAccessToken = nameNode.openReadonly(filename).token
        def accessToken = nameNode.openReadwrite(filename).token

        // 无效的token
        when:
        dataNode.read(UUID.randomUUID(), blockNumber, 0, 1)

        then:
        thrown(IllegalAccessTokenException)
        // 无权访问该blockNumber，或者该blockNumber不存在
        when:
        dataNode.read(readonlyAccessToken, blockNumber + 1, 0, 1)

        then:
        thrown(IllegalAccessTokenException)
        // 无权访问该blockNumber，或者该blockNumber不存在
        when:
        dataNode.read(readonlyAccessToken, -1, 0, 1)

        then:
        thrown(IllegalAccessTokenException)
        // 读取范围越界
        when:
        dataNode.read(readonlyAccessToken, blockNumber, DataNode.BLOCK_SIZE, 1)

        then:
        thrown(IllegalArgumentException)
        // 读取范围越界
        when:
        dataNode.read(readonlyAccessToken, blockNumber, 0, DataNode.BLOCK_SIZE + 1)

        then:
        thrown(IllegalArgumentException)
        // 上述操作对之前写入的数据应该没有影响
        dataNode.read(readonlyAccessToken, blockNumber, POSITION, BUFFER_SIZE) == dataBuffer
        dataNode.read(accessToken, blockNumber, POSITION, BUFFER_SIZE) == dataBuffer
    }

    def "Write"() {
        def accessToken = nameNode.create(filename).token
        def blockNumber = nameNode.addBlocks(accessToken, 1)[0].id
        def readonlyAccessToken = nameNode.openReadonly(filename).token
        // 无效的token
        when:
        dataNode.write(UUID.randomUUID(), blockNumber, 0, new byte[1])

        then:
        thrown(IllegalAccessTokenException)
        // 无权访问该blockNumber，或者该blockNumber不存在
        when:
        dataNode.write(accessToken, blockNumber + 1, 0, new byte[1])

        then:
        thrown(IllegalAccessTokenException)
        // 无权访问该blockNumber，或者该blockNumber不存在
        when:
        dataNode.write(accessToken, -1, 0, new byte[1])

        then:
        thrown(IllegalAccessTokenException)
        // 写范围越界
        when:
        dataNode.write(accessToken, blockNumber, DataNode.BLOCK_SIZE, new byte[1])

        then:
        thrown(IllegalArgumentException)
        // 写范围越界
        when:
        dataNode.write(accessToken, blockNumber, 0, new byte[DataNode.BLOCK_SIZE + 1])

        then:
        thrown(IllegalArgumentException)
        // 正确写入数据
        when:
        dataNode.write(accessToken, blockNumber, POSITION, dataBuffer)

        then:
        noExceptionThrown()
        // token无写入权限
        when:
        dataNode.write(readonlyAccessToken, blockNumber, POSITION, dataBuffer)

        then:
        thrown(IllegalAccessTokenException)
    }

    // ！！open on write是在客户端实现时才需要这个测试！！
    // 在服务端实现的可以注释掉这部分测试
    def "Client level copy on write"() {
        def accessToken = nameNode.create(filename).token
        def blockNumber = nameNode.addBlocks(accessToken, 1)[0].id
        def readonlyAccessToken = nameNode.openReadonly(filename).token
        nameNode.closeReadwriteFile(accessToken, 1)
        accessToken = nameNode.openReadwrite(filename).token
        def readonlyAccessToken2 = nameNode.openReadonly(filename).token
        def copyOnWriteBlock = nameNode.newCopyOnWriteBlock(accessToken, 0).id
        // 不可直接修改已存在的block上数据
        when:
        dataNode.write(accessToken, blockNumber, POSITION, dataBuffer)

        then:
        thrown(IllegalAccessTokenException)
        // 正确地在新的block上修改数据
        when:
        dataNode.write(accessToken, copyOnWriteBlock, POSITION, dataBuffer)

        then:
        noExceptionThrown()
        // 另一个只读token在同时读取一段数据
        when:
        dataNode.read(readonlyAccessToken2, blockNumber, POSITION, BUFFER_SIZE)

        then:
        noExceptionThrown()
        // 该token无权访问copyOnWriteBlock
        when:
        dataNode.read(readonlyAccessToken2, copyOnWriteBlock, POSITION, BUFFER_SIZE)

        then:
        thrown(IllegalAccessTokenException)
        // 无权访问该blockNumber，该token获取时，accessToken对应的文件还未关闭，blockNumber还未属于该文件
        when:
        dataNode.read(readonlyAccessToken, blockNumber, POSITION, BUFFER_SIZE)

        then:
        thrown(IllegalAccessTokenException)
        // 该token无权访问copyOnWriteBlock
        when:
        dataNode.read(readonlyAccessToken, copyOnWriteBlock, POSITION, BUFFER_SIZE)

        then:
        thrown(IllegalAccessTokenException)
        // readonlyAccessToken2打开时blockNumber所属的文件已经关闭了，所以可以访问到blockNumber
        when:
        nameNode.closeReadwriteFile(accessToken, 1)
        dataNode.read(readonlyAccessToken2, blockNumber, POSITION, BUFFER_SIZE)

        then:
        noExceptionThrown()
        // 该token无权访问copyOnWriteBlock
        when:
        dataNode.read(readonlyAccessToken2, copyOnWriteBlock, POSITION, BUFFER_SIZE)

        then:
        thrown(IllegalAccessTokenException)
        // 无权访问该blockNumber，readonlyAccessToken所能访问的block在打开时就已确定，并且期间不会有变化
        when:
        dataNode.read(readonlyAccessToken, blockNumber, POSITION, BUFFER_SIZE)

        then:
        thrown(IllegalAccessTokenException)
        // 该token无权访问copyOnWriteBlock
        when:
        dataNode.read(readonlyAccessToken, copyOnWriteBlock, POSITION, BUFFER_SIZE)

        then:
        thrown(IllegalAccessTokenException)
        // blockNumber所对应的块在之前已经被copyOnWriteBlock替换，之后打开的token不能读到blockNumber所对应的块
        when:
        def readonlyAccessToken3 = nameNode.openReadonly(filename).token
        dataNode.read(readonlyAccessToken3, blockNumber, POSITION, BUFFER_SIZE)

        then:
        thrown(IllegalAccessTokenException)
        // 正确打开并读取到新的块上的数据
        when:
        dataNode.read(readonlyAccessToken3, copyOnWriteBlock, POSITION, BUFFER_SIZE)

        then:
        noExceptionThrown()
    }
}