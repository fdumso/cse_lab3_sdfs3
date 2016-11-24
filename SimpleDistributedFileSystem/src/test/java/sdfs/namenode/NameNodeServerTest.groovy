package sdfs.namenode

import sdfs.exception.IllegalAccessTokenException
import sdfs.protocol.IDataNodeProtocol
import spock.lang.Shared
import spock.lang.Specification

import static sdfs.Util.generateFilename

class NameNodeServerTest extends Specification {
    @Shared
    NameNode nameNode

    def setupSpec() {
        System.setProperty("sdfs.namenode.dir", File.createTempDir().absolutePath)
        nameNode = new NameNode(NameNodeServer.FLUSH_DISK_INTERNAL_SECONDS)
    }

    def "CloseReadonlyFile"() {
        def filename = generateFilename()
        def accessToken = nameNode.create(filename).token
        def readonlyAccessToken = nameNode.openReadonly(filename).token
        // 无效的token
        when:
        nameNode.closeReadonlyFile(UUID.randomUUID())

        then:
        thrown(IllegalAccessTokenException)
        // accessToken为可读写权限，方法调用错误
        when:
        nameNode.closeReadonlyFile(accessToken)

        then:
        thrown(IllegalAccessTokenException)
        // 正确关闭可读写文件
        when:
        nameNode.closeReadwriteFile(accessToken, 0)

        then:
        noExceptionThrown()
        // accessToken已被关闭
        when:
        nameNode.closeReadonlyFile(accessToken)

        then:
        thrown(IllegalAccessTokenException)
        // 正确关闭只读文件
        when:
        nameNode.closeReadonlyFile(readonlyAccessToken)

        then:
        noExceptionThrown()
        // readonlyAccessToken已被关闭
        when:
        nameNode.closeReadonlyFile(readonlyAccessToken)

        then:
        thrown(IllegalAccessTokenException)
    }

    def "CloseReadwriteFile"() {
        def filename = generateFilename()
        def accessToken = nameNode.create(filename).token
        def readonlyAccessToken = nameNode.openReadonly(filename).token
        // 无效的token
        when:
        nameNode.closeReadwriteFile(UUID.randomUUID(), 0)

        then:
        thrown(IllegalAccessTokenException)
        // 文件大小错误
        when:
        nameNode.closeReadwriteFile(nameNode.create(generateFilename()).token, 1)

        then:
        thrown(IllegalArgumentException)
        // 文件大小错误
        when:
        nameNode.closeReadwriteFile(nameNode.create(generateFilename()).token, -1)

        then:
        thrown(IllegalArgumentException)
        // 正确关闭可读写文件
        when:
        nameNode.closeReadwriteFile(accessToken, 0)

        then:
        noExceptionThrown()
        // accessToken已被关闭
        when:
        nameNode.closeReadwriteFile(accessToken, 0)

        then:
        thrown(IllegalAccessTokenException)
        // readonlyAccessToken为只读权限，方法调用错误
        when:
        nameNode.closeReadwriteFile(readonlyAccessToken, 0)

        then:
        thrown(IllegalAccessTokenException)
        // 文件大小错误
        when:
        accessToken = nameNode.openReadwrite(filename).token
        nameNode.addBlocks(accessToken, 1)
        nameNode.closeReadwriteFile(accessToken, 0)

        then:
        thrown(IllegalArgumentException)
        // accessToken已经过期，注意：上面的close操作虽然失败，但由于并不是因为token错误，所以依然使得accessToken过期
        when:
        nameNode.closeReadwriteFile(accessToken, 1)

        then:
        thrown(IllegalAccessTokenException)
        // 正确关闭读写文件
        when:
        accessToken = nameNode.openReadwrite(filename).token
        nameNode.closeReadwriteFile(accessToken, 0)

        then:
        noExceptionThrown()
        // 正确关闭读写文件，并正确添加以及删除block
        when:
        accessToken = nameNode.openReadwrite(filename).token
        nameNode.addBlocks(accessToken, 1)
        nameNode.removeLastBlocks(accessToken, 1)
        nameNode.closeReadwriteFile(accessToken, 0)
        nameNode.closeReadonlyFile(readonlyAccessToken)

        then:
        noExceptionThrown()
    }


    def "AddBlocks"() {
        def filename = generateFilename()
        def accessToken = nameNode.create(filename).token
        def readonlyAccessToken = nameNode.openReadonly(filename).token
        // 无效的token
        when:
        nameNode.addBlocks(UUID.randomUUID(), 1)

        then:
        thrown(IllegalAccessTokenException)
        // readonlyAccessToken为只读权限，无法添加blocks
        when:
        nameNode.addBlocks(readonlyAccessToken, 1)

        then:
        thrown(IllegalAccessTokenException)
        // 参数错误，数量必须大于零
        when:
        nameNode.addBlocks(accessToken, -1)

        then:
        thrown(IllegalArgumentException)
        // 正确添加block以及关闭可读写文件
        when:
        nameNode.addBlocks(accessToken, 1)
        nameNode.closeReadwriteFile(accessToken, 1)

        then:
        noExceptionThrown()
        // 文件大小错误，文件还有两个block，大小应该大于64kb小于等于128kb
        when:
        accessToken = nameNode.openReadwrite(filename).token
        nameNode.addBlocks(accessToken, 1)
        nameNode.closeReadwriteFile(accessToken, 1)

        then:
        thrown(IllegalArgumentException)
        // 正确添加以及关闭可读写文件，注意：由于上一条测试关闭失败，添加的block被取消，现在文件依然只含有两个block
        when:
        accessToken = nameNode.openReadwrite(filename).token
        nameNode.addBlocks(accessToken, 1)
        nameNode.closeReadwriteFile(accessToken, IDataNodeProtocol.BLOCK_SIZE * 2)
        nameNode.closeReadonlyFile(readonlyAccessToken)

        then:
        noExceptionThrown()
    }

    def "RemoveLastBlocks"() {
        def filename = generateFilename()
        def accessToken = nameNode.create(filename).token
        def readonlyAccessToken = nameNode.openReadonly(filename).token
        // 无效的token
        when:
        nameNode.removeLastBlocks(UUID.randomUUID(), 1)

        then:
        thrown(IllegalAccessTokenException)
        // readonlyAccessToken为只读
        when:
        nameNode.removeLastBlocks(readonlyAccessToken, 1)

        then:
        thrown(IllegalAccessTokenException)
        // accessToken对应的文件是新建的，并不包含任何block
        when:
        nameNode.removeLastBlocks(accessToken, 1)

        then:
        thrown(IndexOutOfBoundsException)
        // 参数错误，删除的block数必须大于零
        when:
        nameNode.addBlocks(accessToken, 1)
        nameNode.removeLastBlocks(accessToken, -1)

        then:
        thrown(IllegalArgumentException)
        // 正确删除block并关闭文件
        when:
        nameNode.removeLastBlocks(accessToken, 1)
        nameNode.closeReadwriteFile(accessToken, 0)
        nameNode.closeReadonlyFile(readonlyAccessToken)

        then:
        noExceptionThrown()
    }

    def "newCopyOnWriteBlock"() {
        def filename = generateFilename()
        def accessToken = nameNode.create(filename).token
        nameNode.addBlocks(accessToken, 1)
        def readonlyAccessToken = nameNode.openReadonly(filename).token
        // 无效的token
        when:
        nameNode.newCopyOnWriteBlock(UUID.randomUUID(), 0)

        then:
        thrown(IllegalAccessTokenException)
        // 权限错误，readonlyAccessToken为只读权限
        when:
        nameNode.newCopyOnWriteBlock(readonlyAccessToken, 0)

        then:
        thrown(IllegalAccessTokenException)
        // 参数错误，accessToken对应文件当前只有一块block，序号从0开始，1代表第二块，所以超出范围
        when:
        nameNode.newCopyOnWriteBlock(accessToken, 1)

        then:
        thrown(IndexOutOfBoundsException)
        // 参数错误，必须大于等于0
        when:
        nameNode.newCopyOnWriteBlock(accessToken, -1)

        then:
        thrown(IndexOutOfBoundsException)
        // 正确修改并关闭文件
        when:
        nameNode.newCopyOnWriteBlock(accessToken, 0)
        nameNode.closeReadwriteFile(accessToken, 1)
        nameNode.closeReadonlyFile(readonlyAccessToken)

        then:
        noExceptionThrown()
    }
}
