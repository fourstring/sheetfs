# Log Structure
## Datanode Log Structure

Datanode只需要记录Write时候的修改，即在WriteChunk时记Log，需要记录的东西包括：
- `datanode id`  -- datanode组的ID（ZK中的ID，所有对应的主从datanode都需要读取这一log记录到自己的持久化磁盘中）
- `version` -- 当前文件的version号
- `chunk id`, `offset`, `size`  -- 实际需要写入的data的chunk的id、写入起始offset、数据size
- `data(zip form)`  -- 数据的zip压缩格式
- `checksum of data`  -- data的checksum

注意:

这里的datanode id会作为多个topic组来加以区分。

会在log中记录data的压缩格式数据来节省在kafka中存储的内容的大小，
同时还会记录checksum来在持久化的时候和本地磁盘中的数据进行比较，
从而快速比较得出该log是否已经做完的判断，避免解压缩