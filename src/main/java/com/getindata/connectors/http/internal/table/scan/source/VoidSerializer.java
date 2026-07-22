package com.getindata.connectors.http.internal.table.scan.source;

import java.io.IOException;

import org.apache.flink.core.io.SimpleVersionedSerializer;

/**
 * EnumChkT=Void 的序列化器，http-scan 不持久化 enumerator 状态（无 checkpoint 续传）。
 */
public class VoidSerializer implements SimpleVersionedSerializer<Void> {

    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(Void checkpoint) throws IOException {
        return new byte[0];
    }

    @Override
    public Void deserialize(int version, byte[] serialized) throws IOException {
        return null;
    }
}
