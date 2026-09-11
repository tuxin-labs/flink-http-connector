package com.getindata.connectors.http.internal.table.scan.source;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Properties;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import com.getindata.connectors.http.internal.table.scan.HttpScanConfig;

/**
 * {@link HttpScanSplit} 的最小序列化器。
 *
 * <p>v1 不支持 checkpoint 断点续传：仅序列化 HttpScanConfig 的 properties 字段，
 * 反序列化时 url/method 等不在 properties 中的字段无法恢复（正常执行路径不依赖此恢复）。
 * 该实现仅为满足 FLIP-27 API 要求。
 */
public class HttpScanSplitSerializer implements SimpleVersionedSerializer<HttpScanSplit> {

    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(HttpScanSplit split) throws IOException {
        try (var bos = new ByteArrayOutputStream();
             var oos = new ObjectOutputStream(bos)) {
            oos.writeObject(split.getConfig().getProperties());
            oos.flush();
            return bos.toByteArray();
        }
    }

    @Override
    public HttpScanSplit deserialize(int version, byte[] serialized) throws IOException {
        try (var bis = new ByteArrayInputStream(serialized);
             var ois = new ObjectInputStream(bis)) {
            Properties props = (Properties) ois.readObject();
            return new HttpScanSplit(HttpScanConfig.from(new Configuration(), props));
        } catch (ClassNotFoundException e) {
            throw new IOException("无法反序列化 HttpScanSplit", e);
        }
    }
}
