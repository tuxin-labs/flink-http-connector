package com.getindata.connectors.http.internal.table.scan.source;

import java.util.Properties;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

import com.getindata.connectors.http.internal.table.scan.HttpScanConfig;
import com.getindata.connectors.http.internal.table.scan.HttpScanConnectorOptions;

class HttpScanSplitSerializerTest {

    @Test
    void shouldRoundtripSplitIdAndProperties() throws Exception {
        var conf = new Configuration();
        conf.set(HttpScanConnectorOptions.URL, "https://x");
        var props = new Properties();
        props.setProperty("gid.connector.http.security.cert.server", "/tmp/cert.pem");
        var split = new HttpScanSplit(HttpScanConfig.from(conf, props));

        var serializer = new HttpScanSplitSerializer();
        byte[] bytes = SimpleVersionedSerialization.writeVersionAndSerialize(serializer, split);
        HttpScanSplit restored = SimpleVersionedSerialization.readVersionAndDeSerialize(serializer, bytes);

        assertThat(restored.splitId()).isEqualTo("http-scan-split-0");
        assertThat(restored.getConfig().getProperties())
            .containsEntry("gid.connector.http.security.cert.server", "/tmp/cert.pem");
    }
}
