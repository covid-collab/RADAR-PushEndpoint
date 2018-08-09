package org.radarcns.gateway

import okhttp3.OkHttpClient
import org.junit.jupiter.api.Test
import org.radarcns.gateway.KafkaTopicsTest.Companion.call
import org.radarcns.producer.rest.ManagedConnectionPool
import java.net.URI
import javax.ws.rs.core.Response

class KafkaRootTest {
    @Test
    fun queryRoot() {
        val baseUri = "http://localhost:8080/radar-gateway"
        val config = Config()
        config.restProxyUrl = "http://localhost:8082"
        config.baseUri = URI.create(baseUri)

        val httpClient = OkHttpClient.Builder()
                .connectionPool(ManagedConnectionPool.GLOBAL_POOL.acquire())
                .build()

        val server = GrizzlyServer(config)
        server.start()

        try {
            call(httpClient, Response.Status.OK) {
                it.url(baseUri)
            }
            call(httpClient, Response.Status.OK) {
                it.url(baseUri).head()
            }
        } finally {
            server.shutdown()
        }
    }
}