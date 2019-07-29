package org.radarcns.gateway.resource

import okhttp3.OkHttpClient
import org.junit.jupiter.api.Test
import org.radarcns.gateway.Config
import org.radarcns.gateway.GrizzlyServer
import org.radarcns.gateway.resource.KafkaTopicsTest.Companion.call
import java.net.URI
import javax.ws.rs.core.Response.Status

class KafkaRootTest {
    @Test
    fun queryRoot() {
        val baseUri = "http://localhost:8090/radar-gateway"
        val config = Config()
        config.restProxyUrl = "http://localhost:8082"
        config.schemaRegistryUrl = "http://localhost:8081"
        config.baseUri = URI.create(baseUri)

        val httpClient = OkHttpClient()

        val server = GrizzlyServer(config)
        server.start()

        try {
            httpClient.call(Status.OK) {
                url(baseUri)
            }
            httpClient.call(Status.OK) {
                url(baseUri)
                head()
            }
            httpClient.call(Status.NO_CONTENT) {
                url(baseUri)
                method("OPTIONS", null)
            }
        } finally {
            server.shutdown()
        }
    }
}