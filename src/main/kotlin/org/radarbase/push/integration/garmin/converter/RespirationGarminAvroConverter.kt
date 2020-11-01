package org.radarbase.push.integration.garmin.converter

import com.fasterxml.jackson.databind.JsonNode
import org.apache.avro.specific.SpecificRecord
import org.radarcns.kafka.ObservationKey
import org.radarcns.push.integration.garmin.GarminRespiration
import java.time.Instant
import javax.ws.rs.BadRequestException
import javax.ws.rs.container.ContainerRequestContext

class RespirationGarminAvroConverter(topic: String = "push_integration_garmin_respiration") :
    GarminAvroConverter(topic) {

    override fun validate(tree: JsonNode) {
        val respiration = tree[ROOT]
        if (respiration == null || !respiration.isArray) {
            throw BadRequestException("The Respiration data was invalid.")
        }
    }

    override fun convert(
        tree: JsonNode,
        request: ContainerRequestContext
    ): List<Pair<SpecificRecord, SpecificRecord>> {

        val observationKey = observationKey(request)
        return tree[ROOT]
            .map { node -> getRecord(node, observationKey) }
            .flatten()
    }

    private fun getRecord(
        node: JsonNode,
        observationKey: ObservationKey
    ): List<Pair<ObservationKey, GarminRespiration>> {
        val startTime = node["startTimeInSeconds"].asDouble()
        return node[SUB_NODE].fields().asSequence().map { (key, value) ->
            Pair(
                observationKey,
                GarminRespiration.newBuilder().apply {
                    summaryId = node["summaryId"]?.asText()
                    time = startTime + key.toDouble()
                    timeReceived = Instant.now().toEpochMilli() / 1000.0
                    startTimeOffsetInSeconds = node["startTimeOffsetInSeconds"]?.asInt()
                    respirationInBreathsPerMinute = value?.asDouble()
                    durationInSeconds = node["durationInSeconds"]?.asInt()
                }.build()
            )
        }.toList()
    }

    companion object {
        const val ROOT = "allDayRespiration"
        const val SUB_NODE = "timeOffsetEpochToBreaths"
    }
}
