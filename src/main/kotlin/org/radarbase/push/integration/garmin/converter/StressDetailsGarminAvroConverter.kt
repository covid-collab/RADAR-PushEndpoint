package org.radarbase.push.integration.garmin.converter

import com.fasterxml.jackson.databind.JsonNode
import org.apache.avro.specific.SpecificRecord
import org.radarbase.push.integration.common.user.User
import org.radarcns.push.garmin.GarminStressDetailSummary
import java.time.Instant
import jakarta.ws.rs.BadRequestException
import jakarta.ws.rs.container.ContainerRequestContext

class StressDetailsGarminAvroConverter(topic: String = "push_integration_garmin_stress") :
    GarminAvroConverter(topic) {

    override fun validate(tree: JsonNode) {
        val stress = tree[ROOT]
        if (stress == null || !stress.isArray) {
            throw BadRequestException("The Stress data was invalid.")
        }
    }

    override fun convert(
        tree: JsonNode,
        user: User
    ): List<Pair<SpecificRecord, SpecificRecord>> {

        return tree[ROOT]
            .map { node -> Pair(user.observationKey, getRecord(node)) }
    }

    private fun getRecord(node: JsonNode): GarminStressDetailSummary {
        return GarminStressDetailSummary.newBuilder().apply {
            summaryId = node["summaryId"]?.asText()
            time = node["startTimeInSeconds"].asDouble()
            timeReceived = Instant.now().toEpochMilli() / 1000.0
            startTimeOffset = node["startTimeOffsetInSeconds"]?.asInt()
            duration = node["durationInSeconds"]?.asInt()
            date = node["calendarDate"]?.asText()
        }.build()
    }

    companion object {
        const val ROOT = "stressDetails"
    }
}
