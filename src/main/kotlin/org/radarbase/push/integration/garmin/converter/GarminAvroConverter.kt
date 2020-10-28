package org.radarbase.push.integration.garmin.converter

import com.fasterxml.jackson.databind.JsonNode
import org.apache.avro.specific.SpecificRecord
import org.radarbase.push.integration.common.converter.AvroConverter
import org.radarbase.push.integration.common.user.User
import org.radarcns.kafka.ObservationKey
import javax.ws.rs.BadRequestException
import javax.ws.rs.container.ContainerRequestContext

abstract class GarminAvroConverter(override val topic: String) : AvroConverter {

    fun observationKey(requestContext: ContainerRequestContext): ObservationKey {
        val user: User = requestContext.getProperty("user") as User
        return ObservationKey(user.projectId, user.userId, user.sourceId)
    }

    @Throws(BadRequestException::class)
    abstract fun validate(tree: JsonNode)

    fun validateAndConvert(tree: JsonNode, requestContext: ContainerRequestContext):
            List<Pair<SpecificRecord, SpecificRecord>> {
        validate(tree)
        return convert(tree, requestContext)
    }
}