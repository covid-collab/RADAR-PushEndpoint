package org.radarbase.push.integration.garmin.user

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonProperty
import org.radarbase.push.integration.common.user.User
import org.radarcns.kafka.ObservationKey
import java.time.Instant

@JsonIgnoreProperties(ignoreUnknown = true)
data class GarminUser(
        @JsonProperty("id") override val id: String,
        @JsonProperty("createdAt") override val createdAt: Instant,
        @JsonProperty("projectId") override val projectId: String,
        @JsonProperty("userId") override val userId: String,
        @JsonProperty("humanReadableUserId") override val humanReadableUserId: String?,
        @JsonProperty("sourceId") override val sourceId: String,
        @JsonProperty("externalId") override val externalId: String?,
        @JsonProperty("isAuthorized") override val isAuthorized: Boolean,
        @JsonProperty("startDate") override val startDate: Instant,
        @JsonProperty("endDate") override val endDate: Instant,
        @JsonProperty("version") override val version: String? = null,
        @JsonProperty("serviceUserId") override val serviceUserId: String
) : User {

    override val observationKey: ObservationKey = ObservationKey(projectId, userId, sourceId)
    override val versionedId: String = "$id${version?.let { "#$it" } ?: ""}"

}
