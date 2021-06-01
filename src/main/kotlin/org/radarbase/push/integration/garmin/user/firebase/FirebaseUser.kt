package org.radarbase.push.integration.garmin.user.firebase

import com.google.cloud.firestore.annotation.IgnoreExtraProperties
import com.google.cloud.firestore.annotation.PropertyName
import org.radarbase.push.integration.common.user.User
import org.radarcns.kafka.ObservationKey
import java.time.Instant

data class FirebaseUser(
    val uuid: String,
    override val userId: String,
    val firebaseUserDetails: FirebaseUserDetails,
    val garminAuthDetails: FirebaseGarminAuthDetails,
) : User {
    override val id: String
        get() = uuid
    override val projectId: String
        get() = firebaseUserDetails.projectId
    override val sourceId: String
        get() = garminAuthDetails.sourceId
    override val externalId: String
        get() = garminAuthDetails.userInfo?.userId ?: ""
    override val startDate: Instant
        get() = Instant.ofEpochSecond(garminAuthDetails.startDate
            ?: throw IllegalStateException("The start date cannot be null"))
    override val endDate: Instant
        get() = Instant.ofEpochSecond(garminAuthDetails.endDate
            ?: throw IllegalStateException("The end date cannot be null"))
    override val createdAt: Instant
        get() = garminAuthDetails.oauth2Credentials?.datetime?.let { Instant.ofEpochSecond(it) }
            ?: Instant.MIN
    override val humanReadableUserId: String?
        get() = null
    override val serviceUserId: String
        get() = garminAuthDetails.userInfo?.userId ?: throw IllegalStateException("The user Id " +
            "cannot be null.")
    override val version: String?
        get() = garminAuthDetails.version
    override val isAuthorized: Boolean
        get() = !garminAuthDetails.oauth2Credentials?.oauthTokens.isNullOrEmpty()

    override val observationKey: ObservationKey = ObservationKey(projectId, userId, sourceId)
    override val versionedId: String = "$id${version?.let { "#$it" } ?: ""}"
}

@IgnoreExtraProperties
data class FirebaseUserDetails(
    @get:PropertyName("project_id") @set:PropertyName("project_id")
    var projectId: String = "radar-firebase-default-project")
