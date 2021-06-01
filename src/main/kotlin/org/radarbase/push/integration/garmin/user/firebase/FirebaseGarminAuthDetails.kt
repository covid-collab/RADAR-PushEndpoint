package org.radarbase.push.integration.garmin.user.firebase

import com.google.cloud.firestore.annotation.IgnoreExtraProperties
import com.google.cloud.firestore.annotation.PropertyName

@IgnoreExtraProperties
data class FirebaseGarminAuthDetails(
    @get:PropertyName("source_id") @set:PropertyName("source_id")
    var sourceId: String = DEFAULT_SOURCE_ID,
    @get:PropertyName("start_date") @set:PropertyName("start_date")
    var startDate: Long? = null,
    @get:PropertyName("end_date") @set:PropertyName("end_date")
    var endDate: Long? = null,
    @get:PropertyName("version") @set:PropertyName("version")
    var version: String? = null,
    @get:PropertyName(OAUTH_KEY) @set:PropertyName(OAUTH_KEY)
    var oauth2Credentials: GarminOauthUserCredentials? = null,
    @get:PropertyName("userId") @set:PropertyName("userId")
    var userInfo: UserInfo? = null,
) {

    companion object {
        const val OAUTH_KEY = "resource_token"
        protected const val DEFAULT_SOURCE_ID = "garmin"
    }

}

@IgnoreExtraProperties
data class GarminOauthUserCredentials(
    @get:PropertyName("datetime") @set:PropertyName("datetime")
    var datetime: Long? = null,
    @get:PropertyName("oauth_token") @set:PropertyName("oauth_token")
    var oauthTokens: List<String>? = null,
    @get:PropertyName("oauth_token_secret") @set:PropertyName("oauth_token_secret")
    var oauthTokenSecrets: List<String>? = null,
)

@IgnoreExtraProperties
data class UserInfo(
    @get:PropertyName("userId") @set:PropertyName("userId")
    var userId: String? = null,
    @get:PropertyName("errorMessage") @set:PropertyName("errorMessage")
    var errorMessage: String? = null
)
