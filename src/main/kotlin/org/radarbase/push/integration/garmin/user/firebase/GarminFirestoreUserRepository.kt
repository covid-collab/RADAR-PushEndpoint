package org.radarbase.push.integration.garmin.user.firebase

import jakarta.inject.Named
import jakarta.ws.rs.core.Context
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import org.radarbase.gateway.Config
import org.radarbase.jersey.exception.HttpBadGatewayException
import org.radarbase.jersey.exception.HttpBadRequestException
import org.radarbase.jersey.exception.HttpUnauthorizedException
import org.radarbase.push.integration.common.auth.DelegatedAuthValidator.Companion.GARMIN_QUALIFIER
import org.radarbase.push.integration.common.auth.SignRequestParams
import org.radarbase.push.integration.common.user.User
import org.radarbase.push.integration.garmin.auth.OauthSignature
import org.radarbase.push.integration.garmin.user.GarminUserRepository
import org.radarbase.push.integration.garmin.user.firebase.FirebaseUtil.deleteDocument
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.lang.Math.floor
import java.time.Instant
import java.util.stream.Stream

class GarminFirestoreUserRepository(
    @Context val config: Config,
    @Context val httpClient: OkHttpClient,
    @Named(GARMIN_QUALIFIER) val covidCollabFirestore: CovidCollabFirestore,
) : GarminUserRepository(config) {

    override fun getUserAccessTokenSecret(user: User): String =
        covidCollabFirestore.getUser(user.id)
            ?.garminAuthDetails
            ?.oauth2Credentials
            ?.oauthTokenSecrets
            ?.first()
            ?: throw HttpUnauthorizedException(
                "invalid_access_token_secret", "The access token " +
                    "secret for user ${user.id} could not be found."
            )

    override fun getSignedRequest(user: User, payload: SignRequestParams): SignRequestParams =
        signRequest(getAccessToken(user), getUserAccessTokenSecret(user), payload)

    override fun deregisterUser(serviceUserId: String, userAccessToken: String) {
        val accessTokenSecret = try {
            this.getUserAccessTokenSecret(this.findByExternalId(serviceUserId))
        } catch (ex: NoSuchElementException) {
            logger.info(
                "User not found with id ${serviceUserId}, trying to deregister without " +
                    "access token secret."
            )
            ""
        } catch (ex: HttpUnauthorizedException) {
            logger.info(
                "Access token Secret not found for id ${serviceUserId}, trying to deregister " +
                    "without access token secret."
            )
            ""
        }

        // Send deregister to Garmin and Delete user from garmin collection in firebase if exists
        if (revokeToken(userAccessToken, accessTokenSecret)) {
            logger.info("Successfully deregistered user $serviceUserId.")

            // only delete the document on successful deregistration
            covidCollabFirestore.getDocumentReferenceByServiceId(serviceUserId)
                ?.let { garminDocument -> deleteDocument(garminDocument) }
        } else {
            logger.error(
                "Not able to deregister user. Please contact Garmin Support Team (connect-support@developer.garmin.com)" +
                    "to remove the user with access token: $userAccessToken and user ID: $serviceUserId"
            )
        }
    }

    override fun get(key: String): User? {
        logger.debug("Get user from Covid Collab Firebase Repo")
        return covidCollabFirestore.getUser(key)
    }

    override fun stream(): Stream<User> = covidCollabFirestore.getUsers().stream()

    override fun getAccessToken(user: User): String =
        covidCollabFirestore.getUser(user.id)
            ?.garminAuthDetails?.oauth2Credentials?.oauthTokens?.first()
            ?: throw HttpUnauthorizedException(
                "invalid_access_token",
                "The access token for user ${user.id} could not be found."
            )

    override fun hasPendingUpdates(): Boolean = covidCollabFirestore.hasPendingUpdates

    override fun applyPendingUpdates() = covidCollabFirestore.applyUpdates()


    fun revokeToken(token: String, accessTokenSecret: String?): Boolean {

        if (token.isEmpty()) throw HttpBadRequestException(
            "token-empty",
            "Token cannot be null or empty"
        )
        val req = createRequest(
            "DELETE",
            GARMIN_DEREGISTER_ENDPOINT,
            token,
            accessTokenSecret,
        )

        return httpClient.newCall(req).execute().use { response ->
            when (response.code) {
                200, 204 -> true
                400, 401, 403 -> {
                    logger.warn(
                        "Error while revoking token. Code: ${response.code}, " +
                            "Body: ${response.body?.string()}"
                    )
                    false
                }
                else -> throw HttpBadGatewayException(
                    "Cannot connect to ${GARMIN_DEREGISTER_ENDPOINT}: HTTP status ${response.code}"
                )
            }
        }
    }

    private fun signRequest(accessToken: String, tokenSecret: String, payload: SignRequestParams):
        SignRequestParams {

        val signedParams = payload.parameters.toMutableMap()
        signedParams[OAUTH_ACCESS_TOKEN] = accessToken
        signedParams[OAUTH_SIGNATURE_METHOD] = OAUTH_SIGNATURE_METHOD_VALUE
        signedParams[OAUTH_SIGNATURE] = OauthSignature(
            payload.url,
            signedParams.toSortedMap(),
            payload.method,
            config.pushIntegration.garmin.consumerSecret,
            tokenSecret,
        ).getEncodedSignature()

        return SignRequestParams(payload.url, payload.method, signedParams)
    }

    private fun getAuthParams(
        accessToken: String?,
        tokenVerifier: String?,
    ): MutableMap<String, String?> {
        return mutableMapOf(
            OAUTH_CONSUMER_KEY to config.pushIntegration.garmin.consumerKey,
            OAUTH_NONCE to floor(Math.random() * 1000000000).toInt().toString(),
            OAUTH_SIGNATURE_METHOD to OAUTH_SIGNATURE_METHOD_VALUE,
            OAUTH_TIMESTAMP to Instant.now().epochSecond.toString(),
            OAUTH_ACCESS_TOKEN to accessToken,
            OAUTH_VERIFIER to tokenVerifier,
            OAUTH_VERSION to OAUTH_VERSION_VALUE,
        )
    }

    private fun createRequest(
        method: String,
        url: String,
        accessToken: String,
        accessTokenSecret: String?,
        tokenVerifier: String? = null
    ):
        Request {
        val params = this.getAuthParams(accessToken, tokenVerifier)
        params[OAUTH_SIGNATURE] = OauthSignature(
            url, params, method,
            config.pushIntegration.garmin.consumerSecret,
            accessTokenSecret
        ).getEncodedSignature()

        val headers = params.toFormattedHeader()

        return Request.Builder()
            .url(url)
            .header("Authorization", "OAuth $headers")
            .method(method, if (method == "POST") "".toRequestBody(null) else null)
            .build()
    }

    private fun Map<String, String?>.toFormattedHeader(): String = this
        .map { (k, v) -> "$k=\"$v\"" }
        .joinToString()

    companion object {
        private val logger: Logger =
            LoggerFactory.getLogger(GarminFirestoreUserRepository::class.java)

        const val GARMIN_DEREGISTER_ENDPOINT =
            "https://healthapi.garmin.com/wellness-api/rest/user/registration"

        const val OAUTH_CONSUMER_KEY = "oauth_consumer_key"
        const val OAUTH_NONCE = "oauth_nonce"
        const val OAUTH_SIGNATURE = "oauth_signature"
        const val OAUTH_SIGNATURE_METHOD = "oauth_signature_method"
        const val OAUTH_SIGNATURE_METHOD_VALUE = "HMAC-SHA1"
        const val OAUTH_TIMESTAMP = "oauth_timestamp"
        const val OAUTH_ACCESS_TOKEN = "oauth_token"
        const val OAUTH_VERSION = "oauth_version"
        const val OAUTH_VERSION_VALUE = "1.0"
        const val OAUTH_VERIFIER = "oauth_verifier"
        const val OAUTH_ACCESS_TOKEN_SECRET = "oauth_token_secret"
        const val OAUTH_CALLBACK = "oauth_callback"
    }
}
