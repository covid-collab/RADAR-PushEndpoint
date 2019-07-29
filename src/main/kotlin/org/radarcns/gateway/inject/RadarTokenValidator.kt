package org.radarcns.gateway.inject

import org.radarcns.auth.authentication.TokenValidator
import org.radarcns.auth.config.TokenVerifierPublicKeyConfig
import org.radarcns.gateway.Config
import org.radarcns.gateway.auth.Auth
import org.radarcns.gateway.auth.AuthValidator
import org.radarcns.gateway.auth.ManagementPortalAuth
import java.net.URI
import javax.ws.rs.container.ContainerRequestContext
import javax.ws.rs.core.Context

/** Creates a TokenValidator based on the current management portal configuration. */
class RadarTokenValidator constructor(@Context config: Config) : AuthValidator {
    private val tokenValidator: TokenValidator = try {
        TokenValidator()
    } catch (e: RuntimeException) {
        val cfg = TokenVerifierPublicKeyConfig()
        cfg.publicKeyEndpoints = listOf(URI("${config.managementPortalUrl}/oauth/token_key"))
        cfg.resourceName = config.jwtResourceName
        TokenValidator(cfg)
    }

    override fun verify(request: ContainerRequestContext): Auth? {
        val token = getToken(request) ?: return null
        return ManagementPortalAuth(tokenValidator.validateAccessToken(token))
    }
}