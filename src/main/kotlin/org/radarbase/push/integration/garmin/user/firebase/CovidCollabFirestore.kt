package org.radarbase.push.integration.garmin.user.firebase

import com.google.cloud.firestore.*
import com.google.cloud.firestore.DocumentChange.Type.*
import jakarta.ws.rs.core.Context
import org.radarbase.gateway.Config
import org.radarbase.push.integration.common.user.User
import org.radarbase.push.integration.garmin.user.firebase.FirebaseGarminAuthDetails.Companion.OAUTH_KEY
import org.radarbase.push.integration.garmin.user.firebase.FirebaseUserRepository.Companion.getDocument
import org.radarbase.push.integration.garmin.user.firebase.FirebaseUserRepository.Companion.getFirestore
import org.radarbase.push.integration.garmin.user.firebase.FirebaseUserRepository.Companion.initFirebase
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.IOException
import java.util.concurrent.ConcurrentHashMap

class CovidCollabFirestore(
    @Context private val config: Config,
) : EventListener<QuerySnapshot> {
    var hasPendingUpdates = true
        private set
    private val cachedUsers = ConcurrentHashMap<String, FirebaseUser>()
    private val userCollection: CollectionReference
    private val garminCollection: CollectionReference
    private val garminCollectionListenerRegistration: ListenerRegistration

    init {
        initFirebase()
        userCollection = getFirestore()
            .collection(config.pushIntegration.garmin.userRepositoryFirestoreUserCollection)
        garminCollection = getFirestore()
            .collection(config.pushIntegration.garmin.userRepositoryFirestoreGarminCollection)
        garminCollectionListenerRegistration =
            garminCollection.addSnapshotListener(this)
    }

    fun getUsers(): MutableCollection<User> = ConcurrentHashMap<String, User>(cachedUsers).values

    @Throws(IOException::class)
    fun getUser(key: String): FirebaseUser? {
        return try {
            cachedUsers[key]
        } catch (ex: NullPointerException) {
            logger.warn("The requested user was not found in cache. Creating a new one.")
            createUser(key)
        }
    }

    fun getDocumentByExternalId(externalId: String): DocumentSnapshot? {
        val uuid = getUsers().find { it.serviceUserId == externalId }?.id ?: return null
        return getDocument(uuid, garminCollection)
    }

    @Throws(IOException::class)
    protected fun createUser(uuid: String): FirebaseUser? {
        logger.debug("Creating user using uuid...")
        val garminDocumentSnapshot = getDocument(uuid, garminCollection)
        val userDocumentSnapshot = getDocument(uuid, userCollection)
        return createUser(userDocumentSnapshot, garminDocumentSnapshot)
    }

    protected fun createUser(
        userSnapshot: DocumentSnapshot, garminSnapshot: DocumentSnapshot): FirebaseUser? {
        if (!garminSnapshot.contains(OAUTH_KEY)) {
            logger.warn(
                "The ${OAUTH_KEY} key for user {} in the fitbit" +
                    " document is not present. Skipping...", garminSnapshot.id)
            return null
        }

        // Get the fitbit document for the user which contains Auth Info
        val authDetails: FirebaseGarminAuthDetails? = garminSnapshot
            .toObject(FirebaseGarminAuthDetails::class.java)
        // Get the user document for the user which contains User Details
        val userDetails: FirebaseUserDetails? = userSnapshot
            .toObject(FirebaseUserDetails::class.java)

        logger.debug("Auth details: {}", authDetails)
        logger.debug("User Details: {}", userDetails)

        // if auth details are not available, skip this user.
        if (authDetails?.oauth2Credentials == null) {
            logger.warn(
                "The auth details for user {} in the database are not valid. Skipping...",
                garminSnapshot.id)
            return null
        }

        return FirebaseUser(
            uuid = garminSnapshot.id,
            userId = garminSnapshot.id,
            firebaseUserDetails = userDetails ?: FirebaseUserDetails(),
            garminAuthDetails = authDetails
        )
    }

    @Synchronized
    private fun updateUser(garminDocumentSnapshot: DocumentSnapshot) {
        try {
            val user: FirebaseUser? = createUser(
                getDocument(garminDocumentSnapshot.id, userCollection), garminDocumentSnapshot)
            logger.debug("User to be updated: {}", user)
            if (checkValidUser(user)) {
                val user1: FirebaseUser? = cachedUsers.put(user?.id ?: return, user)
                if (user1 == null) {
                    logger.debug("Created new User: {}", user.id)
                } else {
                    with(logger) {
                        debug("Updated existing user: {}", user1)
                        debug("Updated user is: {}", user)
                    }
                }
                hasPendingUpdates = true
            } else {
                logger.info("User {} cannot be processed due to constraints",
                    user?.id ?: garminDocumentSnapshot.id)
                removeUser(garminDocumentSnapshot)
            }
        } catch (e: IOException) {
            logger.error(
                "The update of the user {} was not possible.", garminDocumentSnapshot.id, e)
        }
    }

    /**
     * We add the user based on the following conditions -
     * 1) The user is not null
     * 2) The user has not auth errors signified by the auth_result key in firebase. We accept 409
     * code too since it is just a concurrent request conflict and does not effect the validity of
     * the tokens.
     *
     * @param user The user to check for validity
     * @return true if user is valid, false otherwise.
     */
    private fun checkValidUser(user: FirebaseUser?): Boolean {
        return (user != null
            && user.garminAuthDetails.endDate != null
            && user.garminAuthDetails.startDate != null
            && user.garminAuthDetails.userInfo?.userId != null
            && !user.garminAuthDetails.oauth2Credentials?.oauthTokens.isNullOrEmpty()
            && user.garminAuthDetails.userInfo?.errorMessage.isNullOrEmpty())
    }

    private fun removeUser(documentSnapshot: DocumentSnapshot) {
        val user: FirebaseUser? = cachedUsers.remove(documentSnapshot.id)
        if (user != null) {
            logger.info("Removed User: {}:", user)
            hasPendingUpdates = true
        }
    }


    override fun onEvent(snapshots: QuerySnapshot?, e: FirestoreException?) {
        if (e != null) {
            logger.warn("Listen for updates failed: $e")
            return
        }
        logger.info(
            "OnEvent Called: {}, {}", snapshots?.documentChanges?.size, snapshots?.documents?.size)
        var countAdded = 0
        for (dc in snapshots?.documentChanges ?: return) {
            try {
                logger.debug("Type: {}", dc.type)
                when (dc.type) {
                    ADDED, MODIFIED -> {
                        this.updateUser(dc.document)
                        countAdded++
                    }
                    REMOVED -> this.removeUser(dc.document)
                    else -> {
                    }
                }
            } catch (exc: Exception) {
                logger.warn(
                    "Could not process document change event for document: {}",
                    dc.document.id,
                    exc)
            }
        }
        logger.info("Added/Updated {} Users", countAdded)
    }

    fun applyUpdates() {
        if (hasPendingUpdates) {
            hasPendingUpdates = false
        } else {
            throw IOException(
                "No pending updates available." +
                    " Try calling this method only when updates are available")
        }
    }

    companion object {

        private val logger: Logger = LoggerFactory.getLogger(CovidCollabFirestore::class.java)
    }
}
