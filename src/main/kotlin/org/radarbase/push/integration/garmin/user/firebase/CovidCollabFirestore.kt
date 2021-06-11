package org.radarbase.push.integration.garmin.user.firebase

import com.google.cloud.firestore.*
import com.google.cloud.firestore.DocumentChange.Type.*
import jakarta.ws.rs.core.Context
import org.radarbase.gateway.Config
import org.radarbase.push.integration.common.user.User
import org.radarbase.push.integration.garmin.user.firebase.FirebaseGarminAuthDetails.Companion.OAUTH_KEY
import org.radarbase.push.integration.garmin.user.firebase.FirebaseUtil.getDocument
import org.radarbase.push.integration.garmin.user.firebase.FirebaseUtil.getFirestore
import org.radarbase.push.integration.garmin.user.firebase.FirebaseUtil.initFirebase
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.IOException
import java.util.concurrent.ConcurrentHashMap

open class CovidCollabFirestore(
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
            cachedUsers[key] ?: createUser(key)
        } catch (ex: NullPointerException) {
            throw IOException("The requested key was null. Please provide a valid key.")
        }
    }

    fun getDocumentReferenceByServiceId(serviceId: String): DocumentReference? {
        val uuid = getUsers().find { it.serviceUserId == serviceId }?.id ?: return null
        return garminCollection.document(uuid)
    }

    @Throws(IOException::class)
    protected fun createUser(uuid: String): FirebaseUser? {
        logger.debug("Creating user using uuid...")
        val garminDocumentSnapshot = getDocument(uuid, garminCollection)
        val userDocumentSnapshot = getDocument(uuid, userCollection)
        return createUser(userDocumentSnapshot, garminDocumentSnapshot)
    }

    protected fun createUser(
        userSnapshot: DocumentSnapshot, garminSnapshot: DocumentSnapshot
    ): FirebaseUser? {
        if (!garminSnapshot.contains(OAUTH_KEY)) {
            logger.warn(
                "The $OAUTH_KEY key for user {} in the garmin" +
                    " document is not present. Skipping...", garminSnapshot.id
            )
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
                garminSnapshot.id
            )
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
                getDocument(garminDocumentSnapshot.id, userCollection), garminDocumentSnapshot
            )
            logger.debug("User to be updated: {}", user)
            if (checkValidUser(user)) {
                cachedUsers.put(user?.id ?: return, user)?.let { existingUser ->
                    with(logger) {
                        debug("Updated existing user: {}", existingUser)
                        debug("Updated user is: {}", user)
                    }
                } ?: logger.debug("Created new User: {}", user.id)
                hasPendingUpdates = true
            } else {
                logger.info(
                    "User {} cannot be processed due to constraints",
                    user?.id ?: garminDocumentSnapshot.id
                )
                removeUser(garminDocumentSnapshot)
            }
        } catch (e: IOException) {
            logger.error(
                "The update of the user {} was not possible.", garminDocumentSnapshot.id, e
            )
        }
    }

    private fun checkValidUser(user: FirebaseUser?): Boolean {
        return user != null
            && user.garminAuthDetails.endDate != null
            && user.garminAuthDetails.startDate != null
            && user.garminAuthDetails.userInfo?.userId != null
            && !user.garminAuthDetails.oauth2Credentials?.oauthTokens.isNullOrEmpty()
            && user.garminAuthDetails.userInfo?.errorMessage.isNullOrEmpty()
    }

    private fun removeUser(documentSnapshot: DocumentSnapshot) =
        cachedUsers.remove(documentSnapshot.id)?.let { user ->
            logger.info("Removed User: {}:", user)
            hasPendingUpdates = true
        }


    override fun onEvent(snapshots: QuerySnapshot?, e: FirestoreException?) {
        if (e != null) {
            logger.warn("Listen for updates failed: $e")
            return
        }
        logger.info(
            "OnEvent Called: {}, {}", snapshots?.documentChanges?.size, snapshots?.documents?.size
        )
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
                    exc
                )
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
                    " Try calling this method only when updates are available"
            )
        }
    }

    companion object {
        private val logger: Logger = LoggerFactory.getLogger(CovidCollabFirestore::class.java)
    }
}
