package org.radarbase.push.integration.garmin.user.firebase

import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.firestore.*
import com.google.firebase.FirebaseApp
import com.google.firebase.FirebaseOptions
import com.google.firebase.cloud.FirestoreClient
import org.radarbase.gateway.Config
import org.radarbase.push.integration.garmin.user.GarminUserRepository
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.IOException
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

abstract class FirebaseUserRepository(val config: Config) : GarminUserRepository(config) {

    companion object {

        private val logger: Logger = LoggerFactory.getLogger(FirebaseUserRepository::class.java)

        fun initFirebase() {
            // The path to the credentials file should be provided by
            // the GOOGLE_APPLICATION_CREDENTIALS env var.
            // See https://firebase.google.com/docs/admin/setup#initialize-sdk for more details.
            val options = try {
                FirebaseOptions.Builder()
                    .setCredentials(GoogleCredentials.getApplicationDefault())
                    .build()
            } catch (exc: IOException) {
                logger.error("Failed to get credentials for Firebase app.", exc)
                throw IllegalStateException(exc)
            }

            try {
                FirebaseApp.initializeApp(options)
            } catch (exc: IllegalStateException) {
                logger.warn("Firebase app was already initialised. {}", exc.message)
            }
        }


        fun getFirestore(): Firestore {
            return FirestoreClient.getFirestore()
        }

        /**
         * Get a document from a Collection in Firestore.
         *
         * @param key The document ID to pull from the collection
         * @param collection The collection reference to query
         * @return the document
         * @throws IOException If there was a problem getting the document
         */
        @Throws(IOException::class)
        fun getDocument(key: String, collection: CollectionReference): DocumentSnapshot = try {
            collection.document(key).get()[20, TimeUnit.SECONDS]
        } catch (e: InterruptedException) {
            throw IOException(e)
        } catch (e: ExecutionException) {
            throw IOException(e)
        } catch (e: TimeoutException) {
            throw IOException(e)
        }

        /**
         * Writes the specified object to a Firestore document.
         *
         * @param documentReference document reference for the document to be updated
         * @param obj The POJO to write to the document
         * @throws IOException If there was a problem updating the document
         */
        @Throws(IOException::class)
        fun updateDocument(documentReference: DocumentReference, obj: Any) = try {
            documentReference.set(obj, SetOptions.merge())[20, TimeUnit.SECONDS]
        } catch (e: InterruptedException) {
            throw IOException(e)
        } catch (e: TimeoutException) {
            throw IOException(e)
        } catch (e: ExecutionException) {
            throw IOException(e)
        }

        fun deleteDocument(documentReference: DocumentReference) = try {
            documentReference.delete()
        } catch (e: InterruptedException) {
            throw IOException(e)
        } catch (e: TimeoutException) {
            throw IOException(e)
        } catch (e: ExecutionException) {
            throw IOException(e)
        }
    }
}
