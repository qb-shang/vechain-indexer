package org.vechain.indexer.xapp.metadata

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.withContext
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Component

/**
 * Fetches an app's IPFS metadata JSON through the configured gateway and keeps only the
 * `version_history` and `tweets` portions.
 *
 * The on-chain `metadataURI` is normally a bare CID (e.g. `bafkrei…`) but may also be an
 * `ipfs://<cid>` URI or a full URL; [toGatewayUrl] normalises all of these. Fetch failures (network
 * errors, non-2xx, malformed JSON) are retried with exponential backoff and, if still failing,
 * surface as `null` so the caller can skip the event rather than overwrite good data.
 */
@Component
@Profile("b3tr", "xapp-metadata")
class IpfsMetadataClient(
    private val objectMapper: ObjectMapper,
    @Value("\${indexer.ipfs.gateway-base-url}") private val gatewayBaseUrl: String,
) {
    private val logger = LoggerFactory.getLogger(this::class.java)

    data class ResolvedAppMetadata(
        val versionHistory: List<AppMetadataVersionEntry>,
        val tweets: List<String>,
        val optionalFieldsCount: Int = 0,
        val badgeSettings: Map<String, BadgeSetting> = emptyMap(),
    )

    private val httpClient: HttpClient by lazy {
        HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(TIMEOUT_SECONDS)).build()
    }

    /** Resolves [metadataUri] to its JSON content, or null if it cannot be resolved or fetched. */
    suspend fun resolve(metadataUri: String): ResolvedAppMetadata? {
        val url = toGatewayUrl(metadataUri) ?: return null
        var attempt = 0
        while (true) {
            try {
                val body = withContext(Dispatchers.IO) { getBody(url) }
                val root = objectMapper.readTree(body) ?: return null
                return parse(root)
            } catch (e: Exception) {
                attempt++
                if (attempt > MAX_RETRIES) {
                    logger.warn("Failed to fetch app metadata from {}: {}", url, e.message)
                    return null
                }
                val waitMs = BACKOFF_BASE_MS * (1L shl (attempt - 1))
                logger.info(
                    "[retry {}/{}] metadata fetch failed: {} -- waiting {}ms",
                    attempt,
                    MAX_RETRIES,
                    e.message,
                    waitMs,
                )
                delay(waitMs)
            }
        }
    }

    /**
     * Normalises a metadataURI into a fetchable URL.
     * - bare CID or `ipfs://<cid>(/...)` → `<gateway>/<cid>`
     * - `http(s)://...` → used as-is
     * - empty / unresolvable → null
     */
    fun toGatewayUrl(metadataUri: String): String? {
        val trimmed = metadataUri.trim()
        if (trimmed.isEmpty()) return null
        if (
            trimmed.startsWith("http://", ignoreCase = true) ||
                trimmed.startsWith("https://", ignoreCase = true)
        ) {
            return trimmed
        }
        val cid = trimmed.removePrefix("ipfs://").trim().substringBefore("/").trim()
        if (cid.isEmpty()) return null
        return gatewayBaseUrl.trimEnd('/') + "/" + cid
    }

    private fun getBody(url: String): String {
        val request =
            HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Accept", "application/json")
                .timeout(Duration.ofSeconds(TIMEOUT_SECONDS))
                .GET()
                .build()
        val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
        if (response.statusCode() !in 200..299) {
            throw RuntimeException("HTTP ${response.statusCode()}: ${response.body().take(200)}")
        }
        return response.body()
    }

    private fun parseVersionHistory(root: JsonNode): List<AppMetadataVersionEntry> {
        val arr = root.path("version_history")
        if (!arr.isArray) return emptyList()
        return arr.map { node ->
            AppMetadataVersionEntry(
                version = node.path("version").asText(""),
                notes = node.path("notes").asText(""),
                timestamp = node.path("timestamp").asLong(0),
            )
        }
    }

    private fun parseTweets(root: JsonNode): List<String> {
        val arr = root.path("tweets")
        if (!arr.isArray) return emptyList()
        return arr.map { it.asText("") }.filter { v -> v.isNotBlank() }
    }

    /**
     * Parses the `badgeSettings` object from the fetched metadata JSON into a badge-type-value →
     * [BadgeSetting] map. Each entry is `{"isPrivate": bool}`; a missing or non-object
     * `badgeSettings` yields an empty map, and a missing `isPrivate` defaults to `false`.
     */
    private fun parseBadgeSettings(root: JsonNode): Map<String, BadgeSetting> {
        val node = root.path("badgeSettings")
        if (!node.isObject) return emptyMap()
        val result = LinkedHashMap<String, BadgeSetting>()
        node.fieldNames().forEach { key ->
            result[key] =
                BadgeSetting(isPrivate = node.path(key).path("isPrivate").asBoolean(false))
        }
        return result
    }

    /**
     * Parses the fetched metadata JSON into [ResolvedAppMetadata]: version_history entries, tweet
     * URLs, and a count of the optional metadata fields the app has filled in. Extracted from
     * [resolve] so the pure parsing logic is unit-testable without HTTP.
     */
    internal fun parse(root: JsonNode): ResolvedAppMetadata =
        ResolvedAppMetadata(
            versionHistory = parseVersionHistory(root),
            tweets = parseTweets(root),
            optionalFieldsCount = countOptionalFields(root),
            badgeSettings = parseBadgeSettings(root),
        )

    /**
     * Counts how many of the optional metadata fields the app filled in. The optional items are:
     * `social_urls`, `tweets`, `version_history`, `whitepaper`, `tutorial` (`tutorial_video` OR
     * `tutorial_images` count as a single item), `more_details.team_background`,
     * `more_details.app_roadmap`, `more_details.ecosystem_partners`. Maximum is 8.
     */
    private fun countOptionalFields(root: JsonNode): Int {
        var count = 0
        if (isFilled(root.path("social_urls"))) count++
        if (isFilled(root.path("tweets"))) count++
        if (isFilled(root.path("version_history"))) count++
        if (isFilled(root.path("whitepaper"))) count++
        if (isFilled(root.path("tutorial_video")) || isFilled(root.path("tutorial_images"))) count++
        val moreDetails = root.path("more_details")
        if (isFilled(moreDetails.path("team_background"))) count++
        if (isFilled(moreDetails.path("app_roadmap"))) count++
        if (isFilled(moreDetails.path("ecosystem_partners"))) count++
        return count
    }

    /** True iff [node] is present and non-empty (non-blank string / non-empty array or object). */
    private fun isFilled(node: JsonNode): Boolean =
        when {
            node.isMissingNode || node.isNull -> false
            node.isTextual -> node.asText().isNotBlank()
            node.isArray -> node.size() > 0
            node.isObject -> node.size() > 0
            else -> true
        }

    companion object {
        private const val MAX_RETRIES = 3
        private const val TIMEOUT_SECONDS = 20L
        private const val BACKOFF_BASE_MS = 1000L
    }
}
