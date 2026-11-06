package org.vechain.indexer.xapp.scoring

import java.math.BigDecimal
import java.math.MathContext
import java.math.RoundingMode
import kotlin.math.exp
import org.bson.Document
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Profile
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.aggregation.Aggregation
import org.springframework.data.mongodb.core.aggregation.ConditionalOperators
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.stereotype.Service
import org.vechain.indexer.IndexerNames
import org.vechain.indexer.b3tr.round.AppRoundScoreService

/** Result of the Community Score computation for a single app. */
data class CommunityScoreResult(
    val appId: String,
    val avgRating: BigDecimal,
    val numRatings: Long,
    val helpfulVotes: Long,
    val unhelpfulVotes: Long,
    val confidenceFactor: BigDecimal,
    val helpfulRatio: BigDecimal,
    val engagementFactor: BigDecimal,
    val adjustedRating: BigDecimal,
    val communityScore: BigDecimal,
)

/**
 * Computes the Community Score for apps based on star ratings and review votes, as defined in the
 * VBD Reputation Layer Proposal (Layer 4, 30% weight). This is a pure calculation over the current
 * rating/review state — no dependency on cycle scores or rounds.
 *
 * This is the single implementation used by the production scoring pipeline
 * ([AppRoundScoreService]). App ids in the results are lower-cased to match the round-score
 * documents.
 *
 * Other services call [computeCommunityScore] to obtain the score for a single app or
 * [computeCommunityScores] for all apps.
 */
@Profile("b3tr", "b3tr-app-round-score")
@Service
open class CommunityScoreService(
    private val mongoTemplate: MongoTemplate,
    /**
     * R_min — minimum ratings for a non-zero Community Score. From
     * `indexer.community-score.min-ratings` (default 20).
     */
    @Value("\${indexer.community-score.min-ratings:20}") private val rMin: Long,
) {

    companion object {
        private val K = BigDecimal("100") // Smoothing constant for ConfidenceFactor
        private val MC = MathContext(20, RoundingMode.HALF_UP)
    }

    /** Compute the Community Score for a specific app, or null if it has no rating data. */
    open fun computeCommunityScore(appId: String): CommunityScoreResult? =
        computeCommunityScores().find { it.appId == appId.lowercase() }

    /**
     * Compute Community Scores for all apps that have rating data. Returns results sorted by
     * communityScore descending.
     */
    open fun computeCommunityScores(): List<CommunityScoreResult> {
        val ratingStats = aggregateRatingStats()
        if (ratingStats.isEmpty()) return emptyList()

        // R_min (minimum ratings for a non-zero Community Score) is configured via
        // `indexer.community-score.min-ratings` (default 20).
        val voteStats = aggregateVoteStats()

        // Compute adjusted rating for each app with rating data
        val appDataList = mutableListOf<AppData>()
        for (rs in ratingStats.values) {
            val vs = voteStats[rs.appId]
            val data =
                AppData(
                    appId = rs.appId,
                    avgRating = rs.avgRating,
                    numRatings = rs.numRatings,
                    helpfulVotes = vs?.helpfulVotes ?: 0L,
                    unhelpfulVotes = vs?.unhelpfulVotes ?: 0L,
                )
            data.adjustedRating = computeAdjustedRating(data, rMin)
            appDataList.add(data)
        }

        // Percentile rank across eligible apps only (NumRatings >= rMin), per the Community Score
        // spec: "Percentile Rank of AdjustedRating among all ELIGIBLE dApps". Ineligible apps get a
        // score of 0 and must NOT dilute the ranking pool — otherwise every ineligible app added to
        // the system inflates all eligible apps' scores. (reuses the shared mid-rank
        // implementation)
        val allAdjustedRatings =
            appDataList.filter { it.numRatings >= rMin }.map { it.adjustedRating }
        for (data in appDataList) {
            data.communityScore =
                if (data.numRatings < rMin) BigDecimal.ZERO
                else AppRoundScoreService.percentileRank(data.adjustedRating, allAdjustedRatings)
        }

        return appDataList
            .sortedByDescending { it.communityScore }
            .map { data ->
                val helpfulRatio = computeHelpfulRatio(data.helpfulVotes, data.unhelpfulVotes)
                CommunityScoreResult(
                    appId = data.appId,
                    avgRating = data.avgRating,
                    numRatings = data.numRatings,
                    helpfulVotes = data.helpfulVotes,
                    unhelpfulVotes = data.unhelpfulVotes,
                    confidenceFactor = computeConfidenceFactor(data.numRatings),
                    helpfulRatio = helpfulRatio,
                    engagementFactor = computeEngagementFactor(helpfulRatio),
                    adjustedRating = data.adjustedRating,
                    communityScore = data.communityScore,
                )
            }
    }

    // ── Data aggregation ──────────────────────────────────────────────────

    private data class AppRatingStats(
        val appId: String,
        val avgRating: BigDecimal,
        val numRatings: Long,
    )

    private data class AppVoteStats(
        val appId: String,
        val helpfulVotes: Long,
        val unhelpfulVotes: Long,
    )

    private class AppData(
        val appId: String,
        val avgRating: BigDecimal,
        val numRatings: Long,
        val helpfulVotes: Long,
        val unhelpfulVotes: Long,
        var adjustedRating: BigDecimal = BigDecimal.ZERO,
        var communityScore: BigDecimal = BigDecimal.ZERO,
    )

    private fun aggregateRatingStats(): Map<String, AppRatingStats> {
        val collection = IndexerNames.XAPP_RATING.COLLECTION
        if (!mongoTemplate.collectionExists(collection)) return emptyMap()

        val aggregation =
            Aggregation.newAggregation(
                Aggregation.match(Criteria.where("_id").ne(null)),
                Aggregation.group("appId").count().`as`("count").avg("rating").`as`("average"),
            )

        val results = mongoTemplate.aggregate(aggregation, collection, Document::class.java)
        val map = mutableMapOf<String, AppRatingStats>()
        results.forEach { doc ->
            val appId = (doc["_id"] as? String)?.lowercase() ?: return@forEach
            val count = toLong(doc.get("count"))
            val average = BigDecimal.valueOf((doc.get("average") as? Number)?.toDouble() ?: 0.0)
            map[appId] = AppRatingStats(appId = appId, avgRating = average, numRatings = count)
        }
        return map
    }

    private fun aggregateVoteStats(): Map<String, AppVoteStats> {
        val collection = IndexerNames.XAPP_REVIEW_VOTE.COLLECTION
        if (!mongoTemplate.collectionExists(collection)) return emptyMap()

        val aggregation =
            Aggregation.newAggregation(
                // Join votes with reviews to get appId and check isHidden
                Aggregation.lookup(
                    IndexerNames.XAPP_REVIEW.COLLECTION,
                    "reviewId",
                    "reviewId",
                    "review",
                ),
                Aggregation.unwind("review"),
                Aggregation.match(Criteria.where("review.isHidden").`is`(false)),
                // voteType 1 = upvote (helpful), 2 = downvote (unhelpful)
                Aggregation.group("review.appId")
                    .sum(
                        ConditionalOperators.`when`(Criteria.where("voteType").`is`(1))
                            .then(1)
                            .otherwise(0)
                    )
                    .`as`("helpfulVotes")
                    .sum(
                        ConditionalOperators.`when`(Criteria.where("voteType").`is`(2))
                            .then(1)
                            .otherwise(0)
                    )
                    .`as`("unhelpfulVotes"),
            )

        val results = mongoTemplate.aggregate(aggregation, collection, Document::class.java)
        val map = mutableMapOf<String, AppVoteStats>()
        results.forEach { doc ->
            val appId = (doc["_id"] as? String)?.lowercase() ?: return@forEach
            val helpful = toLong(doc.get("helpfulVotes"))
            val unhelpful = toLong(doc.get("unhelpfulVotes"))
            map[appId] =
                AppVoteStats(appId = appId, helpfulVotes = helpful, unhelpfulVotes = unhelpful)
        }
        return map
    }

    // ── Formula components ────────────────────────────────────────────────

    /**
     * ConfidenceFactor = 1 - e^(-NumRatings / K). The exponential is transcendental, so it is
     * evaluated via Double then wrapped in BigDecimal to preserve all remaining arithmetic.
     */
    private fun computeConfidenceFactor(numRatings: Long): BigDecimal {
        val exponent = -numRatings.toDouble() / K.toDouble()
        return BigDecimal.ONE.subtract(exp(exponent).toBigDecimal(), MC)
    }

    /** HelpfulRatio = helpfulVotes / (helpfulVotes + unhelpfulVotes), default 0.5 if no votes. */
    private fun computeHelpfulRatio(helpfulVotes: Long, unhelpfulVotes: Long): BigDecimal {
        val total = helpfulVotes + unhelpfulVotes
        return if (total == 0L) {
            BigDecimal("0.5")
        } else {
            BigDecimal(helpfulVotes).divide(BigDecimal(total), MC)
        }
    }

    /** EngagementFactor = 0.5 + 0.5 * HelpfulRatio, range [0.5, 1.0] */
    private fun computeEngagementFactor(helpfulRatio: BigDecimal): BigDecimal =
        BigDecimal("0.5").add(BigDecimal("0.5").multiply(helpfulRatio, MC), MC)

    /**
     * AdjustedRating = AvgRating * ConfidenceFactor * EngagementFactor (0 when NumRatings < rMin).
     */
    private fun computeAdjustedRating(data: AppData, rMin: Long): BigDecimal {
        if (data.numRatings < rMin) return BigDecimal.ZERO
        val confidenceFactor = computeConfidenceFactor(data.numRatings)
        val helpfulRatio = computeHelpfulRatio(data.helpfulVotes, data.unhelpfulVotes)
        val engagementFactor = computeEngagementFactor(helpfulRatio)
        return data.avgRating.multiply(confidenceFactor, MC).multiply(engagementFactor, MC)
    }
}

private fun toLong(value: Any?): Long =
    when (value) {
        is Long -> value
        is Int -> value.toLong()
        is Number -> value.toLong()
        else -> 0L
    }
