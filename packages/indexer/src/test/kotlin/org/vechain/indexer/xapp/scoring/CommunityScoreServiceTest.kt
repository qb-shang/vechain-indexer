package org.vechain.indexer.xapp.scoring

import io.mockk.MockKAnnotations
import io.mockk.every
import io.mockk.impl.annotations.MockK
import io.mockk.junit5.MockKExtension
import java.math.BigDecimal
import org.bson.Document
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.aggregation.Aggregation
import org.springframework.data.mongodb.core.aggregation.AggregationResults
import org.vechain.indexer.IndexerNames

@ExtendWith(MockKExtension::class)
class CommunityScoreServiceTest {

    @MockK private lateinit var mongoTemplate: MongoTemplate

    private lateinit var communityScoreService: CommunityScoreService

    @BeforeEach
    fun setUp() {
        MockKAnnotations.init(this)
        // R_min (20) is now a constructor param from config; no on-chain read.
        communityScoreService = CommunityScoreService(mongoTemplate, 20L)
    }

    /**
     * Stubs the rating/vote aggregations consumed by [CommunityScoreService]. Each [rows] entry is
     * one app's rating aggregate (appId, numRatings, avgRating); vote stats are left empty so
     * helpfulRatio defaults to 0.5 for every app.
     */
    private fun stubRatings(vararg rows: Triple<String, Long, Double>) {
        every { mongoTemplate.collectionExists(IndexerNames.XAPP_RATING.COLLECTION) } returns true
        every { mongoTemplate.collectionExists(IndexerNames.XAPP_REVIEW_VOTE.COLLECTION) } returns
            true
        every {
            mongoTemplate.aggregate(
                any<Aggregation>(),
                IndexerNames.XAPP_RATING.COLLECTION,
                Document::class.java,
            )
        } returns
            AggregationResults(
                rows.map { (appId, count, avg) ->
                    Document("_id", appId).append("count", count).append("average", avg)
                },
                Document(),
            )
        every {
            mongoTemplate.aggregate(
                any<Aggregation>(),
                IndexerNames.XAPP_REVIEW_VOTE.COLLECTION,
                Document::class.java,
            )
        } returns AggregationResults(emptyList(), Document())
    }

    @Test
    fun `ineligible apps are excluded from the percentile ranking pool`() {
        // Three eligible apps (>= R_MIN=20) + one ineligible (n=1). The ineligible app's
        // adjustedRating=0 must NOT dilute the eligible apps' percentile ranks; without the fix the
        // lowest eligible app would score ~25 instead of 0.
        stubRatings(
            Triple("appa", 23L, 4.4),
            Triple("appb", 22L, 4.4),
            Triple("appc", 21L, 4.4),
            Triple("appjunk", 1L, 3.0),
        )

        val scores = communityScoreService.computeCommunityScores().associateBy { it.appId }

        // Ineligible app: score is exactly 0
        assertEquals(BigDecimal.ZERO, scores["appjunk"]!!.communityScore)

        // With junk excluded from the pool, the 3 eligible apps rank among themselves:
        // appA (highest adjusted) > appB > appC (lowest → 0).
        assertEquals(
            0,
            scores["appc"]!!.communityScore.compareTo(BigDecimal.ZERO),
            "appC should be last among eligible apps → score 0 (not diluted by junk)",
        )
        assertTrue(
            scores["appa"]!!.communityScore > scores["appb"]!!.communityScore,
            "appA (more ratings) should outrank appB",
        )
        assertTrue(
            scores["appb"]!!.communityScore > scores["appc"]!!.communityScore,
            "appB should outrank appC",
        )
    }

    @Test
    fun `apps below R_MIN always score zero even when all apps are ineligible`() {
        stubRatings(Triple("appa", 5L, 4.0), Triple("appb", 3L, 5.0))

        val scores = communityScoreService.computeCommunityScores()
        scores.forEach {
            assertEquals(
                0,
                it.communityScore.compareTo(BigDecimal.ZERO),
                "app ${it.appId} should be 0",
            )
        }
    }

    @Test
    fun `app ids are lower-cased to match round-score documents`() {
        stubRatings(Triple("0xABCdef", 25L, 4.0))

        val scores = communityScoreService.computeCommunityScores()

        assertEquals(listOf("0xabcdef"), scores.map { it.appId })
        assertEquals(25L, communityScoreService.computeCommunityScore("0xABCDEF")!!.numRatings)
    }
}
