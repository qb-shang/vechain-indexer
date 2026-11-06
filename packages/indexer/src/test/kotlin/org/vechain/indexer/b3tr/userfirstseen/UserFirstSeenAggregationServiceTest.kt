package org.vechain.indexer.b3tr.userfirstseen

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.bson.Document
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.ObjectProvider
import org.springframework.context.ApplicationEventPublisher
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.aggregation.Aggregation
import org.springframework.data.mongodb.core.aggregation.AggregationResults
import org.springframework.data.mongodb.core.query.Query
import org.vechain.indexer.IndexerNames
import org.vechain.indexer.b3tr.event.CycleInfoAggregationCompletedEvent
import org.vechain.indexer.b3tr.event.UserFirstSeenAggregationCompletedEvent
import org.vechain.indexer.b3tr.navigator.NavigatorVoteRoundReportAggregationService
import org.vechain.indexer.b3tr.round.CycleInfo
import org.vechain.indexer.b3tr.round.repository.CycleInfoRepository

internal class UserFirstSeenAggregationServiceTest {

    private val mongoTemplate = mockk<MongoTemplate>(relaxed = true)
    private val publisher = mockk<ApplicationEventPublisher>(relaxed = true)
    private val cycleInfoRepository = mockk<CycleInfoRepository>()
    private val navigatorReports =
        mockk<ObjectProvider<NavigatorVoteRoundReportAggregationService>>(relaxed = true)

    private val service =
        UserFirstSeenAggregationService(
            mongoTemplate,
            publisher,
            cycleInfoRepository,
            navigatorReports,
        )

    private val published = mutableListOf<Int>()

    private fun setUp(latestRound: Int, watermark: Int?) {
        every { cycleInfoRepository.findTopByOrderByRoundIdDesc() } returns
            CycleInfo(latestRound, 1_000L, 1_000L, "EmissionDistributed", 0L)
        every { mongoTemplate.findOne(any<Query>(), Document::class.java, any<String>()) } returns
            watermark?.let { Document("lastProcessedRoundId", it) }
        every {
            mongoTemplate.aggregate(
                any<Aggregation>(),
                IndexerNames.APP_ROUND_ACTION_SUMMARY.COLLECTION,
                Document::class.java,
            )
        } returns AggregationResults(emptyList(), Document())
        every { publisher.publishEvent(any<UserFirstSeenAggregationCompletedEvent>()) } answers
            {
                published.add(firstArg<UserFirstSeenAggregationCompletedEvent>().processedUpToRound)
            }
    }

    @Test
    fun `first sync processes every completed round in order`() {
        setUp(latestRound = 4, watermark = null)

        service.onCycleInfoCompleted(CycleInfoAggregationCompletedEvent(syncedUpToRound = 4))

        assertEquals(listOf(1, 2, 3), published)
    }

    @Test
    fun `only rounds above the watermark are processed`() {
        setUp(latestRound = 6, watermark = 3)

        service.onCycleInfoCompleted(CycleInfoAggregationCompletedEvent(syncedUpToRound = 6))

        assertEquals(listOf(4, 5), published)
    }

    @Test
    fun `nothing is published when the latest completed round is already processed`() {
        setUp(latestRound = 4, watermark = 3)

        service.onCycleInfoCompleted(CycleInfoAggregationCompletedEvent(syncedUpToRound = 4))

        assertEquals(emptyList<Int>(), published)
        verify { navigatorReports.ifAvailable(any()) }
    }

    @Test
    fun `a failing round stops the catch-up so later rounds retry next time`() {
        setUp(latestRound = 5, watermark = null)
        var calls = 0
        every {
            mongoTemplate.aggregate(
                any<Aggregation>(),
                IndexerNames.APP_ROUND_ACTION_SUMMARY.COLLECTION,
                Document::class.java,
            )
        } answers
            {
                calls++
                if (calls == 2) error("boom") else AggregationResults(emptyList(), Document())
            }

        service.onCycleInfoCompleted(CycleInfoAggregationCompletedEvent(syncedUpToRound = 5))

        assertEquals(listOf(1), published)
    }
}
