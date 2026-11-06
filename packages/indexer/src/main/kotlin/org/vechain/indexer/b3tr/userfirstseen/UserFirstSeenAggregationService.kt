package org.vechain.indexer.b3tr.userfirstseen

import java.time.Instant
import org.bson.Document
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.ObjectProvider
import org.springframework.context.ApplicationEventPublisher
import org.springframework.context.annotation.Profile
import org.springframework.context.event.EventListener
import org.springframework.data.mongodb.core.BulkOperations
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.aggregation.Aggregation
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.query.Update
import org.springframework.stereotype.Service
import org.vechain.indexer.IndexerNames
import org.vechain.indexer.b3tr.event.CycleInfoAggregationCompletedEvent
import org.vechain.indexer.b3tr.event.UserFirstSeenAggregationCompletedEvent
import org.vechain.indexer.b3tr.navigator.NavigatorVoteRoundReportAggregationService
import org.vechain.indexer.b3tr.round.repository.CycleInfoRepository
import org.vechain.indexer.utils.IdUtils.generateId

@Service
@Profile("b3tr", "b3tr-user-first-seen")
class UserFirstSeenAggregationService(
    private val mongoTemplate: MongoTemplate,
    private val applicationEventPublisher: ApplicationEventPublisher,
    private val cycleInfoRepository: CycleInfoRepository,
    private val navigatorVoteReportAggregationService:
        ObjectProvider<NavigatorVoteRoundReportAggregationService>,
) {
    private val logger = LoggerFactory.getLogger(this::class.java)

    companion object {
        private const val WATERMARK_ID = "user_first_seen_watermark"
        private const val WATERMARK_COLLECTION = "b3tr_user_first_seen_watermark"
    }

    /**
     * Aggregates first-seen data for every completed round not processed yet, oldest first, and
     * publishes one [UserFirstSeenAggregationCompletedEvent] per round so downstream scoring runs
     * for each of them. Processing all pending rounds (not just `latest - 1`) matters after a fresh
     * sync or downtime, when several rounds complete at once; first-seen rows are insert-only, so
     * rounds must be handled in ascending order.
     */
    @EventListener
    fun onCycleInfoCompleted(event: CycleInfoAggregationCompletedEvent) {
        if (event.syncedUpToRound <= 0) {
            logger.info("No new rounds synced in CycleInfo event, skipping UserFirstSeen")
            return
        }
        val rounds =
            try {
                pendingRounds()
            } catch (e: Exception) {
                logger.error("UserFirstSeen aggregation failed", e)
                emptyList()
            }
        for (round in rounds) {
            try {
                aggregateFirstSeen(round)
            } catch (e: Exception) {
                // Later rounds depend on earlier first-seen rows; retry from here next time.
                logger.error("UserFirstSeen aggregation failed for round {}", round, e)
                break
            }
            applicationEventPublisher.publishEvent(
                UserFirstSeenAggregationCompletedEvent(processedUpToRound = round)
            )
        }
        // Run the dependent navigator vote round report aggregation now that fresh cycle info is
        // available. The bean is profile-gated and optional, so ifAvailable no-ops when absent. It
        // has its own watermark and try/catch.
        navigatorVoteReportAggregationService.ifAvailable { it.aggregateNavigatorVoteReports() }
    }

    /** Completed rounds (up to `latest cycle - 1`) above the watermark, ascending. */
    private fun pendingRounds(): List<Int> {
        val latestCycleInfo = cycleInfoRepository.findTopByOrderByRoundIdDesc()
        if (latestCycleInfo == null) {
            logger.info("No cycle info available, skipping UserFirstSeen")
            return emptyList()
        }
        val targetRound = latestCycleInfo.roundId - 1
        val watermarkDoc =
            mongoTemplate.findOne(
                Query(Criteria.where("_id").`is`(WATERMARK_ID)),
                Document::class.java,
                WATERMARK_COLLECTION,
            )
        val lastProcessedRound = watermarkDoc?.getInteger("lastProcessedRoundId") ?: 0
        if (targetRound <= lastProcessedRound) {
            logger.info(
                "Target round {} already processed (watermark={}), skipping",
                targetRound,
                lastProcessedRound,
            )
            return emptyList()
        }
        return ((lastProcessedRound + 1)..targetRound).toList()
    }

    private fun aggregateFirstSeen(targetRound: Int) {
        val started = System.currentTimeMillis()
        logger.info("Starting user first_seen aggregation for round {}...", targetRound)

        // Aggregate first-seen data for the target round only
        val sourceCollection = IndexerNames.APP_ROUND_ACTION_SUMMARY.COLLECTION
        val targetCollection = IndexerNames.USER_FIRST_SEEN.COLLECTION

        logger.info("Aggregating first_seen for round {}", targetRound)

        val aggregation =
            Aggregation.newAggregation(
                Aggregation.match(Criteria.where("roundId").`is`(targetRound)),
                Aggregation.group("appId", "user").min("roundId").`as`("firstRoundId"),
            )

        val results = mongoTemplate.aggregate(aggregation, sourceCollection, Document::class.java)

        val now = Instant.now().toEpochMilli()
        val bulkOps = mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED, targetCollection)
        var count = 0

        results.forEach { doc ->
            val appId = (doc["_id"] as? Document)?.getString("appId") ?: return@forEach
            val user = (doc["_id"] as? Document)?.getString("user") ?: return@forEach
            val firstRoundId = doc.getInteger("firstRoundId") ?: return@forEach
            val id = generateId(appId, user)

            bulkOps.upsert(
                Query(Criteria.where("_id").`is`(id)),
                Update()
                    .setOnInsert("_id", id)
                    .setOnInsert("appId", appId)
                    .setOnInsert("user", user)
                    .setOnInsert("firstRoundId", firstRoundId)
                    .setOnInsert("updatedAt", now),
            )
            count++
        }

        if (count > 0) {
            bulkOps.execute()
            logger.info("Bulk upsert completed for {} (appId, user) pairs", count)
        }

        // Update watermark
        mongoTemplate.upsert(
            Query(Criteria.where("_id").`is`(WATERMARK_ID)),
            Update.update("lastProcessedRoundId", targetRound).set("updatedAt", now),
            WATERMARK_COLLECTION,
        )

        val elapsed = System.currentTimeMillis() - started
        logger.info(
            "User first_seen aggregation completed in {}ms (watermark -> {})",
            elapsed,
            targetRound,
        )
    }
}
