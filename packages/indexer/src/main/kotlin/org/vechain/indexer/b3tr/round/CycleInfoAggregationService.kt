package org.vechain.indexer.b3tr.round

import kotlinx.coroutines.runBlocking
import org.bson.Document
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.ApplicationEventPublisher
import org.springframework.context.annotation.Profile
import org.springframework.data.domain.Sort
import org.springframework.data.mongodb.core.BulkOperations
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.query.Update
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import org.vechain.indexer.IndexerNames
import org.vechain.indexer.b3tr.event.CycleInfoAggregationCompletedEvent
import org.vechain.indexer.b3tr.round.repository.CycleInfoRepository
import org.vechain.indexer.event.AbiLoader
import org.vechain.indexer.event.model.abi.AbiElement
import org.vechain.indexer.event.utils.FunctionReturnDecoder
import org.vechain.indexer.thor.client.ThorClient
import org.vechain.indexer.thor.model.BlockRevision
import org.vechain.indexer.thor.model.EventCriteria
import org.vechain.indexer.thor.model.EventLogsRequest
import org.vechain.indexer.thor.model.LogsOptions
import org.vechain.indexer.thor.model.LogsRange
import org.vechain.indexer.utils.ContractUtils

@Service
@Profile("b3tr", "b3tr-cycle-info")
class CycleInfoAggregationService(
    private val mongoTemplate: MongoTemplate,
    private val thorClient: ThorClient,
    private val cycleInfoRepository: CycleInfoRepository,
    private val applicationEventPublisher: ApplicationEventPublisher,
    private val b3trRoundService: B3trRoundService,
    @param:Value("\${business-event.substitutions.EMISSIONS}")
    private val emissionsContractAddress: String,
    @param:Value("\${business-event.substitutions.X_ALLOC_POOL_CONTRACT}")
    private val xAllocationPoolAddress: String,
) {
    private val logger = LoggerFactory.getLogger(this::class.java)

    companion object {
        private const val WATERMARK_ID = "cycle_info_watermark"
        private const val WATERMARK_COLLECTION = "b3tr_cycle_info_watermark"

        private const val EMISSION_DISTRIBUTED_SIG =
            "EmissionDistributed(uint256,uint256,uint256,uint256)"
        private const val EMISSION_DISTRIBUTED_V2_SIG =
            "EmissionDistributedV2(uint256,uint256,uint256,uint256,uint256)"
    }

    private val emissionDistributedTopic: String by lazy {
        "0x${ContractUtils.getEventSignature(EMISSION_DISTRIBUTED_SIG)}"
    }

    private val emissionDistributedV2Topic: String by lazy {
        "0x${ContractUtils.getEventSignature(EMISSION_DISTRIBUTED_V2_SIG)}"
    }

    private val allFundsClaimedAbi: AbiElement by lazy {
        AbiLoader.loadFunctions("abis/b3tr", listOf("allFundsClaimed")).firstOrNull {
            it.name == "allFundsClaimed"
        } ?: throw IllegalArgumentException("Function 'allFundsClaimed' not found in ABI")
    }

    @Scheduled(
        fixedDelayString = "\${indexer.cycle-info.interval-ms:60000}",
        initialDelayString = "\${indexer.cycle-info.initial-delay-ms:30000}",
    )
    fun aggregateCycleInfo() {
        var syncedRound = 0
        try {
            syncedRound = runBlocking { aggregateCycleInfoInternal() }
        } catch (e: Exception) {
            logger.error("Cycle info aggregation failed", e)
        } finally {
            if (syncedRound > 0) {
                applicationEventPublisher.publishEvent(
                    CycleInfoAggregationCompletedEvent(syncedUpToRound = syncedRound)
                )
            }
        }
    }

    private suspend fun aggregateCycleInfoInternal(): Int {
        val started = System.currentTimeMillis()
        logger.info("Starting cycle info aggregation...")

        // Guard: only proceed once the "start new round & claim allocations" operation that
        // emits the next cycle has fully settled on-chain. The pending emission is cycle M+1,
        // which also claims round M, where M is the latest round already recorded in
        // b3tr_cycle_info. Wait until a new round has started (current round > M) and all
        // allocations for round M have been claimed before recording any further cycles.
        val latestRecordedRound = getLatestRecordedRoundId()
        if (
            latestRecordedRound != null &&
                latestRecordedRound > 0 &&
                !isRoundSettled(latestRecordedRound)
        ) {
            logger.info(
                "Latest recorded round {} not settled on-chain yet " +
                    "(new round started & all allocations claimed); skipping cycle info aggregation",
                latestRecordedRound,
            )
            return 0
        }
        val collection = IndexerNames.CYCLE_INFO.COLLECTION

        // 1. Read watermark — last processed cycle
        val watermarkDoc =
            mongoTemplate.findOne(
                Query(Criteria.where("_id").`is`(WATERMARK_ID)),
                Document::class.java,
                WATERMARK_COLLECTION,
            )
        val lastProcessedRound = watermarkDoc?.getInteger("lastRoundId") ?: 0

        // 2. Get current best block number
        val bestBlock = thorClient.getBlock(BlockRevision.Keyword.BEST)
        val bestBlockNumber = bestBlock.number
        logger.info("Best block={}, last processed round={}", bestBlockNumber, lastProcessedRound)

        // 3. Query EmissionDistributed events
        val events = fetchAllEventLogs(bestBlockNumber)
        logger.info("Fetched {} emission events in total", events.size)

        if (events.isEmpty()) {
            logger.info("No emission events found, skipping")
            return 0
        }

        // 4. Filter new rounds (round > lastProcessedRound) and deduplicate by roundId
        val newRounds =
            events
                .mapNotNull { parseRoundFromEvent(it) }
                .filter { it.first > lastProcessedRound }
                .groupBy { it.first }
                .mapValues { (_, entries) -> entries.first() }

        if (newRounds.isEmpty()) {
            logger.info("No new rounds to process (lastProcessed={})", lastProcessedRound)
            return 0
        }

        // 5. Bulk upsert into MongoDB
        val now = System.currentTimeMillis()
        val bulkOps = mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED, collection)
        var count = 0

        for ((roundId, data) in newRounds.toSortedMap()) {
            val (_, blockNumber, blockTimestamp, eventType) = data
            bulkOps.upsert(
                Query(Criteria.where("_id").`is`(roundId.toString())),
                Update()
                    .set("_id", roundId.toString())
                    .set("roundId", roundId)
                    .set("blockNumber", blockNumber)
                    .set("blockTimestamp", blockTimestamp)
                    .set("eventType", eventType)
                    .set("updatedAt", now),
            )
            count++
        }

        if (count > 0) {
            bulkOps.execute()
        }

        // 6. Update watermark
        val maxRoundId = newRounds.keys.max()
        mongoTemplate.upsert(
            Query(Criteria.where("_id").`is`(WATERMARK_ID)),
            Update.update("lastRoundId", maxRoundId).set("updatedAt", now),
            WATERMARK_COLLECTION,
        )

        val elapsed = System.currentTimeMillis() - started
        logger.info(
            "Cycle info aggregation completed: {} new rounds ({}..{}), {}ms elapsed",
            count,
            newRounds.keys.min(),
            maxRoundId,
            elapsed,
        )
        // Also publish on the first sync (empty cycle_info), so rounds already completed get
        // first-seen data and scores instead of waiting for the next round to settle.
        return maxRoundId
    }

    /**
     * Returns the highest roundId currently stored in b3tr_cycle_info, or null if the collection is
     * empty.
     */
    private fun getLatestRecordedRoundId(): Int? {
        val doc =
            mongoTemplate.findOne(
                Query().with(Sort.by(Sort.Direction.DESC, "roundId")).limit(1),
                Document::class.java,
                IndexerNames.CYCLE_INFO.COLLECTION,
            )
        return doc?.getInteger("roundId")
    }

    /**
     * Checks on-chain whether the "start new round & claim allocations" operation that follows
     * [roundId] has fully completed: a new round has started (current round > [roundId]) and all
     * allocations for [roundId] have been claimed via XAllocationPool.allFundsClaimed.
     */
    private suspend fun isRoundSettled(roundId: Int): Boolean {
        val revision = BlockRevision.Keyword.BEST
        val currentRound = b3trRoundService.getCurrentRound(revision) ?: return false
        if (currentRound <= roundId) return false
        return isAllFundsClaimed(roundId, revision)
    }

    private suspend fun isAllFundsClaimed(roundId: Int, revision: BlockRevision): Boolean {
        val clause =
            ContractUtils.createClause(
                xAllocationPoolAddress,
                allFundsClaimedAbi,
                roundId.toBigInteger(),
            )
        val response = thorClient.inspectClauses(listOf(clause), revision).first()
        if (response.reverted) return false
        val decoded = FunctionReturnDecoder.decode(response.data, allFundsClaimedAbi.outputs)
        return decoded[""] as? Boolean ?: false
    }

    private suspend fun fetchAllEventLogs(
        toBlock: Long
    ): List<org.vechain.indexer.thor.model.EventLog> {
        val criteriaSet =
            listOf(
                EventCriteria(
                    address = emissionsContractAddress,
                    topic0 = emissionDistributedTopic,
                ),
                EventCriteria(
                    address = emissionsContractAddress,
                    topic0 = emissionDistributedV2Topic,
                ),
            )

        val allEvents = mutableListOf<org.vechain.indexer.thor.model.EventLog>()
        var offset = 0L
        val limit = 1000L

        while (true) {
            val request =
                EventLogsRequest(
                    range = LogsRange(unit = "block", from = 0, to = toBlock),
                    options = LogsOptions(offset = offset, limit = limit),
                    criteriaSet = criteriaSet,
                    order = "asc",
                )

            val page = thorClient.getEventLogs(request)
            allEvents.addAll(page)
            logger.info(
                "Fetched page: offset={}, count={}, total={}",
                offset,
                page.size,
                allEvents.size,
            )

            if (page.size < limit) break
            offset += page.size
        }

        return allEvents
    }

    private fun parseRoundFromEvent(
        event: org.vechain.indexer.thor.model.EventLog
    ): Tuple4<Int, Long, Long, String>? {
        val topics = event.topics
        if (topics.size < 2) return null

        val roundId = topics[1].removePrefix("0x").toBigInteger(16).toInt()
        val blockNumber = event.meta.blockNumber
        val blockTimestamp = event.meta.blockTimestamp

        val eventType =
            if (event.topics.firstOrNull() == emissionDistributedV2Topic) {
                "EmissionDistributedV2"
            } else {
                "EmissionDistributed"
            }

        return Tuple4(roundId, blockNumber, blockTimestamp, eventType)
    }
}

private data class Tuple4<T1, T2, T3, T4>(
    val first: T1,
    val second: T2,
    val third: T3,
    val fourth: T4,
)
