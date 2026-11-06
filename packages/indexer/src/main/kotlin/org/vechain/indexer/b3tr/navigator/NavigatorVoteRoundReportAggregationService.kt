package org.vechain.indexer.b3tr.navigator

import java.math.BigDecimal
import java.math.BigInteger
import org.bson.Document
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Profile
import org.springframework.data.domain.Sort
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.query.Update
import org.springframework.stereotype.Service
import org.vechain.indexer.IndexerNames
import org.vechain.indexer.b3tr.round.repository.CycleInfoRepository

@Service
@Profile("b3tr", "b3tr-navigator", "b3tr-navigator-vote-report")
class NavigatorVoteRoundReportAggregationService(
    private val mongoTemplate: MongoTemplate,
    private val cycleInfoRepository: CycleInfoRepository,
) {
    private val logger = LoggerFactory.getLogger(this::class.java)

    companion object {
        private const val WATERMARK_ID = "navigator_vote_round_report_watermark"
        private const val WATERMARK_COLLECTION = "b3tr_navigator_vote_round_report_watermark"
        private const val HISTORY_EVENT_NAME = "B3TR_XALLOCATION_VOTE"
    }

    private data class RoundAggregationResult(
        val report: NavigatorVoteRoundReport,
        val dappRanking: NavigatorDappVoteRoundRanking,
    )

    /**
     * Aggregates navigator vote round reports. Now triggered by
     * [org.vechain.indexer.b3tr.userfirstseen.UserFirstSeenAggregationService.onCycleInfoCompleted]
     * whenever new cycle info is synced, instead of a fixed-delay scheduler. The internal watermark
     * makes this idempotent, so it is safe to invoke repeatedly.
     */
    fun aggregateNavigatorVoteReports() {
        try {
            aggregateInternal()
        } catch (e: Exception) {
            logger.error("Navigator vote round report aggregation failed", e)
        }
    }

    private fun aggregateInternal() {
        val latestCycle = cycleInfoRepository.findTopByOrderByRoundIdDesc()
        if (latestCycle == null) {
            logger.info("No cycle info found, skip navigator vote report aggregation")
            return
        }

        val watermarkDoc =
            mongoTemplate.findOne(
                Query(Criteria.where("_id").`is`(WATERMARK_ID)),
                Document::class.java,
                WATERMARK_COLLECTION,
            )
        val lastProcessedRound = watermarkDoc?.getInteger("lastRoundId") ?: 0

        // The latest cycle_info round is still in progress (votes not final), so only
        // aggregate up to the previous (last completed) round. Mirrors
        // UserFirstSeenAggregationService, which also targets latest - 1.
        val latestCompletedRound = latestCycle.roundId - 1
        if (latestCompletedRound <= lastProcessedRound) {
            logger.info(
                "No new completed rounds (latest={}, completed={}, processed={})",
                latestCycle.roundId,
                latestCompletedRound,
                lastProcessedRound,
            )
            return
        }

        val allCycles =
            cycleInfoRepository
                .findByRoundIdIn((lastProcessedRound + 1..latestCompletedRound).toList())
                .sortedBy { it.roundId }
        if (allCycles.isEmpty()) return

        allCycles.forEach { cycle ->
            val roundId = cycle.roundId
            val result = buildRoundAggregation(roundId, cycle.blockNumber, cycle.blockTimestamp)
            mongoTemplate.save(result.report, IndexerNames.NAVIGATOR_VOTE_ROUND_REPORT.COLLECTION)
            mongoTemplate.save(
                result.dappRanking,
                IndexerNames.NAVIGATOR_DAPP_VOTE_ROUND_RANKING.COLLECTION,
            )
            logger.info(
                "Navigator vote report aggregated for round {}: navigators={}, delegatedVotes={}, totalVotes={}, dapps={}",
                roundId,
                result.report.navigatorCount,
                result.report.delegatedVotes,
                result.report.totalVotes,
                result.dappRanking.dappCount,
            )
        }

        val now = System.currentTimeMillis()
        mongoTemplate.upsert(
            Query(Criteria.where("_id").`is`(WATERMARK_ID)),
            Update.update("lastRoundId", latestCompletedRound).set("updatedAt", now),
            WATERMARK_COLLECTION,
        )
    }

    private fun buildRoundAggregation(
        roundId: Int,
        roundBlockNumber: Long,
        roundBlockTimestamp: Long,
    ): RoundAggregationResult {
        val reportId = NavigatorVoteRoundReport.idForRound(roundId)
        // Attribute each vote to the navigator the citizen had at the vote's block, so past rounds
        // are not rewritten by later delegation changes. Navigator status (for votes cast by a
        // navigator directly) is still read from current state.
        val delegations =
            NavigatorDelegationTimeline(
                mongoTemplate.findAll(
                    NavigatorDelegationEvent::class.java,
                    IndexerNames.NAVIGATOR_DELEGATION_EVENT.COLLECTION,
                )
            )
        val activeNavigatorAddresses =
            mongoTemplate
                .find(
                    Query(
                        Criteria.where("status")
                            .`in`(NavigatorStatus.ACTIVE.name, NavigatorStatus.EXITING.name)
                    ),
                    Document::class.java,
                    IndexerNames.NAVIGATOR.COLLECTION,
                )
                .mapNotNull { doc ->
                    (doc.getString("address") ?: doc.getString("_id") ?: doc.get("_id")?.toString())
                        ?.lowercase()
                }
                .toSet()

        val voteQuery =
            Query(
                    Criteria.where("eventName")
                        .`is`(HISTORY_EVENT_NAME)
                        .and("roundId")
                        .`is`(roundId.toString())
                )
                .with(Sort.by(Sort.Direction.ASC, "blockNumber"))

        val votes =
            mongoTemplate.find(voteQuery, Document::class.java, IndexerNames.HISTORY.COLLECTION)

        val voteEventsByNavigator = mutableMapOf<String, Long>()
        val citizenSetByNavigator = mutableMapOf<String, MutableSet<String>>()
        val weightByNavigator = mutableMapOf<String, BigInteger>()
        val appWeightByNavigator = mutableMapOf<String, MutableMap<String, BigInteger>>()

        var delegatedVotes = 0L
        var manualVotes = 0L
        var totalWeight = BigInteger.ZERO

        votes.forEach { ev ->
            val voter = ((ev["from"] as? String) ?: (ev["origin"] as? String) ?: "").lowercase()
            val voteBlock = (ev["blockNumber"] as? Number)?.toLong() ?: Long.MAX_VALUE
            val isActiveNavigatorVoter =
                activeNavigatorAddresses.contains(voter) ||
                    mongoTemplate.exists(
                        Query(
                            Criteria.where("_id")
                                .`is`(voter)
                                .and("status")
                                .`in`(NavigatorStatus.ACTIVE.name, NavigatorStatus.EXITING.name)
                        ),
                        IndexerNames.NAVIGATOR.COLLECTION,
                    )
            val navigator =
                delegations.navigatorAt(voter, voteBlock)
                    ?: if (isActiveNavigatorVoter) voter else null
            if (navigator.isNullOrBlank()) {
                manualVotes++
                return@forEach
            }
            delegatedVotes++
            voteEventsByNavigator[navigator] = (voteEventsByNavigator[navigator] ?: 0L) + 1
            citizenSetByNavigator.computeIfAbsent(navigator) { mutableSetOf() }.add(voter)

            @Suppress("UNCHECKED_CAST")
            val appVotes = ev["appVotes"] as? List<Document> ?: emptyList()
            appVotes.forEach { appVote ->
                val appId = (appVote["appId"] as? String)?.lowercase() ?: return@forEach
                val weight =
                    (appVote["voteWeight"] as? String)?.toBigIntegerOrNull() ?: BigInteger.ZERO
                weightByNavigator[navigator] =
                    (weightByNavigator[navigator] ?: BigInteger.ZERO) + weight
                appWeightByNavigator.computeIfAbsent(navigator) { mutableMapOf() }
                appWeightByNavigator[navigator]!![appId] =
                    (appWeightByNavigator[navigator]!![appId] ?: BigInteger.ZERO) + weight
                totalWeight += weight
            }
        }

        val navigatorSummaries =
            voteEventsByNavigator.keys.sorted().map { navigator ->
                val topApps =
                    (appWeightByNavigator[navigator] ?: emptyMap())
                        .entries
                        .sortedByDescending { it.value }
                        .take(10)
                        .map { NavigatorAppWeight(it.key, it.value) }

                NavigatorVoteSummary(
                    navigator = navigator,
                    voteEvents = voteEventsByNavigator[navigator] ?: 0L,
                    citizenCount = citizenSetByNavigator[navigator]?.size?.toLong() ?: 0L,
                    weightTotal = BigDecimal(weightByNavigator[navigator] ?: BigInteger.ZERO),
                    topApps = topApps,
                )
            }

        val navigatorCount =
            mongoTemplate.count(
                Query(
                    Criteria.where("status")
                        .`in`(NavigatorStatus.ACTIVE.name, NavigatorStatus.EXITING.name)
                ),
                IndexerNames.NAVIGATOR.COLLECTION,
            )

        val report =
            NavigatorVoteRoundReport(
                id = reportId,
                blockId = "",
                blockNumber = roundBlockNumber,
                blockTimestamp = roundBlockTimestamp,
                roundId = roundId,
                reportStatus = "COMPLETED",
                reportGeneratedAt = System.currentTimeMillis(),
                totalVotes = votes.size.toLong(),
                delegatedVotes = delegatedVotes,
                manualVotes = manualVotes,
                autoVotes = 0L,
                navigatorCount = navigatorCount,
                roundDeadline = null,
                roundSnapshot = null,
                totalDelegatedWeight = BigDecimal(totalWeight),
                navigatorSummaries = navigatorSummaries,
            )

        val dappRanking =
            buildDappRanking(roundId, roundBlockNumber, roundBlockTimestamp, appWeightByNavigator)
        return RoundAggregationResult(report = report, dappRanking = dappRanking)
    }

    private fun buildDappRanking(
        roundId: Int,
        roundBlockNumber: Long,
        roundBlockTimestamp: Long,
        appWeightByNavigator: Map<String, Map<String, BigInteger>>,
    ): NavigatorDappVoteRoundRanking {
        val totalWeightByApp = mutableMapOf<String, BigInteger>()
        val navigatorSetByApp = mutableMapOf<String, MutableSet<String>>()

        appWeightByNavigator.forEach { (navigator, appWeights) ->
            appWeights.forEach { (appId, weight) ->
                totalWeightByApp[appId] = (totalWeightByApp[appId] ?: BigInteger.ZERO) + weight
                navigatorSetByApp.computeIfAbsent(appId) { mutableSetOf() }.add(navigator)
            }
        }

        val sortedApps =
            totalWeightByApp.entries.sortedWith(
                compareByDescending<Map.Entry<String, BigInteger>> { it.value }.thenBy { it.key }
            )

        // Standard competition ranking: apps with equal weight share the same rank,
        // and the next rank skips ahead (e.g. weights 100,100,50,20,20 -> 1,1,3,4,4).
        var rank = 0
        var previousWeight: BigInteger? = null
        val rankedApps =
            sortedApps.mapIndexed { idx, entry ->
                if (previousWeight == null || entry.value.compareTo(previousWeight) != 0) {
                    rank = idx + 1
                    previousWeight = entry.value
                }
                NavigatorDappVoteRankingEntry(
                    rank = rank,
                    appId = entry.key,
                    weight = entry.value,
                    contributingNavigators = navigatorSetByApp[entry.key]?.size?.toLong() ?: 0L,
                )
            }

        val totalWeight = totalWeightByApp.values.fold(BigInteger.ZERO, BigInteger::add)
        return NavigatorDappVoteRoundRanking(
            id = NavigatorDappVoteRoundRanking.idForRound(roundId),
            blockId = "",
            blockNumber = roundBlockNumber,
            blockTimestamp = roundBlockTimestamp,
            roundId = roundId,
            reportGeneratedAt = System.currentTimeMillis(),
            dappCount = rankedApps.size.toLong(),
            totalWeight = BigDecimal(totalWeight),
            dappRankings = rankedApps,
        )
    }
}
