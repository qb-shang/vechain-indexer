package org.vechain.indexer.b3tr.round

import jakarta.annotation.PostConstruct
import java.math.BigDecimal
import java.math.BigInteger
import java.math.MathContext
import java.math.RoundingMode
import kotlinx.coroutines.runBlocking
import org.bson.Document
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Profile
import org.springframework.context.event.EventListener
import org.springframework.data.domain.Sort
import org.springframework.data.mongodb.core.BulkOperations
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.aggregation.Aggregation
import org.springframework.data.mongodb.core.index.Index
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.query.Update
import org.springframework.stereotype.Service
import org.vechain.indexer.IndexerNames
import org.vechain.indexer.b3tr.event.UserFirstSeenAggregationCompletedEvent
import org.vechain.indexer.event.AbiLoader
import org.vechain.indexer.event.model.abi.AbiElement
import org.vechain.indexer.event.utils.FunctionReturnDecoder
import org.vechain.indexer.thor.client.ThorClient
import org.vechain.indexer.utils.ContractUtils
import org.vechain.indexer.xapp.badge.BadgeType
import org.vechain.indexer.xapp.scoring.CommunityScoreService

/**
 * Computes the **Activity Score**, **Distribution Efficiency Score**, **Community Score**,
 * **Profile Score**, and the composite **Total Score** for each dApp per round, as defined in the
 * VBD Reputation Layer Proposal. Triggered by [UserFirstSeenAggregationCompletedEvent] to ensure
 * [b3tr_user_first_seen][IndexerNames.USER_FIRST_SEEN] data is up-to-date.
 *
 * ## App set per round
 * The set of apps scored for a round is the authoritative on-chain list from
 * `XAllocationVoting.getAppIdsOfRound(roundId)` — not just the apps that happen to have action
 * data. The full on-chain set is also the percentile population for the activity and distribution
 * scores: apps without action data contribute `0` to every metric, forming the baseline that active
 * apps rank above (and they themselves are percentile-ranked at the bottom). This keeps scores
 * meaningful even when only a few apps have activity. Community Score is still computed
 * independently over all apps with rating data.
 *
 * ## Activity Score algorithm
 * For each round, per dApp:
 * 1. **WAU** = distinct users with rewarded actions (row count in
 *    `b3tr_app_action_summaries_round`)
 * 2. **NewUsers** = users whose `firstRoundId` equals the target round (from
 *    `b3tr_user_first_seen`)
 * 3. **Actions** = total rewarded actions (sum of `actionsRewarded`)
 * 4. Apply **log scaling**: `ln(1 + value)`
 * 5. Compute **percentile rank** across all dApps (with tie handling)
 * 6. `ActivityScore = 0.5 × P_WAU + 0.3 × P_NewUsers + 0.2 × P_Actions`
 *
 * ## Distribution Efficiency Score algorithm (Appendix A.3)
 * 1. **Alloc_7d** = B3TR allocated to the dApp this round, from
 *    [b3tr_x_alloc_results][IndexerNames.X_ALLOC_RESULT].`totalAmount` (already 18-decimal scaled
 *    to B3TR units)
 * 2. **Dist_7d** = B3TR actually distributed to users this round, = sum of `totalRewardAmount` in
 *    `b3tr_app_action_summaries_round` (same B3TR units, so the ratio is unit-consistent)
 * 3. `DistributionEfficiency = min(Dist_7d / Alloc_7d, 1)`, or `0` when `Alloc_7d == 0`
 * 4. `DistributionScore = PercentileRank(DistributionEfficiency)` across all dApps (with tie
 *    handling), in `[0, 100]`
 *
 * ## Community Score algorithm (round-independent snapshot)
 * Computed over the current rating/review state (no round filter) and merged into each scored app:
 * 1. Aggregate `AvgRating` / `NumRatings` from [xapp_ratings][IndexerNames.XAPP_RATING], and
 *    `HelpfulVotes` / `UnhelpfulVotes` from [xapp_review_votes][IndexerNames.XAPP_REVIEW_VOTE]
 *    (joined to [xapp_reviews][IndexerNames.XAPP_REVIEW] on `reviewId`, excluding hidden reviews).
 * 2. `ConfidenceFactor = 1 - e^(-NumRatings / K)`, `K = 100`.
 * 3. `HelpfulRatio = HelpfulVotes / (HelpfulVotes + UnhelpfulVotes)` (`0.5` when no votes);
 *    `EngagementFactor = 0.5 + 0.5 × HelpfulRatio`.
 * 4. `AdjustedRating = AvgRating × ConfidenceFactor × EngagementFactor` (`0` when `NumRatings <
 *    R_min`, `R_min = 20`).
 * 5. `CommunityScore = PercentileRank(AdjustedRating)` across eligible apps only (`NumRatings >=
 *    R_min`, with tie handling), in `[0, 100]`. Ineligible apps and apps without rating data get
 *    `CommunityScore = 0`. Implemented in [CommunityScoreService].
 *
 * ## Profile Score algorithm (round-independent snapshot)
 * Computed from [xapp_metadata][IndexerNames.XAPP_METADATA] for the on-chain app set — the latest
 * snapshot plus the inline `_previousVersions` history — and merged into each scored app:
 * 1. `StaticScore = optionalFieldsCount / 8 × 100` (completeness of the 8 optional metadata fields,
 *    from the latest snapshot).
 * 2. `ProductRecency` / `SocialRecency ∈ {0, 0.5, 1.0}`: days since the most recent snapshot whose
 *    `versionHistoryUpdated` / `tweetsUpdated` flag is `1` (≤14d → 1.0, 14–30d → 0.5, >30d → 0).
 *    The update flags are precomputed at write time (see `AppMetadataService`); a content type
 *    whose flag never fired is treated as never updated → recency `0`.
 * 3. `RecencyFactor = (ProductRecency + SocialRecency) / 2`.
 * 4. `ProfileScore = StaticScore × (0.8 + 0.2 × RecencyFactor)`, in `[0, 100]`. Apps without a
 *    metadata document get `ProfileScore = 0`.
 *
 * ## Total Score algorithm
 * Each dimension's weighted contribution is persisted as a `*FinalScore` field, rounded to 2 dp:
 * `activityFinalScore = 0.3 × ActivityScore`, `distributionFinalScore = 0.3 × DistributionScore`,
 * `communityFinalScore = 0.3 × CommunityScore`, `profileFinalScore = 0.1 × ProfileScore`. The
 * composite `totalScore` is the sum of those four already-2-dp final scores (so the four final
 * scores sum exactly to `totalScore`), in `[0, 100]`.
 */
@Service
@Profile("b3tr", "b3tr-app-round-score")
class AppRoundScoreService(
    private val mongoTemplate: MongoTemplate,
    private val thorClient: ThorClient,
    @param:Value("\${business-event.substitutions.X_ALLOC_VOTING_CONTRACT}")
    private val xAllocVotingContract: String,
    private val communityScoreService: CommunityScoreService,
) {
    private val logger = LoggerFactory.getLogger(this::class.java)

    /**
     * ABI for `XAllocationVoting.getAppIdsOfRound(uint256) -> bytes32[]`, the authoritative
     * on-chain list of app ids participating in a round. Loaded lazily from `abis/b3tr` (the
     * function was added to `VeBetterDAO-x-allocations-voting.json`).
     */
    private val getAppIdsOfRoundAbi: AbiElement by lazy {
        AbiLoader.loadFunctions("abis/b3tr", listOf("getAppIdsOfRound")).firstOrNull {
            it.name == "getAppIdsOfRound"
        } ?: throw IllegalArgumentException("Function 'getAppIdsOfRound' not found in ABI")
    }

    companion object {
        // Weights from the proposal
        private val WEIGHT_WAU = BigDecimal("0.5")
        private val WEIGHT_NEW_USERS = BigDecimal("0.3")
        private val WEIGHT_ACTIONS = BigDecimal("0.2")

        private val HUNDRED = BigDecimal("100")
        private val HALF = BigDecimal("0.5")

        private val COMMUNITY_MC = MathContext(20, RoundingMode.HALF_UP)

        // Profile Score constants (VBD Reputation Layer Proposal, Profile Score)
        private val OPTIONAL_FIELDS_TOTAL =
            BigDecimal(8) // total optional metadata fields (M_total)
        private val PROFILE_BASE_WEIGHT = BigDecimal("0.8") // completeness absolute weight
        private val PROFILE_RECENCY_WEIGHT = BigDecimal("0.2") // recency modulation weight
        private const val MILLIS_PER_SECOND = 1000L
        private const val MILLIS_PER_DAY = 86_400_000L
        private const val RECENCY_DAYS_FULL = 14L // ≤14d → recency 1.0
        private const val RECENCY_DAYS_PARTIAL = 30L // 14–30d → recency 0.5
        private const val PREVIOUS_VERSIONS_FIELD = "_previousVersions"

        // Total Score weights (VBD Reputation Layer Proposal, Total Score)
        private val WEIGHT_ACTIVITY = BigDecimal("0.3")
        private val WEIGHT_DISTRIBUTION = BigDecimal("0.3")
        private val WEIGHT_COMMUNITY = BigDecimal("0.3")
        private val WEIGHT_PROFILE = BigDecimal("0.1")

        // Navigators Pick badge: number of top dApps (by navigator voting weight) that earn it.
        private const val NAVIGATORS_PICK_TOP_N = 3

        // ln1pBig helpers
        private val TWO = BigDecimal(2)
        private val LN2 = BigDecimal("0.69314718055994530941723212145817656807550013436025")
        private val MATH_CTX = MathContext(34, RoundingMode.HALF_UP)

        // Internal computation scale; results are rounded to 4 dp when stored.
        private const val COMPUTE_SCALE = 10
        private const val MAX_LN_ITER = 200

        /**
         * Percentile rank with tie handling. `P(x_i) = (# { j | x_j < x_i } + 0.5 × T) / N × 100`
         * where T = number of values tied with x_i. Static utility — callable from Kotlin and Java
         * as `AppRoundScoreService.percentileRank(value, allValues)`.
         *
         * Operates on [BigDecimal] so comparisons and ties are exact; `below`, `tied`, and `n` are
         * exact integer counts. Returns `0` for an empty population.
         *
         * @param value the value to rank
         * @param allValues the full population to rank against (may include [value])
         * @return percentile rank in `[0, 100]`, computed at [COMPUTE_SCALE] precision
         */
        @JvmStatic
        fun percentileRank(value: BigDecimal, allValues: List<BigDecimal>): BigDecimal {
            val n = allValues.size
            if (n == 0) return BigDecimal.ZERO
            var below = 0
            var tied = 0
            for (v in allValues) {
                when (v.compareTo(value)) {
                    -1 -> below++
                    0 -> tied++
                }
            }
            val belowBd = BigDecimal(below)
            val nBd = BigDecimal(n)
            return if (tied == 1) {
                belowBd.divide(nBd, COMPUTE_SCALE, RoundingMode.HALF_UP).multiply(HUNDRED)
            } else {
                belowBd
                    .add(HALF.multiply(BigDecimal(tied)))
                    .divide(nBd, COMPUTE_SCALE, RoundingMode.HALF_UP)
                    .multiply(HUNDRED)
            }
        }

        /**
         * High-precision `ln(1 + x)` for non-negative x, computed entirely with [BigDecimal] (no
         * Double). Static utility — callable from Kotlin and Java as
         * `AppRoundScoreService.ln1pBig(x)`.
         *
         * Argument reduction `1+x = m · 2^p` with `m ∈
         * [1, 2)`, then `ln(m) = 2 · artanh((m-1)/(m+1))`; the artanh series converges fast because `(m-1)/(m+1) ∈ [0, 1/3)`. `ln(2)` is a precomputed high-precision constant, so the whole chain stays exact up to the [MATH_CTX]
         * working precision. Returns `0` for `x ≤ 0`.
         *
         * @param x a non-negative value
         * @return `ln(1 + x)` at [MATH_CTX] precision
         */
        @JvmStatic
        fun ln1pBig(x: BigDecimal): BigDecimal {
            if (x.signum() <= 0) return BigDecimal.ZERO
            val y = x.add(BigDecimal.ONE) // 1 + x, y >= 1

            // reduce y = m · 2^p, m in [1, 2)
            var p = 0
            var m = y
            while (m.compareTo(TWO) >= 0) {
                m = m.divide(TWO, MATH_CTX)
                p++
            }

            // ln(m) = 2 · artanh(t), t = (m-1)/(m+1), t in [0, 1/3)
            val t = m.subtract(BigDecimal.ONE).divide(m.add(BigDecimal.ONE), MATH_CTX)

            // artanh(t) = Σ_{k>=0} t^(2k+1)/(2k+1)
            var sum = BigDecimal.ZERO
            var term = t // t^1
            var k = 1
            var guard = 0
            while (guard++ < MAX_LN_ITER) {
                val inc = term.divide(BigDecimal(k), MATH_CTX)
                sum = sum.add(inc, MATH_CTX)
                if (inc.signum() == 0) break
                term = term.multiply(t, MATH_CTX).multiply(t, MATH_CTX) // t^(2k+1) -> t^(2k+3)
                k += 2
            }
            return sum.multiply(TWO, MATH_CTX).add(BigDecimal(p).multiply(LN2, MATH_CTX), MATH_CTX)
        }
    }

    @PostConstruct
    fun initIndexes() {
        val collectionName = IndexerNames.APP_ROUND_SCORE.COLLECTION
        if (!mongoTemplate.collectionExists(collectionName)) {
            mongoTemplate.createCollection(collectionName)
        }
        val indexOps = mongoTemplate.indexOps(collectionName)
        indexOps.ensureIndex(
            Index().on("appId", Sort.Direction.ASC).on("roundId", Sort.Direction.ASC).unique()
        )
        indexOps.ensureIndex(Index().on("roundId", Sort.Direction.ASC))
        logger.info("Indexes ensured for collection {}", collectionName)
    }

    data class AppMetrics(val actions: Long, val wau: Long, val distributedB3tr: BigDecimal)

    /**
     * A per-app score document assembled in the first pass of [scoreRound], awaiting its `rank`.
     * The `rank` field is filled in the second pass, once every app's [totalScore] is known and the
     * within-round ranking can be computed (see [rankByTotalScore]).
     */
    private data class PendingScoreUpsert(
        val appId: String,
        val totalScore: BigDecimal,
        val update: Update,
    )

    // ──────────────────────────── Entry point ────────────────────────────

    @EventListener
    fun onFirstSeenCompleted(event: UserFirstSeenAggregationCompletedEvent) {
        if (event.processedUpToRound <= 0) {
            logger.info("something is error for get round by Thor, skipping AppRoundScore")
            return
        }
        try {
            computeScores(event.processedUpToRound)
        } catch (e: Exception) {
            logger.error("AppRoundScore computation failed", e)
        }
    }

    // ──────────────────────────── Core logic ────────────────────────────

    private fun computeScores(targetRound: Int) {
        val started = System.currentTimeMillis()
        logger.info("Starting AppRoundScore computation for round {}...", targetRound)

        val sourceCollection = IndexerNames.APP_ROUND_ACTION_SUMMARY.COLLECTION
        val firstSeenCollection = IndexerNames.USER_FIRST_SEEN.COLLECTION
        val xAllocCollection = IndexerNames.X_ALLOC_RESULT.COLLECTION
        val targetCollection = IndexerNames.APP_ROUND_SCORE.COLLECTION

        val firstSeenAvailable = mongoTemplate.collectionExists(firstSeenCollection)
        if (!firstSeenAvailable) {
            logger.warn(
                "Collection {} does not exist, newUsers will be 0 for all apps",
                firstSeenCollection,
            )
        }

        val totalScored =
            scoreRound(
                sourceCollection = sourceCollection,
                firstSeenCollection = firstSeenCollection,
                xAllocCollection = xAllocCollection,
                targetCollection = targetCollection,
                roundId = targetRound,
                firstSeenAvailable = firstSeenAvailable,
            )

        // Award xApp badges for this round (TOP_ECOSYSTEM_DAPP ranks by totalScore,
        // TOP_DISTRIBUTION_PERFORMER by distributionScore, NAVIGATORS_PICK by navigator voting
        // rank). Each badge type guards on its own data source, so this runs even when this round
        // produced no scored apps (e.g. the navigator ranking can exist independently).
        awardBadges(targetRound)

        val elapsed = System.currentTimeMillis() - started
        logger.info(
            "AppRoundScore completed: round {}, {} apps scored, {}ms elapsed",
            targetRound,
            totalScored,
            elapsed,
        )
    }

    private fun scoreRound(
        sourceCollection: String,
        firstSeenCollection: String,
        xAllocCollection: String,
        targetCollection: String,
        roundId: Int,
        firstSeenAvailable: Boolean,
    ): Int {
        // 0. Authoritative app list for this round comes from the XAllocationVoting contract
        //    (getAppIdsOfRound). The full set — including apps with no action data — is scored and
        //    forms the percentile population (see below).
        val onChainAppIds = fetchAppIdsOfRound(roundId)
        if (onChainAppIds.isEmpty()) {
            logger.info(
                "No apps returned by getAppIdsOfRound for round {} (chain error or empty round); skipping",
                roundId,
            )
            // No apps to score → nothing to persist → skip the round.
            return 0
        }

        // 1. Aggregate per-app metrics from action summaries (a subset of the on-chain apps)
        val appMetrics = aggregateAppMetrics(sourceCollection, roundId)

        // 2. Count new users per app from first-seen collection
        val newUsersData =
            if (firstSeenAvailable) {
                countNewUsers(firstSeenCollection, roundId)
            } else {
                emptyMap()
            }

        // 2b. Fetch B3TR allocations per app from x-allocation results (Alloc_7d)
        val allocations = getAllocations(xAllocCollection, roundId)

        // The percentile population is the FULL on-chain app set for the round. Apps without
        // action-summary data contribute 0 to every metric, so they form the baseline that active
        // apps rank above — and they themselves are percentile-ranked at the bottom. This keeps
        // scores meaningful even when only a few apps have activity.

        // 3. Collect raw values for percentile computation (BigDecimal; exact for Long metrics)
        val allWau = onChainAppIds.map { BigDecimal(appMetrics[it]?.wau ?: 0L) }
        val allNewUsers = onChainAppIds.map { BigDecimal(newUsersData[it] ?: 0L) }
        val allActions = onChainAppIds.map { BigDecimal(appMetrics[it]?.actions ?: 0L) }

        // Pre-compute log-scaled values once (BigDecimal ln1p), reused for every app's ranking
        val logAllWau = allWau.map { ln1pBig(it) }
        val logAllNewUsers = allNewUsers.map { ln1pBig(it) }
        val logAllActions = allActions.map { ln1pBig(it) }

        // 3b. Pre-compute distribution efficiencies per app (population for percentile ranking)
        val efficiencies =
            onChainAppIds.associateWith { appId ->
                distributionEfficiency(
                    appMetrics[appId]?.distributedB3tr ?: BigDecimal.ZERO,
                    allocations[appId] ?: BigDecimal.ZERO,
                )
            }
        val allEfficiencies = efficiencies.values.toList()

        // 3c. Community Score — computed over the current rating/review state (round-independent),
        // then merged into each scored app. See CommunityScoreService for the ranking population.
        val communityScores =
            communityScoreService.computeCommunityScores().associateBy { it.appId }

        // 3d. Profile Score — computed from xapp_metadata for the on-chain app set
        // (round-independent snapshot), then merged into each scored app. Population is the apps
        // that have a metadata document.
        val profileScores = computeProfileScores(onChainAppIds)

        // 4. Build per-app score documents. The `rank` field depends on every app's totalScore, so
        // the upserts are deferred: the first pass computes components and assembles each Update,
        // the second pass fills `rank` once the round is fully scored and executes the bulk write.
        val now = System.currentTimeMillis()
        val pending = mutableListOf<PendingScoreUpsert>()

        for (appId in onChainAppIds) {
            val metrics = appMetrics[appId]

            // Log scaling (BigDecimal throughout); apps without data contribute ln(1 + 0) = 0
            val logWau = ln1pBig(BigDecimal(metrics?.wau ?: 0L))
            val logNew = ln1pBig(BigDecimal(newUsersData[appId] ?: 0L))
            val logAct = ln1pBig(BigDecimal(metrics?.actions ?: 0L))

            // Percentile ranks with tie handling (BigDecimal comparisons — exact ties)
            val pWau = percentileRank(logWau, logAllWau)
            val pNew =
                if (firstSeenAvailable) {
                    percentileRank(logNew, logAllNewUsers)
                } else BigDecimal.ZERO
            val pAct = percentileRank(logAct, logAllActions)

            // Weighted activity score (BigDecimal arithmetic)
            val activityScore =
                WEIGHT_WAU.multiply(pWau)
                    .add(WEIGHT_NEW_USERS.multiply(pNew))
                    .add(WEIGHT_ACTIONS.multiply(pAct))

            // Distribution efficiency score (Appendix A.3)
            val distB3tr = metrics?.distributedB3tr ?: BigDecimal.ZERO
            val allocB3tr = allocations[appId] ?: BigDecimal.ZERO
            val distEff = efficiencies[appId] ?: BigDecimal.ZERO
            val distScore = percentileRank(distEff, allEfficiencies)

            // Community Score (round-independent snapshot merged into this app's round document)
            val community = communityScores[appId]

            // Profile Score (round-independent snapshot merged into this app's round document)
            val profile = profileScores[appId]

            // Persisted dimension scores (4 dp). The final (weighted) scores below are derived from
            // these exact stored values, so each *FinalScore is reproducible as <dimensionScore> ×
            // <weight> and the four final scores sum exactly to totalScore.
            val activityScoreValue = activityScore.setScale(4, RoundingMode.HALF_UP)
            val distributionScoreValue = distScore.setScale(4, RoundingMode.HALF_UP)
            val communityScoreValue =
                (community?.communityScore ?: BigDecimal.ZERO).setScale(4, RoundingMode.HALF_UP)
            val profileScoreValue =
                (profile?.profileScore ?: BigDecimal.ZERO).setScale(4, RoundingMode.HALF_UP)

            // Weighted contributions (VBD Reputation Layer Proposal, Total Score), each rounded to
            // 2 dp: Activity/Distribution/Community × 30%, Profile × 10%.
            val activityFinalScore =
                activityScoreValue.multiply(WEIGHT_ACTIVITY).setScale(2, RoundingMode.HALF_UP)
            val distributionFinalScore =
                distributionScoreValue
                    .multiply(WEIGHT_DISTRIBUTION)
                    .setScale(2, RoundingMode.HALF_UP)
            val communityFinalScore =
                communityScoreValue.multiply(WEIGHT_COMMUNITY).setScale(2, RoundingMode.HALF_UP)
            val profileFinalScore =
                profileScoreValue.multiply(WEIGHT_PROFILE).setScale(2, RoundingMode.HALF_UP)

            // Total Score = sum of the four rounded final scores, persisted as `totalScore`.
            val totalScore =
                activityFinalScore
                    .add(distributionFinalScore)
                    .add(communityFinalScore)
                    .add(profileFinalScore)

            val id = "${appId}_$roundId"
            val update =
                Update()
                    .set("_id", id)
                    .set("appId", appId)
                    .set("roundId", roundId)
                    .set("wau", metrics?.wau ?: 0L)
                    .set("newUsers", newUsersData[appId] ?: 0L)
                    .set("actions", metrics?.actions ?: 0L)
                    .set("pWau", pWau.setScale(4, RoundingMode.HALF_UP))
                    .set("pNewUsers", pNew.setScale(4, RoundingMode.HALF_UP))
                    .set("pActions", pAct.setScale(4, RoundingMode.HALF_UP))
                    .set("activityScore", activityScoreValue)
                    .set("allocB3tr", allocB3tr.setScale(6, RoundingMode.HALF_UP))
                    .set("distributedB3tr", distB3tr.setScale(6, RoundingMode.HALF_UP))
                    .set("distributionEfficiency", distEff.setScale(6, RoundingMode.HALF_UP))
                    .set("distributionScore", distributionScoreValue)
                    .set(
                        "avgRating",
                        (community?.avgRating ?: BigDecimal.ZERO).setScale(4, RoundingMode.HALF_UP),
                    )
                    .set("numRatings", community?.numRatings ?: 0L)
                    .set("helpfulVotes", community?.helpfulVotes ?: 0L)
                    .set("unhelpfulVotes", community?.unhelpfulVotes ?: 0L)
                    .set(
                        "confidenceFactor",
                        (community?.confidenceFactor ?: BigDecimal.ZERO).setScale(
                            6,
                            RoundingMode.HALF_UP,
                        ),
                    )
                    .set(
                        "helpfulRatio",
                        (community?.helpfulRatio ?: BigDecimal.ZERO).setScale(
                            6,
                            RoundingMode.HALF_UP,
                        ),
                    )
                    .set(
                        "engagementFactor",
                        (community?.engagementFactor ?: BigDecimal.ZERO).setScale(
                            6,
                            RoundingMode.HALF_UP,
                        ),
                    )
                    .set(
                        "adjustedRating",
                        (community?.adjustedRating ?: BigDecimal.ZERO).setScale(
                            6,
                            RoundingMode.HALF_UP,
                        ),
                    )
                    .set("communityScore", communityScoreValue)
                    .set("optionalFieldsCount", profile?.optionalFieldsCount ?: 0)
                    .set(
                        "staticScore",
                        (profile?.staticScore ?: BigDecimal.ZERO).setScale(4, RoundingMode.HALF_UP),
                    )
                    .set(
                        "productRecency",
                        (profile?.productRecency ?: BigDecimal.ZERO).setScale(
                            6,
                            RoundingMode.HALF_UP,
                        ),
                    )
                    .set(
                        "socialRecency",
                        (profile?.socialRecency ?: BigDecimal.ZERO).setScale(
                            6,
                            RoundingMode.HALF_UP,
                        ),
                    )
                    .set(
                        "recencyFactor",
                        (profile?.recencyFactor ?: BigDecimal.ZERO).setScale(
                            6,
                            RoundingMode.HALF_UP,
                        ),
                    )
                    .set("profileScore", profileScoreValue)
                    .set("activityFinalScore", activityFinalScore)
                    .set("distributionFinalScore", distributionFinalScore)
                    .set("communityFinalScore", communityFinalScore)
                    .set("profileFinalScore", profileFinalScore)
                    .set("totalScore", totalScore)
                    .set("updatedAt", now)
            // Rank on the persisted totalScore (the same value written above) so `rank` stays
            // consistent with awardTopPercentageBadge, which ranks against the stored value.
            pending.add(PendingScoreUpsert(appId, totalScore, update))
        }

        // 5. Rank within the round (competition ranking, appId tie-break), then bulk upsert.
        val rankByAppId = rankByTotalScore(pending.map { it.appId to it.totalScore })
        if (pending.isNotEmpty()) {
            val bulkOps = mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED, targetCollection)
            for ((appId, _, update) in pending) {
                update.set("rank", rankByAppId.getValue(appId))
                bulkOps.upsert(Query(Criteria.where("_id").`is`("${appId}_$roundId")), update)
            }
            bulkOps.execute()
        }
        val count = pending.size
        logger.info("Round {} scored: {} apps", roundId, count)
        return count
    }

    /**
     * Competition ranking ("1224") of [appIdScorePairs] by score descending, with `appId` ascending
     * as a stable tie-break — the same convention as [awardTopPercentageBadge]. Apps with equal
     * scores share the rank of the first position in their tie group; the next lower score jumps to
     * its 1-based position. Returns an `appId` → 1-based rank map (empty for an empty population).
     */
    private fun rankByTotalScore(
        appIdScorePairs: List<Pair<String, BigDecimal>>
    ): Map<String, Int> {
        if (appIdScorePairs.isEmpty()) return emptyMap()
        val ranked =
            appIdScorePairs.sortedWith(
                compareByDescending<Pair<String, BigDecimal>> { it.second }.thenBy { it.first }
            )
        val ranks = HashMap<String, Int>(ranked.size)
        var currentRank = 1
        for (i in ranked.indices) {
            if (i > 0 && ranked[i].second.compareTo(ranked[i - 1].second) != 0) {
                currentRank = i + 1
            }
            ranks[ranked[i].first] = currentRank
        }
        return ranks
    }

    // ──────────────────────────── Badge awarding ────────────────────────────

    /** An app's persisted round scores used as ranking inputs for badge awarding. */
    private data class AppRoundScoreValue(
        val appId: String,
        val totalScore: BigDecimal,
        val distributionScore: BigDecimal,
        // Within-round competition rank by totalScore — the `rank` field persisted in
        // b3tr_app_round_scores. 0 when the document has no rank (e.g. an un-backfilled round).
        val rank: Int,
    )

    /**
     * Awards xApp badges for [roundId] based on the scores this service just persisted in
     * [b3tr_app_round_scores][IndexerNames.APP_ROUND_SCORE]:
     * - [BadgeType.TOP_ECOSYSTEM_DAPP] — top 10% by `totalScore`.
     * - [BadgeType.TOP_DISTRIBUTION_PERFORMER] — top 10% by `distributionScore`.
     *
     * Upserts one [org.vechain.indexer.xapp.badge.BadgeAward] per winner into
     * [xapp_badge_awards][IndexerNames.XAPP_BADGE_AWARD], keyed by `appId_badgeType_round` so
     * re-running is idempotent. This mirrors the badge logic that previously lived in
     * `CycleScoringService.awardBadges`, but ranks against the authoritative per-round scores this
     * service writes instead of `CycleScoringService`'s in-memory totals.
     */
    private fun awardBadges(roundId: Int) {
        val badgeCollection = IndexerNames.XAPP_BADGE_AWARD.COLLECTION
        val now = System.currentTimeMillis()
        val roundBlockTimestamp =
            mongoTemplate
                .findOne(
                    Query(Criteria.where("roundId").`is`(roundId)),
                    Document::class.java,
                    IndexerNames.CYCLE_INFO.COLLECTION,
                )
                ?.let { (it.get("blockTimestamp") as? Number)?.toLong() }

        // 1 & 2. Score-based badges rank against b3tr_app_round_scores; skip both when that round
        // has no scored apps (the data source they depend on is empty).
        val scores = fetchRoundScores(roundId)
        if (scores.isNotEmpty()) {
            // 1. APP评分徽章 (TOP_ECOSYSTEM_DAPP): top 10% by totalScore
            awardTopPercentageBadge(
                badgeCollection,
                roundId,
                scores,
                BadgeType.TOP_ECOSYSTEM_DAPP,
                valueSelector = { it.totalScore },
                allowTieExpansion = true,
                // Badge rank = the app's `rank` in b3tr_app_round_scores (within-round position by
                // totalScore), not the recomputed top-10% rank.
                rankSelector = { it.rank },
                // Badge `score` = the app's totalScore from b3tr_app_round_scores.
                scoreSelector = { it.totalScore },
                now = now,
                roundBlockTimestamp = roundBlockTimestamp,
            )

            // 2. 高效分发徽章 (TOP_DISTRIBUTION_PERFORMER): top 10% by distributionScore
            awardTopPercentageBadge(
                badgeCollection,
                roundId,
                scores,
                BadgeType.TOP_DISTRIBUTION_PERFORMER,
                valueSelector = { it.distributionScore },
                allowTieExpansion = false,
                // Badge `score` = the app's distributionScore from b3tr_app_round_scores.
                scoreSelector = { it.distributionScore },
                now = now,
                roundBlockTimestamp = roundBlockTimestamp,
            )
        }

        // 3. Navigator精选徽章 (NAVIGATORS_PICK): top N by navigator voting weight. Independent of
        // b3tr_app_round_scores — a round can have a navigator ranking without scored apps.
        awardNavigatorsPickBadges(badgeCollection, roundId, now, roundBlockTimestamp)
    }

    /**
     * Reads the per-app scores persisted for [roundId] from
     * [b3tr_app_round_scores][IndexerNames.APP_ROUND_SCORE]. `totalScore` and `distributionScore`
     * are stored as 4-dp decimals; a missing value defaults to `0.0`. Returns an empty list if the
     * collection does not exist or has no documents for the round.
     */
    private fun fetchRoundScores(roundId: Int): List<AppRoundScoreValue> {
        val docs =
            mongoTemplate.find(
                Query(Criteria.where("roundId").`is`(roundId)),
                Document::class.java,
                IndexerNames.APP_ROUND_SCORE.COLLECTION,
            )
        return docs.mapNotNull { doc ->
            val appId = doc.getString("appId") ?: return@mapNotNull null
            AppRoundScoreValue(
                appId = appId,
                totalScore = toBigDecimal(doc.get("totalScore")),
                distributionScore = toBigDecimal(doc.get("distributionScore")),
                rank = (doc.get("rank") as? Number)?.toInt() ?: 0,
            )
        }
    }

    /**
     * Awards a badge of [badgeType] to the top 10% of [apps] (truncated, minimum 1 — e.g. 25 apps →
     * 2 badges) as ranked by [valueSelector]. Sorting is by value descending, then by `appId` for
     * stable ordering. Equal values share a rank; when [allowTieExpansion] is `true`, apps tied
     * with the last in-top-10% entry are included as well. Each winner is upserted into
     * [badgeCollection] keyed by `appId_badgeType_round`.
     *
     * The persisted badge `rank` defaults to the recomputed top-10% competition rank, but when
     * [rankSelector] is supplied it is used instead — e.g. [BadgeType.TOP_ECOSYSTEM_DAPP] takes the
     * app's `rank` straight from `b3tr_app_round_scores` (its within-round position by
     * `totalScore`). When [scoreSelector] is supplied, the app's score is also persisted on the
     * badge under `score` — e.g. [BadgeType.TOP_ECOSYSTEM_DAPP] records `totalScore`.
     */
    private fun awardTopPercentageBadge(
        badgeCollection: String,
        roundId: Int,
        apps: List<AppRoundScoreValue>,
        badgeType: BadgeType,
        valueSelector: (AppRoundScoreValue) -> BigDecimal,
        allowTieExpansion: Boolean,
        rankSelector: ((AppRoundScoreValue) -> Int)? = null,
        scoreSelector: ((AppRoundScoreValue) -> BigDecimal)? = null,
        now: Long,
        roundBlockTimestamp: Long?,
    ) {
        if (apps.isEmpty()) return

        // Sort descending by value, then by appId for stable ordering
        val ranked =
            apps.sortedWith(
                compareByDescending<AppRoundScoreValue> { valueSelector(it) }.thenBy { it.appId }
            )

        // Top 10%, truncating the remainder (e.g. 25 apps × 10% = 2.5 → 2 badges). Integer division
        // keeps it exact (no floating-point error). Minimum 1, so the winner always gets one.
        val topCount = maxOf(1, ranked.size / 10)

        // Assign ranks with tie handling
        val rankedApps = mutableListOf<Pair<AppRoundScoreValue, Int>>()
        var currentRank = 1
        for (i in ranked.indices) {
            if (i > 0) {
                val prevValue = valueSelector(ranked[i - 1])
                val currValue = valueSelector(ranked[i])
                if (currValue.compareTo(prevValue) != 0) {
                    currentRank = i + 1
                }
            }
            // Only include apps within the top 10%
            if (allowTieExpansion) {
                // Include if rank <= topCount, or if tied with the last included app
                if (currentRank <= topCount) {
                    rankedApps.add(ranked[i] to currentRank)
                } else {
                    // Check if tied with the last included app
                    val lastIncludedValue = valueSelector(rankedApps.last().first)
                    if (valueSelector(ranked[i]).compareTo(lastIncludedValue) == 0) {
                        rankedApps.add(ranked[i] to currentRank)
                    }
                }
            } else {
                if (currentRank <= topCount) {
                    rankedApps.add(ranked[i] to currentRank)
                }
            }
        }

        if (rankedApps.isEmpty()) return

        // Bulk upsert badge awards
        val bulkOps = mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED, badgeCollection)
        for ((app, computedRank) in rankedApps) {
            // Prefer the persisted round-score rank when a rankSelector is supplied (e.g.
            // TOP_ECOSYSTEM_DAPP takes rank from b3tr_app_round_scores); fall back to the
            // recomputed
            // competition rank otherwise, or when the persisted rank is missing (0).
            val rank = rankSelector?.let { sel -> sel(app).takeIf { r -> r > 0 } } ?: computedRank
            val id = "${app.appId}_${badgeType.value}_$roundId"
            val update =
                Update()
                    .set("_id", id)
                    .set("version", 1)
                    .set("blockId", "")
                    .set("blockNumber", 0L)
                    .set("blockTimestamp", now)
                    .set("appId", app.appId)
                    .set("badgeType", badgeType.value)
                    .set("round", roundId.toLong())
                    .set("rank", rank)
                    .set("isPublished", true)
                    .set("roundBlockTimestamp", roundBlockTimestamp)
            // Optional `score` field, e.g. TOP_ECOSYSTEM_DAPP records the app's totalScore from
            // b3tr_app_round_scores.
            scoreSelector?.let { update.set("score", it(app)) }
            bulkOps.upsert(Query(Criteria.where("_id").`is`(id)), update)
        }
        bulkOps.execute()

        logger.info(
            "Round {}: awarded {} {} badges (top {}% of {} apps)",
            roundId,
            rankedApps.size,
            badgeType.name,
            topCount,
            apps.size,
        )
    }

    /**
     * Awards the [BadgeType.NAVIGATORS_PICK] badge to the top [NAVIGATORS_PICK_TOP_N] dApps in
     * [roundId] by navigator voting weight. The ranking lives in
     * [b3tr_navigator_dapp_vote_round_rankings][IndexerNames.NAVIGATOR_DAPP_VOTE_ROUND_RANKING]:
     * its `dappRankings` array is already ranked at write time (rank 1 = highest weight), so the
     * entries with `rank ≤ [NAVIGATORS_PICK_TOP_N]` each get a
     * [org.vechain.indexer.xapp.badge.BadgeAward], upserted into [badgeCollection] keyed by
     * `appId_badgeType_round`. The `appId` is lower-cased to match the `_id` convention of the
     * other badges (and `b3tr_app_round_scores.appId`). No-op if no ranking document exists for the
     * round.
     */
    private fun awardNavigatorsPickBadges(
        badgeCollection: String,
        roundId: Int,
        now: Long,
        roundBlockTimestamp: Long?,
    ) {
        val doc =
            mongoTemplate.findOne(
                Query(Criteria.where("roundId").`is`(roundId)),
                Document::class.java,
                IndexerNames.NAVIGATOR_DAPP_VOTE_ROUND_RANKING.COLLECTION,
            ) ?: return

        val rankings = doc.get("dappRankings") as? List<*> ?: return
        // dappRankings is written as NavigatorDappVoteRankingEntry; rank is already 1-based and
        // sorted by weight descending. Keep the (rank, appId) pairs in the top N.
        val winners =
            rankings.mapNotNull { entry ->
                val e = entry as? Document ?: return@mapNotNull null
                val rank = (e.get("rank") as? Number)?.toInt() ?: return@mapNotNull null
                val appId = e.getString("appId") ?: return@mapNotNull null
                if (rank in 1..NAVIGATORS_PICK_TOP_N) rank to appId.lowercase() else null
            }
        if (winners.isEmpty()) return

        val bulkOps = mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED, badgeCollection)
        for ((rank, appId) in winners) {
            val id = "${appId}_${BadgeType.NAVIGATORS_PICK.value}_$roundId"
            bulkOps.upsert(
                Query(Criteria.where("_id").`is`(id)),
                Update()
                    .set("_id", id)
                    .set("version", 1)
                    .set("blockId", "")
                    .set("blockNumber", 0L)
                    .set("blockTimestamp", now)
                    .set("appId", appId)
                    .set("badgeType", BadgeType.NAVIGATORS_PICK.value)
                    .set("round", roundId.toLong())
                    .set("rank", rank)
                    .set("isPublished", true)
                    .set("roundBlockTimestamp", roundBlockTimestamp),
            )
        }
        bulkOps.execute()

        logger.info(
            "Round {}: awarded {} {} badges (top {} of navigator rankings)",
            roundId,
            winners.size,
            BadgeType.NAVIGATORS_PICK.name,
            NAVIGATORS_PICK_TOP_N,
        )
    }

    // ──────────────────────────── On-chain app lookup ────────────────────────────

    /**
     * Calls `XAllocationVoting.getAppIdsOfRound(roundId)` at the best block and returns the app ids
     * (on-chain `bytes32`) as lower-cased `0x`-hex strings — the same format used as `appId` in the
     * action-summary and x-allocation collections. Returns an empty list if the call reverts or
     * yields no data, so [scoreRound] can treat that as "nothing to score for this round".
     */
    private fun fetchAppIdsOfRound(roundId: Int): List<String> = runBlocking {
        val clause =
            ContractUtils.createClause(
                xAllocVotingContract,
                getAppIdsOfRoundAbi,
                BigInteger.valueOf(roundId.toLong()),
            )
        val response = thorClient.inspectClauses(listOf(clause)).first()
        if (response.reverted) {
            logger.warn("getAppIdsOfRound({}) reverted: {}", roundId, response.vmError)
            return@runBlocking emptyList()
        }
        val data = response.data
        if (data.isBlank() || data == "0x") return@runBlocking emptyList()

        val decoded = FunctionReturnDecoder.decode(data, getAppIdsOfRoundAbi.outputs)
        val raw = decoded[""] as? List<*> ?: emptyList<Any?>()
        raw.mapNotNull { normalizeAppId(it) }
    }

    /**
     * Normalizes a decoded `bytes32` element to a lower-cased `0x`-hex string. indexer-core's
     * `Types.BYTES` decoder returns static `bytes32` as a `0x`-prefixed hex [String] (verified in
     * `FunctionReturnDecoderTest`), so `getAppIdsOfRound`'s `bytes32[]` output is a `List<String>`.
     * Anything unexpected yields `null`, so the offending app id is skipped rather than crashing
     * the whole round.
     */
    private fun normalizeAppId(value: Any?): String? {
        if (value !is String) {
            logger.warn("Unexpected app-id element type: {}", value?.javaClass?.name)
            return null
        }
        val hex = value.lowercase()
        return if (hex.startsWith("0x")) hex else "0x$hex"
    }

    // ──────────────────────────── Aggregation helpers ────────────────────────────

    /**
     * Groups by appId for a given round, producing:
     * - `actions` = sum of `actionsRewarded`
     * - `wau` = count of rows (= distinct users in this app+round)
     * - `distributedB3tr` = sum of `totalRewardAmount` (= Dist_7d; already 18-decimal scaled to
     *   B3TR units by the action-summary write path)
     */
    private fun aggregateAppMetrics(collection: String, roundId: Int): Map<String, AppMetrics> {
        val aggregation =
            Aggregation.newAggregation(
                Aggregation.match(Criteria.where("roundId").`is`(roundId)),
                Aggregation.group("appId")
                    .sum("actionsRewarded")
                    .`as`("actions")
                    .sum("totalRewardAmount")
                    .`as`("distributedB3tr")
                    .count()
                    .`as`("wau"),
            )

        val results = mongoTemplate.aggregate(aggregation, collection, Document::class.java)
        val map = mutableMapOf<String, AppMetrics>()
        results.forEach { doc ->
            val appId = (doc["_id"] as? String)?.lowercase() ?: return@forEach
            val actions = toLong(doc.get("actions"))
            val wau = toLong(doc.get("wau"))
            val distributedB3tr = toBigDecimal(doc.get("distributedB3tr"))
            map[appId] = AppMetrics(actions = actions, wau = wau, distributedB3tr = distributedB3tr)
        }
        return map
    }

    /**
     * Reads per-app B3TR allocation (Alloc_7d) for a round from
     * [b3tr_x_alloc_results][IndexerNames.X_ALLOC_RESULT], keyed by lower-cased `appId`.
     * `totalAmount` is already 18-decimal scaled to B3TR units; a `null`/absent value (no rewards
     * claimed yet) is treated as `0`.
     */
    private fun getAllocations(collection: String, roundId: Int): Map<String, BigDecimal> {
        val results =
            mongoTemplate.find(
                Query(Criteria.where("roundId").`is`(roundId)),
                Document::class.java,
                collection,
            )
        val map = mutableMapOf<String, BigDecimal>()
        for (doc in results) {
            val appId = doc.getString("appId")?.lowercase() ?: continue
            map[appId] = toBigDecimal(doc.get("totalAmount"))
        }
        return map
    }

    /**
     * Distribution efficiency = `min(Dist_7d / Alloc_7d, 1)`, or `0` when `Alloc_7d == 0` (VBD
     * Reputation Layer Proposal, Appendix A.3). Both inputs are in human B3TR units, so the ratio
     * is unit-consistent. Result is in `[0, 1]`.
     */
    private fun distributionEfficiency(distributed: BigDecimal, allocated: BigDecimal): BigDecimal =
        if (allocated.compareTo(BigDecimal.ZERO) > 0) {
            val ratio = distributed.divide(allocated, COMPUTE_SCALE, RoundingMode.HALF_UP)
            if (ratio.compareTo(BigDecimal.ONE) > 0) BigDecimal.ONE else ratio
        } else {
            BigDecimal.ZERO
        }

    /** Counts new users per appId where `firstRoundId == roundId`. */
    private fun countNewUsers(collection: String, roundId: Int): Map<String, Long> {
        val aggregation =
            Aggregation.newAggregation(
                Aggregation.match(Criteria.where("firstRoundId").`is`(roundId)),
                Aggregation.group("appId").count().`as`("count"),
            )

        val results = mongoTemplate.aggregate(aggregation, collection, Document::class.java)
        val map = mutableMapOf<String, Long>()
        results.forEach { doc ->
            val appId = (doc["_id"] as? String)?.lowercase() ?: return@forEach
            map[appId] = toLong(doc.get("count"))
        }
        return map
    }

    // ──────────────────────────── Profile Score ────────────────────────────

    /** Profile Score components for a single app, persisted into the round-score document. */
    private data class ProfileScoreData(
        val optionalFieldsCount: Int,
        val staticScore: BigDecimal,
        val productRecency: BigDecimal,
        val socialRecency: BigDecimal,
        val recencyFactor: BigDecimal,
        val profileScore: BigDecimal,
    )

    /**
     * The subset of an `AppMetadata` snapshot that feeds the Profile Score — extracted from the
     * current document and each inline `_previousVersions` entry.
     */
    private data class AppMetadataSnapshot(
        val blockTimestamp: Long, // Unix seconds (when the snapshot was published on-chain)
        val versionHistoryUpdated: Int, // 1 iff version_history changed in this snapshot
        val tweetsUpdated: Int, // 1 iff tweets changed in this snapshot
        val optionalFieldsCount: Int, // 0-8 filled optional fields
    )

    /**
     * Computes the Profile Score for every app in [onChainAppIds] that has an `xapp_metadata`
     * document (VBD Reputation Layer Proposal, Profile Score).
     *
     * `ProfileScore = StaticScore × (0.8 + 0.2 × RecencyFactor)`, where:
     * - `StaticScore = optionalFieldsCount / 8 × 100` (completeness of the 8 optional metadata
     *   fields, from the latest snapshot).
     * - `RecencyFactor = (ProductRecency + SocialRecency) / 2`.
     * - `ProductRecency` / `SocialRecency ∈ {0, 0.5, 1.0}` from the time since the most recent
     *   snapshot whose `versionHistoryUpdated` / `tweetsUpdated` flag is `1` (≤14d → 1.0, 14–30d →
     *   0.5, >30d → 0).
     *
     * The update flags are precomputed at write time (see `AppMetadataService`). To find when each
     * flag last fired we scan the current snapshot plus the inline `_previousVersions` history.
     * That array is normally stripped from reads by `FilteringMongoTemplate`, so the raw `Document`
     * form is read directly (the version-exclusion projection only applies to `VersionedDocument`
     * entity types, not to `Document`). Apps without a metadata document — or whose flags never
     * fired — get the corresponding recency `0` (but keep their `StaticScore`). Round-independent
     * snapshot; returned keyed by lower-cased `appId`. "now" is wall-clock time.
     */
    private fun computeProfileScores(onChainAppIds: List<String>): Map<String, ProfileScoreData> {
        if (onChainAppIds.isEmpty()) return emptyMap()
        val collection = IndexerNames.XAPP_METADATA.COLLECTION
        if (!mongoTemplate.collectionExists(collection)) return emptyMap()

        val docs =
            mongoTemplate.find(
                Query(Criteria.where("_id").`in`(onChainAppIds)),
                Document::class.java,
                collection,
            )

        val now = System.currentTimeMillis()
        val result = mutableMapOf<String, ProfileScoreData>()
        for (doc in docs) {
            val appId = (doc["_id"] as? String)?.lowercase() ?: continue
            val snapshots = extractMetadataSnapshots(doc)
            if (snapshots.isEmpty()) continue

            val current = snapshots.first()
            val staticScore = computeStaticScore(current.optionalFieldsCount)
            val productRecency = computeRecency(snapshots, now) { it.versionHistoryUpdated == 1 }
            val socialRecency = computeRecency(snapshots, now) { it.tweetsUpdated == 1 }
            val recencyFactor = productRecency.add(socialRecency).divide(TWO, COMMUNITY_MC)
            val profileScore =
                staticScore.multiply(
                    PROFILE_BASE_WEIGHT.add(
                        PROFILE_RECENCY_WEIGHT.multiply(recencyFactor, COMMUNITY_MC),
                        COMMUNITY_MC,
                    ),
                    COMMUNITY_MC,
                )
            result[appId] =
                ProfileScoreData(
                    optionalFieldsCount = current.optionalFieldsCount,
                    staticScore = staticScore,
                    productRecency = productRecency,
                    socialRecency = socialRecency,
                    recencyFactor = recencyFactor,
                    profileScore = profileScore,
                )
        }
        return result
    }

    /**
     * Extracts the current snapshot plus each inline `_previousVersions` entry, newest first (the
     * array is prepended on every update). Entries lacking a `blockTimestamp` are skipped.
     */
    private fun extractMetadataSnapshots(doc: Document): List<AppMetadataSnapshot> {
        val snapshots = mutableListOf<AppMetadataSnapshot>()
        snapshotOf(doc)?.let(snapshots::add)
        val previous = doc.get(PREVIOUS_VERSIONS_FIELD) as? List<*>
        previous?.forEach { entry -> if (entry is Document) snapshotOf(entry)?.let(snapshots::add) }
        return snapshots
    }

    private fun snapshotOf(doc: Document): AppMetadataSnapshot? {
        val blockTimestamp = (doc.get("blockTimestamp") as? Number)?.toLong() ?: return null
        return AppMetadataSnapshot(
            blockTimestamp = blockTimestamp,
            versionHistoryUpdated = (doc.get("versionHistoryUpdated") as? Number)?.toInt() ?: 0,
            tweetsUpdated = (doc.get("tweetsUpdated") as? Number)?.toInt() ?: 0,
            optionalFieldsCount = (doc.get("optionalFieldsCount") as? Number)?.toInt() ?: 0,
        )
    }

    /** StaticScore = optionalFieldsCount / M_total × 100, where M_total = 8 optional fields. */
    private fun computeStaticScore(optionalFieldsCount: Int): BigDecimal =
        if (optionalFieldsCount <= 0) {
            BigDecimal.ZERO
        } else {
            BigDecimal(optionalFieldsCount)
                .multiply(HUNDRED)
                .divide(OPTIONAL_FIELDS_TOTAL, COMMUNITY_MC)
        }

    /**
     * Recency component ∈ {0, 0.5, 1.0} for the content type selected by [updatedIn]. Finds the
     * most recent snapshot (newest first) whose flag fired and maps the elapsed time to the
     * proposal thresholds (≤14d → 1.0, 14–30d → 0.5, >30d → 0). Never updated → 0.
     */
    private fun computeRecency(
        snapshots: List<AppMetadataSnapshot>,
        now: Long,
        updatedIn: (AppMetadataSnapshot) -> Boolean,
    ): BigDecimal {
        val lastUpdateSec =
            snapshots.firstOrNull(updatedIn)?.blockTimestamp ?: return BigDecimal.ZERO
        val elapsedMs = now - lastUpdateSec * MILLIS_PER_SECOND
        return when {
            elapsedMs <= RECENCY_DAYS_FULL * MILLIS_PER_DAY -> BigDecimal.ONE
            elapsedMs <= RECENCY_DAYS_PARTIAL * MILLIS_PER_DAY -> HALF
            else -> BigDecimal.ZERO
        }
    }
}

private fun toLong(value: Any?): Long =
    when (value) {
        is Long -> value
        is Int -> value.toLong()
        is Number -> value.toLong()
        else -> 0L
    }

private fun toBigDecimal(value: Any?): BigDecimal =
    when (value) {
        is BigDecimal -> value
        // Decimal128 (MongoDB driver) and other Number subtypes carry the full-precision value in
        // toString(), so round-tripping through the string avoids Double precision loss.
        is Number -> BigDecimal(value.toString())
        // Score fields in b3tr_app_round_scores are persisted as strings (e.g. "73.8857") because
        // scoreRound writes them via raw Update without a DECIMAL128 target type. Parse them so the
        // badge ranking sees the real values — otherwise every app reads as 0 and ties for top 10%.
        is String -> value.toBigDecimalOrNull() ?: BigDecimal.ZERO
        else -> BigDecimal.ZERO
    }
