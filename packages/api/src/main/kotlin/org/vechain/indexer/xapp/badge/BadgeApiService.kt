package org.vechain.indexer.xapp.badge

import org.bson.Document
import org.springframework.context.annotation.Profile
import org.springframework.data.domain.Pageable
import org.springframework.data.domain.Slice
import org.springframework.data.domain.SliceImpl
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.stereotype.Service
import org.vechain.indexer.IndexerNames
import org.vechain.indexer.IndexerService
import org.vechain.indexer.b3tr.round.repository.CycleInfoRepository
import org.vechain.indexer.rest.PaginatedResponse
import org.vechain.indexer.rest.paginatedResponse
import org.vechain.indexer.xapp.badge.repository.BadgeAwardRepository

@Profile("b3tr", "xapp-badge")
@Service
open class BadgeApiService(
    private val repository: BadgeAwardRepository,
    private val cycleInfoRepository: CycleInfoRepository,
    private val mongoTemplate: MongoTemplate,
) : IndexerService {

    companion object {
        private val BADGE_NAMES =
            mapOf(
                BadgeType.TOP_ECOSYSTEM_DAPP to "Top Ecosystem dApp",
                BadgeType.TOP_DISTRIBUTION_PERFORMER to "Top Distribution Performer",
                BadgeType.NAVIGATORS_PICK to "Navigators' Pick",
            )

        private val BADGE_DESCRIPTIONS =
            mapOf(
                BadgeType.TOP_ECOSYSTEM_DAPP to
                    "Awarded to apps ranked in the top 10% based on scoring results each round.",
                BadgeType.TOP_DISTRIBUTION_PERFORMER to
                    "Awarded to apps ranked in the top 10% based on distribution efficiency each round.",
                BadgeType.NAVIGATORS_PICK to
                    "Awarded to the top 3 apps supported by Navigators each round.",
            )
    }

    open fun getAppBadges(appId: String): List<BadgeSummary> {
        val allAwards = repository.findAllByAppId(appId)
        val privateByType = loadBadgeSettings(appId)
        return BadgeType.entries.map { type ->
            val typeAwards = allAwards.filter { it.badgeType == type }
            BadgeSummary(
                badgeType = type,
                badgeName = BADGE_NAMES[type] ?: type.name,
                isPublished = privateByType[type.value] != true,
                totalBadgesEarned = typeAwards.size,
                latestBadges = typeAwards.maxByOrNull { it.round }?.round,
                earned = if (typeAwards.isEmpty()) 0 else 1,
            )
        }
    }

    open fun getBadgeDetail(appId: String, badgeType: BadgeType): BadgeDetail {
        val awards = repository.findAllByAppIdAndBadgeType(appId, badgeType)
        return BadgeDetail(
            badgeType = badgeType,
            badgeName = BADGE_NAMES[badgeType] ?: badgeType.name,
            isPublished = loadBadgeSettings(appId)[badgeType.value] != true,
            description = BADGE_DESCRIPTIONS[badgeType] ?: "",
            totalBadgesEarned = awards.size,
            latestBadges = awards.maxByOrNull { it.round }?.round,
            records =
                awards
                    .sortedByDescending { it.round }
                    .map { BadgeRecord(round = it.round, rank = it.rank, date = it.blockTimestamp) },
        )
    }

    open fun getBadgeRecords(
        appId: String,
        badgeType: BadgeType,
        pageable: Pageable,
    ): PaginatedResponse<BadgeRecord> {
        val results =
            repository.findAllByAppIdAndBadgeTypeOrderByRoundDesc(appId, badgeType, pageable)
        val records =
            results.map { BadgeRecord(round = it.round, rank = it.rank, date = it.blockTimestamp) }
        val slice: Slice<BadgeRecord> = SliceImpl(records.content, pageable, results.hasNext())
        return paginatedResponse(slice)
    }

    open fun getBadgeStats(appId: String, badgeType: String): BadgeStatsResponse {
        val type = BadgeType.fromValue(badgeType)
        val awards = repository.findAllByAppIdAndBadgeType(appId, type)
        val rankByRound = awards.associateBy { it.round }
        // Exclude the latest round — it is still in progress and its results are not finalized yet.
        val allCycles = cycleInfoRepository.findAll().sortedByDescending { it.roundId }.drop(1)
        return BadgeStatsResponse(
            badgeType = type,
            totalBadgesEarned = awards.size,
            latestBadges = awards.maxByOrNull { it.round }?.round,
            rounds =
                allCycles.map { cycle ->
                    BadgeRoundInfo(
                        round = cycle.roundId.toLong(),
                        rank = rankByRound[cycle.roundId.toLong()]?.rank,
                        date = cycle.blockTimestamp,
                    )
                },
        )
    }

    open fun getPublicBadges(appId: String): List<BadgeAward> {
        val privateByType = loadBadgeSettings(appId)
        val allPublic =
            repository.findAllPublicByAppId(appId).filter {
                privateByType[it.badgeType.value] != true
            }
        if (allPublic.isEmpty()) return emptyList()
        val latestRound = allPublic.maxOf { it.round }
        return allPublic.filter { it.round == latestRound }
    }

    open fun getPreviousRoundBadges(appId: String): AppPreviousRoundBadgeResponse {
        val currentCycle =
            cycleInfoRepository.findTopByOrderByRoundIdDesc()
                ?: throw NoSuchElementException("No cycle info found")
        val previousRound = currentCycle.roundId - 1
        if (previousRound < 1) {
            return AppPreviousRoundBadgeResponse(
                appId = appId,
                round = previousRound,
                roundTimestamp = null,
                badges =
                    BadgeType.entries.map { type ->
                        BadgeAwardItem(
                            badgeType = type,
                            badgeName = BADGE_NAMES[type] ?: type.name,
                            rank = null,
                            earned = 0,
                        )
                    },
            )
        }
        val previousCycle = cycleInfoRepository.findByRoundId(previousRound)
        val awards = repository.findAllByAppId(appId).filter { it.round == previousRound.toLong() }
        val earnedByType = awards.associateBy { it.badgeType }
        // Hide badges the app has marked private in its xapp_metadata badgeSettings: such a badge
        // is
        // reported as not earned (earned = 0) with no rank, regardless of the actual award.
        val privateByType = loadBadgeSettings(appId)
        val badgeItems =
            BadgeType.entries.map { type ->
                val award = earnedByType[type]
                val isPrivate = privateByType[type.value] == true
                BadgeAwardItem(
                    badgeType = type,
                    badgeName = BADGE_NAMES[type] ?: type.name,
                    rank = if (isPrivate) null else award?.rank,
                    earned = if (isPrivate) 0 else if (award != null) 1 else 0,
                )
            }
        return AppPreviousRoundBadgeResponse(
            appId = appId,
            round = previousRound,
            roundTimestamp = previousCycle?.blockTimestamp,
            badges = badgeItems,
        )
    }

    /**
     * Reads the app's `badgeSettings` from [xapp_metadata][IndexerNames.XAPP_METADATA], returning a
     * badge-type-value → `isPrivate` map for the three badge types. Returns an empty map when the
     * app has no metadata document or no `badgeSettings`; a missing `isPrivate` defaults to
     * `false`. The `_id` lookup uses the lower-cased appId, matching how `AppMetadataService`
     * persists it.
     */
    private fun loadBadgeSettings(appId: String): Map<String, Boolean> {
        val doc =
            mongoTemplate.findOne(
                Query(Criteria.where("_id").`is`(appId.lowercase())),
                Document::class.java,
                IndexerNames.XAPP_METADATA.COLLECTION,
            ) ?: return emptyMap()
        val settings = doc.get("badgeSettings", Document::class.java) ?: return emptyMap()
        return BadgeType.entries.associate { type ->
            val entry = settings.get(type.value, Document::class.java)
            type.value to (entry?.getBoolean("isPrivate") ?: false)
        }
    }

    open fun getEarnedBadges(appId: String): AppEarnedBadgesResponse {
        val allAwards = repository.findAllByAppId(appId)
        val privateByType = loadBadgeSettings(appId)
        val badges =
            BadgeType.entries.map { type ->
                val typeAwards = allAwards.filter { it.badgeType == type }
                BadgeSummary(
                    badgeType = type,
                    badgeName = BADGE_NAMES[type] ?: type.name,
                    isPublished = privateByType[type.value] != true,
                    totalBadgesEarned = typeAwards.size,
                    latestBadges = typeAwards.maxByOrNull { it.round }?.round,
                    earned = if (typeAwards.isEmpty()) 0 else 1,
                )
            }
        return AppEarnedBadgesResponse(appId = appId, badges = badges)
    }

    open fun getBadgeRoundRank(appId: String, badgeType: String): BadgeRoundRankResponse {
        val currentCycle =
            cycleInfoRepository.findTopByOrderByRoundIdDesc()
                ?: throw NoSuchElementException("No cycle info found")
        val previousRound = currentCycle.roundId - 1
        val round = if (previousRound < 1) 0L else previousRound.toLong()
        val type = BadgeType.fromValue(badgeType)
        val totalBadgesEarned = repository.countByAppIdAndBadgeType(appId, type)
        val latestAward = repository.findTopByAppIdAndBadgeTypeOrderByRoundDesc(appId, type)
        val targetAwards = repository.findAllByAppIdAndBadgeType(appId, type)
        val rank = if (round > 0) targetAwards.find { it.round == round }?.rank else null
        return BadgeRoundRankResponse(
            appId = appId,
            badgeType = type,
            round = round,
            rank = rank,
            totalBadgesEarned = totalBadgesEarned,
            latestBadges = latestAward?.round,
        )
    }

    override fun getLatestIndexedBlocks(): Map<String, Long> = mapOf()
}
