package org.vechain.indexer.xapp.badge

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonValue
import io.swagger.v3.oas.annotations.media.Schema
import org.springframework.boot.context.properties.bind.ConstructorBinding
import org.springframework.data.annotation.Id
import org.springframework.data.mongodb.core.mapping.Document
import org.vechain.indexer.IndexerNames
import org.vechain.indexer.VersionedDocument

enum class BadgeType(@JsonValue val value: String) {
    TOP_ECOSYSTEM_DAPP("topEcosystemDapp"),
    TOP_DISTRIBUTION_PERFORMER("topDistributionPerformer"),
    NAVIGATORS_PICK("navigatorsPick");

    companion object {
        @JsonCreator
        @JvmStatic
        fun fromValue(value: String): BadgeType = entries.first { it.value == value }
    }
}

@Document(collection = IndexerNames.XAPP_BADGE_AWARD.COLLECTION)
data class BadgeAward
@ConstructorBinding
constructor(
    @Id val id: String,
    override val version: Int,
    override val blockId: String,
    override val blockNumber: Long,
    override val blockTimestamp: Long,
    val appId: String,
    val badgeType: BadgeType,
    val round: Long,
    val rank: Int,
    val isPublished: Boolean = true,
    val roundBlockTimestamp: Long? = null,
) : VersionedDocument {
    constructor(
        version: Int,
        blockId: String,
        blockNumber: Long,
        blockTimestamp: Long,
        appId: String,
        badgeType: BadgeType,
        round: Long,
        rank: Int,
        isPublished: Boolean = true,
        roundBlockTimestamp: Long? = null,
    ) : this(
        id = "${appId}_${badgeType.value}_$round",
        version = version,
        blockId = blockId,
        blockNumber = blockNumber,
        blockTimestamp = blockTimestamp,
        appId = appId,
        badgeType = badgeType,
        round = round,
        rank = rank,
        isPublished = isPublished,
        roundBlockTimestamp = roundBlockTimestamp,
    )

    override fun getDocumentId(): String = id
}

@Schema(description = "Summary of a badge type for an app")
data class BadgeSummary(
    @Schema(description = "Badge type", example = "topEcosystemDapp") val badgeType: BadgeType,
    @Schema(description = "Display name of the badge", example = "Top Ecosystem DApp")
    val badgeName: String,
    @Schema(description = "Whether the badge is published", example = "true")
    val isPublished: Boolean,
    @Schema(description = "Total Badges Earned", example = "5") val totalBadgesEarned: Int,
    @Schema(description = "Latest Badges", example = "12") val latestBadges: Long?,
    @Schema(
        description = "Whether the app has earned this badge: 1 = earned, 0 = not earned",
        example = "1",
    )
    val earned: Int,
)

@Schema(description = "Detailed information for a specific badge type")
data class BadgeDetail(
    @Schema(description = "Badge type", example = "topEcosystemDapp") val badgeType: BadgeType,
    @Schema(description = "Display name of the badge", example = "Top Ecosystem DApp")
    val badgeName: String,
    @Schema(description = "Whether the badge is published", example = "true")
    val isPublished: Boolean,
    @Schema(description = "Description of the badge", example = "Awarded to top ecosystem dApps")
    val description: String,
    @Schema(description = "Total Badges Earned", example = "5") val totalBadgesEarned: Int,
    @Schema(description = "Latest Badges", example = "12") val latestBadges: Long?,
    @Schema(description = "List of award records") val records: List<BadgeRecord>,
)

@Schema(description = "A single badge award record")
data class BadgeRecord(
    @Schema(description = "Round number", example = "12") val round: Long,
    @Schema(description = "Rank in this round", example = "1") val rank: Int,
    @Schema(description = "Timestamp of the round (Unix seconds)", example = "1779424224")
    val date: Long,
)

@Schema(description = "Badge ranking info for a single round")
data class BadgeRoundInfo(
    @Schema(description = "Round number", example = "12") val round: Long,
    @Schema(description = "Ranking", example = "1") val rank: Int?,
    @Schema(description = "Date", example = "1779424224") val date: Long,
)

@Schema(
    description =
        "Badge statistics response including total count, latest round, and per-round rankings"
)
data class BadgeStatsResponse(
    @Schema(description = "Badge type", example = "topEcosystemDapp") val badgeType: BadgeType,
    @Schema(description = "Total Badges Earned", example = "5") val totalBadgesEarned: Int,
    @Schema(description = "Latest Badges", example = "12") val latestBadges: Long?,
    @Schema(description = "Acquisition Records") val rounds: List<BadgeRoundInfo>,
)

@Schema(description = "Badge awards for an app in the previous round")
data class AppPreviousRoundBadgeResponse(
    @Schema(description = "The app ID") val appId: String,
    @Schema(description = "Previous round number") val round: Int,
    @Schema(description = "Previous round timestamp (Unix seconds)") val roundTimestamp: Long?,
    @Schema(description = "List of badges earned in this round") val badges: List<BadgeAwardItem>,
)

@Schema(description = "A single badge award item with rank")
data class BadgeAwardItem(
    @Schema(description = "Badge type", example = "topEcosystemDapp") val badgeType: BadgeType,
    @Schema(description = "Badge display name", example = "Top Ecosystem DApp")
    val badgeName: String,
    @Schema(description = "Rank in this round", example = "3") val rank: Int?,
    @Schema(
        description = "Whether the app earned this badge: 1 = earned, 0 = not earned",
        example = "1",
    )
    val earned: Int,
)

@Schema(description = "Earned badges for an app, grouped by badge type")
data class AppEarnedBadgesResponse(
    @Schema(description = "The app ID") val appId: String,
    @Schema(description = "List of earned badge summaries") val badges: List<BadgeSummary>,
)

@Schema(description = "Badge rank info for a specific round with summary stats")
data class BadgeRoundRankResponse(
    @Schema(description = "The app ID") val appId: String,
    @Schema(description = "Badge type", example = "topEcosystemDapp") val badgeType: BadgeType,
    @Schema(description = "The queried round number", example = "5") val round: Long,
    @Schema(description = "Rank in this round, null if not awarded", example = "3") val rank: Int?,
    @Schema(description = "Total Badges Earned", example = "12") val totalBadgesEarned: Long,
    @Schema(description = "Latest Badges", example = "15") val latestBadges: Long?,
)
