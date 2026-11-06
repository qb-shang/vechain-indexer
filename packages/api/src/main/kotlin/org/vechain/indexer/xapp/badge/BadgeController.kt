package org.vechain.indexer.xapp.badge

import io.swagger.v3.oas.annotations.Operation
import io.swagger.v3.oas.annotations.Parameter
import io.swagger.v3.oas.annotations.tags.Tag
import org.springframework.context.annotation.Profile
import org.springframework.validation.annotation.Validated
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import org.vechain.indexer.constants.XAPP_PATH
import org.vechain.indexer.docs.CommonApiResponses
import org.vechain.indexer.docs.PaginationParameters
import org.vechain.indexer.rest.PaginatedResponse
import org.vechain.indexer.utils.PaginationUtils.toPageable
import org.vechain.indexer.validation.ValidPageSize

@Profile("b3tr", "xapp-badge")
@Validated
@RestController
@RequestMapping(XAPP_PATH)
@Tag(name = "XApp - Badges", description = "Query XApp badge awards.")
open class BadgeController(private val service: BadgeApiService) {

    @GetMapping("/apps/{appId}/badges")
    @Operation(summary = "Get badge summary for an app. Returns all three badge types with stats.")
    @CommonApiResponses
    open fun getAppBadges(@PathVariable appId: String): List<BadgeSummary> =
        service.getAppBadges(appId)

    @GetMapping("/apps/{appId}/badges/{badgeType}")
    @Operation(summary = "Get badge detail for a specific badge type.")
    @CommonApiResponses
    open fun getBadgeDetail(
        @PathVariable appId: String,
        @PathVariable badgeType: BadgeType,
    ): BadgeDetail = service.getBadgeDetail(appId, badgeType)

    @GetMapping("/apps/{appId}/badges/{badgeType}/records")
    @Operation(summary = "Get paginated badge award records for a specific badge type.")
    @CommonApiResponses
    @PaginationParameters
    open fun getBadgeRecords(
        @PathVariable appId: String,
        @PathVariable badgeType: BadgeType,
        @RequestParam(required = false) page: Int?,
        @ValidPageSize @RequestParam(required = false) size: Int?,
        @RequestParam(required = false) direction: String?,
    ): PaginatedResponse<BadgeRecord> =
        service.getBadgeRecords(
            appId = appId,
            badgeType = badgeType,
            pageable = toPageable(page, size, direction ?: "desc", "round"),
        )

    @GetMapping("/apps/{appId}/badges/stats")
    @Operation(summary = "Get badge stats: total count, latest round, and all round rankings.")
    @CommonApiResponses
    open fun getBadgeStats(
        @Parameter(description = "The unique identifier of the app.", required = true)
        @PathVariable
        appId: String,
        @Parameter(
            description =
                "The type of badge to query. Allowed values: topEcosystemDapp, topDistributionPerformer, navigatorsPick.",
            required = true,
        )
        @RequestParam
        badgeType: String,
    ): BadgeStatsResponse = service.getBadgeStats(appId, badgeType)

    @GetMapping("/apps/{appId}/badges/public")
    @Operation(
        summary =
            "Get public badges for app detail display. " +
                "Returns badges from the latest round that are published."
    )
    @CommonApiResponses
    open fun getPublicBadges(@PathVariable appId: String): List<BadgeAward> =
        service.getPublicBadges(appId)

    @GetMapping("/apps/{appId}/badges/earned")
    @Operation(
        summary =
            "Get all earned badges for an app, grouped by badge type with count and latest round."
    )
    @CommonApiResponses
    open fun getEarnedBadges(
        @Parameter(description = "The unique identifier of the app.", required = true)
        @PathVariable
        appId: String
    ): AppEarnedBadgesResponse = service.getEarnedBadges(appId)

    @GetMapping("/apps/{appId}/badges/previousRound")
    @Operation(summary = "Get the badges and rankings for an app in the previous round.")
    @CommonApiResponses
    open fun getPreviousRoundBadges(
        @Parameter(description = "The unique identifier of the app.", required = true)
        @PathVariable
        appId: String
    ): AppPreviousRoundBadgeResponse = service.getPreviousRoundBadges(appId)

    @GetMapping("/apps/{appId}/badges/roundRank")
    @Operation(
        summary = "Get badge rank for the previous round, along with total count and latest round."
    )
    @CommonApiResponses
    open fun getBadgeRoundRank(
        @Parameter(description = "The unique identifier of the app.", required = true)
        @PathVariable
        appId: String,
        @Parameter(
            description =
                "The type of badge to query. Allowed values: topEcosystemDapp, topDistributionPerformer, navigatorsPick.",
            required = true,
        )
        @RequestParam
        badgeType: String,
    ): BadgeRoundRankResponse = service.getBadgeRoundRank(appId, badgeType)
}
