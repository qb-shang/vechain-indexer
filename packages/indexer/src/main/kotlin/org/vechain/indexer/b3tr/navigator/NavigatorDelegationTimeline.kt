package org.vechain.indexer.b3tr.navigator

/**
 * Point-in-time view of citizen → navigator delegations, rebuilt from indexed delegation events.
 * Lets historical rounds be attributed to the navigator a citizen had when they voted, not the one
 * they have today.
 */
class NavigatorDelegationTimeline(events: List<NavigatorDelegationEvent>) {

    private val eventsByCitizen: Map<String, List<NavigatorDelegationEvent>> =
        events
            .groupBy { it.citizen.lowercase() }
            .mapValues { (_, citizenEvents) -> citizenEvents.sortedBy { it.blockNumber } }

    /**
     * Returns the navigator [citizen] was delegated to at [blockNumber] (inclusive), or null if the
     * citizen had no active delegation. When several events share the last block (e.g. a remove
     * followed by a new delegation in one transaction), an active delegation wins.
     */
    fun navigatorAt(citizen: String, blockNumber: Long): String? {
        val history = eventsByCitizen[citizen.lowercase()] ?: return null
        val lastBlock =
            history.lastOrNull { it.blockNumber <= blockNumber }?.blockNumber ?: return null
        val active =
            history.lastOrNull { it.blockNumber == lastBlock && it.eventType != REMOVED }
                ?: return null
        return active.navigator.lowercase()
    }

    companion object {
        const val REMOVED = "B3TR_DelegationRemoved"
    }
}
