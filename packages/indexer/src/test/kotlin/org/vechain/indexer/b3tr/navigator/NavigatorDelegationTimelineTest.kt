package org.vechain.indexer.b3tr.navigator

import java.math.BigDecimal
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test

internal class NavigatorDelegationTimelineTest {

    private fun event(block: Long, citizen: String, navigator: String, type: String) =
        NavigatorDelegationEvent(
            id = "$block-$citizen-$type",
            blockId = "0x$block",
            blockNumber = block,
            blockTimestamp = block * 10,
            txId = "0xtx$block",
            navigator = navigator,
            citizen = citizen,
            eventType = type,
            amount = BigDecimal.ONE,
            delta = BigDecimal.ONE,
        )

    @Test
    fun `returns null before the first delegation`() {
        val timeline =
            NavigatorDelegationTimeline(
                listOf(event(100, "0xc1", "0xnavA", "B3TR_DelegationCreated"))
            )
        assertNull(timeline.navigatorAt("0xc1", 99))
        assertEquals("0xnava", timeline.navigatorAt("0xC1", 100))
    }

    @Test
    fun `follows a citizen switching navigators over time`() {
        val timeline =
            NavigatorDelegationTimeline(
                listOf(
                    event(300, "0xc1", "0xnavB", "B3TR_DelegationCreated"),
                    event(100, "0xc1", "0xnavA", "B3TR_DelegationCreated"),
                    event(200, "0xc1", "0xnavA", NavigatorDelegationTimeline.REMOVED),
                )
            )
        assertEquals("0xnava", timeline.navigatorAt("0xc1", 150))
        assertNull(timeline.navigatorAt("0xc1", 250))
        assertEquals("0xnavb", timeline.navigatorAt("0xc1", 300))
    }

    @Test
    fun `increase and decrease keep the delegation active`() {
        val timeline =
            NavigatorDelegationTimeline(
                listOf(
                    event(100, "0xc1", "0xnavA", "B3TR_DelegationCreated"),
                    event(110, "0xc1", "0xnavA", "B3TR_DelegationIncreased"),
                    event(120, "0xc1", "0xnavA", "B3TR_DelegationDecreased"),
                )
            )
        assertEquals("0xnava", timeline.navigatorAt("0xc1", 125))
    }

    @Test
    fun `remove and re-delegate in the same block resolves to the new navigator`() {
        val timeline =
            NavigatorDelegationTimeline(
                listOf(
                    event(100, "0xc1", "0xnavA", "B3TR_DelegationCreated"),
                    event(200, "0xc1", "0xnavB", "B3TR_DelegationCreated"),
                    event(200, "0xc1", "0xnavA", NavigatorDelegationTimeline.REMOVED),
                )
            )
        assertEquals("0xnavb", timeline.navigatorAt("0xc1", 200))
    }

    @Test
    fun `unknown citizen has no navigator`() {
        assertNull(NavigatorDelegationTimeline(emptyList()).navigatorAt("0xc9", 1_000))
    }
}
