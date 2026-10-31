package com.vineyardfinance.sensors.ui

import androidx.activity.ComponentActivity
import androidx.compose.foundation.layout.*
import androidx.compose.runtime.*
import androidx.compose.ui.test.*
import androidx.compose.ui.test.junit4.createAndroidComposeRule
import com.github.takahirom.roborazzi.RoborazziOptions
import com.github.takahirom.roborazzi.RoborazziTaskType
import com.github.takahirom.roborazzi.captureRoboImage
import com.google.gson.JsonParser
import com.vineyardfinance.sensors.collector.*
import com.vineyardfinance.sensors.data.*
import org.junit.*
import org.junit.runner.RunWith
import org.robolectric.RobolectricTestRunner
import org.robolectric.annotation.Config
import org.robolectric.annotation.GraphicsMode

@OptIn(com.github.takahirom.roborazzi.ExperimentalRoborazziApi::class)
@RunWith(RobolectricTestRunner::class)
@Config(application = android.app.Application::class, sdk = [34], qualifiers = "w393dp-h852dp-mdpi")
@GraphicsMode(GraphicsMode.Mode.NATIVE)
class MobileScreensTest {
    @get:Rule val compose = createAndroidComposeRule<ComponentActivity>()

    @Test
    fun sharedScreensRenderInBothThemes() {
        var screen by mutableStateOf(MobileScreen.Now)
        var dark by mutableStateOf(true)
        val state = fixture()
        val failures = mutableListOf<String>()
        compose.mainClock.autoAdvance = true
        compose.setContent {
            CompositionLocalProvider(LocalFrameTime provides state.time) {
                MobileTheme(dark) {
                    key(screen, dark) { MobileApp(state, null, initialScreen = screen) }
                }
            }
        }
        for (mode in listOf(true, false)) for (tab in MobileScreen.entries) {
            compose.runOnUiThread {
                dark = mode
                screen = tab
            }
            compose.mainClock.advanceTimeBy(500)
            compose.waitForIdle()
            compose.onAllNodesWithText(tab.title).onLast().assertExists()
            val name = "${tab.name}-${if(mode) "dark" else "light"}.png"
            try {
                compose
                    .onRoot()
                    .captureRoboImage(
                        "src/test/snapshots/$name",
                        roborazziOptions =
                            RoborazziOptions(
                                // Skia antialiasing differs slightly between Linux hosts.
                                compareOptions =
                                    RoborazziOptions.CompareOptions(
                                        imageComparator =
                                            com.dropbox.differ.SimpleImageComparator(
                                                maxDistance = 0.1F
                                            )
                                    ),
                                taskType =
                                    if (java.lang.Boolean.getBoolean("mobile.recordScreenshots"))
                                        RoborazziTaskType.Record
                                    else RoborazziTaskType.Verify,
                            ),
                    )
            } catch (error: AssertionError) {
                failures += error.message ?: name
            }
            val sections =
                JsonParser.parseReader(
                        javaClass.getResourceAsStream("/${tab.name}.json")!!.reader()
                    )
                    .asJsonObject["sections"]
                    .asJsonObject
            sections.entrySet().forEach { (_, section) ->
                compose
                    .onNodeWithTag("section-${section.asJsonObject["title"].asString}")
                    .performScrollTo()
                    .assertIsDisplayed()
            }
        }
        Assert.assertTrue(failures.joinToString("\n"), failures.isEmpty())
    }

    @Test
    fun eventAtChartEdgeOpensFromItsVisibleLabel() {
        var clicked: SensorEvent? = null
        val event = SensorEvent("edge", 600.0, "Walk")
        compose.setContent {
            MobileTheme {
                SignalPlot(
                    listOf(SignalPoint(600.0, 72.0)),
                    0.0,
                    600.0,
                    palette.heart,
                    androidx.compose.ui.Modifier.fillMaxWidth()
                        .height(androidx.compose.ui.unit.Dp(164f)),
                    events = listOf(event),
                    onEvent = { clicked = it },
                    showExtremes = true,
                )
            }
        }
        compose.onNodeWithContentDescription("Signal graph. 1 readings.").performTouchInput {
            click(androidx.compose.ui.geometry.Offset(centerRight.x - 48f, 12f))
        }
        Assert.assertEquals(event, clicked)
    }

    @Test
    fun infoDoesNotMoveTheChart() {
        compose.setContent {
            CompositionLocalProvider(LocalFrameTime provides fixture().time) {
                MobileTheme { MobileApp(fixture(), null) }
            }
        }
        compose.onNodeWithTag("section-ECG").assertExists()
        val original = compose.onNodeWithTag("section-ECG").fetchSemanticsNode().boundsInRoot
        compose.onAllNodesWithContentDescription("Info").onFirst().performClick()
        compose.onNodeWithText(NowContent.ecg.info!!).assertIsDisplayed()
        compose.onNodeWithText("OK").performClick()
        Assert.assertEquals(
            original,
            compose.onNodeWithTag("section-ECG").fetchSemanticsNode().boundsInRoot,
        )
    }

    private fun fixture(): MobileState {
        val data =
            JsonParser.parseReader(javaClass.getResourceAsStream("/dashboard.json")!!.reader())
                .asJsonObject
        val time = data["time"].asDouble
        val history =
            data["history"].asJsonArray.map { row ->
                val values = row.asJsonArray.map { it.asDouble }
                HistoryBin(values[0], values.drop(1))
            }
        val acc = data["acceleration"].asJsonArray.map { it.asDouble }
        return MobileState(
            time = time,
            status =
                CollectorSnapshot(
                    state = "Streaming",
                    detail = "ECG + motion + heart rate",
                    connected = true,
                    serviceRunning = true,
                    batteryPercent = data["battery"].asInt,
                    rssiDbm = data["rssi"].asInt,
                    backlogKnown = true,
                ),
            signals =
                SignalSnapshot(
                    heartRate =
                        data["heartRate"].asJsonArray.map {
                            SignalPoint(it.asJsonArray[0].asDouble, it.asJsonArray[1].asDouble)
                        },
                    bpm = data["bpm"].asInt,
                    contact = true,
                    rr = data["rr"].asDouble,
                    ecg =
                        data["ecg"].asJsonArray.map {
                            SignalPoint(it.asJsonArray[0].asDouble, it.asJsonArray[1].asDouble)
                        },
                    acceleration = acc,
                    gravity = acc,
                    current = history.last().values,
                    clockSynced = true,
                ),
            recent = history.filter { it.time >= time - 600 },
            events =
                data["events"].asJsonArray.map {
                    val e = it.asJsonObject
                    SensorEvent(e["id"].asString, e["time"].asDouble, e["label"].asString)
                },
            timeline = listOf(TimelineEvent(time - 1471, "Streaming", "ECG + motion + heart rate")),
            accountConnected = true,
        )
    }
}
