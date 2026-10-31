package com.vineyardfinance.sensors.wizard

/**
 * The in-app onboarding wizard, one step per screen, per
 * docs/onboarding_wizard_proposal_20260808.md — the productized version of
 * the hand-holding the first beta tester needed. Text descriptions only in
 * v1 (no photos exist yet, operator decision). Re-enterable from the main
 * screen. Implementation order note: the proposal lists the heartbeat gate
 * before battery setup, but collection legally cannot start until the
 * battery/background prerequisites pass (CollectorAdmission), so the
 * implemented order runs battery setup first and keeps the live heartbeat
 * as the everything-works confirmation gate.
 */
enum class WizardStep(
    val title: String,
    val body: String,
) {
    NO_OTHER_APP(
        "No other app needed",
        "You don't need the Polar app — this app does everything.\n\n" +
            "Don't pair the sensor in your phone's Bluetooth settings either; " +
            "the app finds it by itself.",
    ),
    WET_THE_PADS(
        "Wet the pads",
        "Wet both electrode pads — the two soft rubbery strips on the inside " +
            "of the strap — with tap water.\n\nThis is only needed when you put " +
            "it on; after that your skin's moisture takes over.",
    ),
    PUT_IT_ON(
        "Put it on",
        "Wear the strap just below the chest muscles, lower than you might " +
            "expect — snug but comfortable.\n\nCenter the sensor on your chest " +
            "with the POLAR text readable by someone facing you.",
    ),
    FIND_SENSOR(
        "Find your sensor",
        "Your sensor only wakes up when it's on your chest with wet pads — " +
            "it should be awake now.\n\nPick your sensor from the list below, " +
            "or type the ID printed on the sensor if it doesn't appear.",
    ),
    KEEP_ALIVE(
        "Keep the app alive",
        "Android likes to stop background apps, which would stop your " +
            "recording. The buttons below open the right settings screens — " +
            "complete each one.\n\nIf you use Data Saver, also allow " +
            "unrestricted data so your recordings upload overnight.",
    ),
    SEE_HEARTBEAT(
        "See your heartbeat",
        "Starting your recording…\n\nThis screen confirms everything works " +
            "end to end before you finish.",
    ),
    RANGE_RULES(
        "What to expect",
        "Keep your phone within about 10 metres of the sensor.\n\n" +
            "No internet is fine — the app stores your data and catches up " +
            "later, for days if needed.\n\nOut of Bluetooth range is not fine — " +
            "minutes away from your phone are lost.",
    ),
    DONE(
        "You're set",
        "Your heart data is recording and uploading.\n\n" +
            "Something confusing? Tell us — we want to hear it.",
    ),
    ;

    companion object {
        fun ordered(): List<WizardStep> = entries.toList()
    }
}

/**
 * Pure navigation state for the wizard: linear, Back always available,
 * Next gated per step by the live facts supplied by the host screen.
 */
data class WizardState(
    val stepIndex: Int = 0,
) {
    val step: WizardStep get() = WizardStep.ordered()[stepIndex]
    val isFirst: Boolean get() = stepIndex == 0
    val isLast: Boolean get() = stepIndex == WizardStep.ordered().lastIndex
    val progressLabel: String get() = "Step ${stepIndex + 1} of ${WizardStep.ordered().size}"

    fun back(): WizardState = if (isFirst) this else copy(stepIndex = stepIndex - 1)

    fun next(gate: WizardGate): WizardState = when {
        isLast -> this
        !gate.allowsLeaving(step) -> this
        else -> copy(stepIndex = stepIndex + 1)
    }
}

/** Per-step advancement facts, computed by the host from live app state. */
data class WizardGate(
    val sensorTargetValid: Boolean,
    val batterySetupComplete: Boolean,
    val heartbeatConfirmed: Boolean,
) {
    fun allowsLeaving(step: WizardStep): Boolean = when (step) {
        WizardStep.FIND_SENSOR -> sensorTargetValid
        WizardStep.KEEP_ALIVE -> batterySetupComplete
        WizardStep.SEE_HEARTBEAT -> heartbeatConfirmed
        else -> true
    }

    /** The honest blocking explanation shown beside a disabled Next. */
    fun blockingDetail(step: WizardStep): String? = when {
        step == WizardStep.FIND_SENSOR && !sensorTargetValid ->
            "Select your sensor from the list, or type the ID printed on it"
        step == WizardStep.KEEP_ALIVE && !batterySetupComplete ->
            "Complete the battery steps above so recording survives in your pocket"
        step == WizardStep.SEE_HEARTBEAT && !heartbeatConfirmed ->
            "Waiting for your heartbeat and the first upload confirmation"
        else -> null
    }
}
