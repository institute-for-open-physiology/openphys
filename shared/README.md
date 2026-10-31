# Shared mobile design

SwiftUI and Compose use the same design values and test data.

| Edit | Controls |
| --- | --- |
| [`design/tokens.json`](design/tokens.json) | Colors, spacing, type sizes |
| [`screens/`](screens/) | Screen names, sections, help text |
| [`fixtures/`](fixtures/) | Sensor tests and screen previews |

## Update

After editing shared values, regenerate from the repository root:

```sh
uv run --no-project python scripts/mobile_contract.py
```

CI rejects stale generated files, missing screens or sections, mismatched sensor results, and changed screenshots.
Screen files use `<Name>View.swift` and `<Name>View.kt`.

<details>
<summary>Update screenshots</summary>

Screenshots use synthetic readings. Review image diffs before replacing baselines.

**Android**, from the repository root:

```sh
cd android
./gradlew testDebugUnitTest -Pmobile.recordScreenshots=true
```

**iOS:** run `SnapshotTests` on iPhone 16 Pro with iOS 26.2. Copy reviewed PNG attachments into `ios/TrainingSensorsUITests/Snapshots/`.

</details>

Internal classes and platform setup can differ. Android keeps server enrollment and foreground collection; HealthKit and Watch are Apple features.
