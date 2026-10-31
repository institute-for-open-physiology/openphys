# Native mobile parity

The Android app follows the approved iOS design in native Kotlin and Compose.
The iOS app stays in SwiftUI.

- Use the same feature names: Now, History, Device, Sync.
- Keep design values, screen contracts, and replay fixtures in `shared/`.
- Generate platform values from those files. CI rejects stale output and missing features.
- Test both implementations with the same inputs and expected results.
- Compare rendered screens in light and dark mode.
- Preserve Android recordings, accounts, permissions, and background collection.

Platform APIs and internal classes may differ. Kotlin Multiplatform is outside this change.
