import java.util.Properties

plugins {
    id("com.android.application")
    id("org.jetbrains.kotlin.android")
    id("org.jetbrains.kotlin.kapt")
    id("org.jetbrains.kotlinx.kover")
}

val signingProperties = Properties().apply {
    val file = rootProject.file("keystore.properties")
    if (file.isFile) file.inputStream().use(::load)
}
val signingStoreFile = providers.environmentVariable("TRAINING_SENSORS_KEYSTORE_PATH").orNull
    ?: signingProperties.getProperty("storeFile")
val signingStorePassword = providers.environmentVariable("TRAINING_SENSORS_KEYSTORE_PASSWORD").orNull
    ?: signingProperties.getProperty("storePassword")
val signingKeyAlias = providers.environmentVariable("TRAINING_SENSORS_KEY_ALIAS").orNull
    ?: signingProperties.getProperty("keyAlias")
val signingKeyPassword = providers.environmentVariable("TRAINING_SENSORS_KEY_PASSWORD").orNull
    ?: signingProperties.getProperty("keyPassword")
val hasReleaseSigning = listOf(signingStoreFile, signingStorePassword, signingKeyAlias, signingKeyPassword).all { !it.isNullOrBlank() }

android {
    namespace = "com.vineyardfinance.sensors"
    compileSdk = 34

    defaultConfig {
        applicationId = "com.vineyardfinance.sensors"
        minSdk = 28
        targetSdk = 34
        versionCode = 11
        versionName = "0.3.0"
        testInstrumentationRunner = "androidx.test.runner.AndroidJUnitRunner"
        buildConfigField("String", "DEFAULT_SERVER_URL", "\"https://sensors-poc.vineyard-finance.com\"")
        buildConfigField("boolean", "SECA_EXPERIMENTAL_LED_KEEPALIVE", providers.gradleProperty("secaExperimentalLedKeepalive").map { it.toBooleanStrict().toString() }.getOrElse("false"))
        buildConfigField("boolean", "SECA_EXPERIMENTAL_RECOVERY", providers.gradleProperty("secaExperimentalRecovery").map { it.toBooleanStrict().toString() }.getOrElse("true"))
        kapt { arguments { arg("room.schemaLocation", "$projectDir/schemas") } }
    }

    signingConfigs {
        if (hasReleaseSigning) {
            create("pocRelease") {
                storeFile = file(requireNotNull(signingStoreFile))
                storePassword = requireNotNull(signingStorePassword)
                keyAlias = requireNotNull(signingKeyAlias)
                keyPassword = requireNotNull(signingKeyPassword)
            }
        }
    }

    buildTypes {
        debug {
            applicationIdSuffix = ".debug"
            versionNameSuffix = "-debug"
        }
        release {
            isMinifyEnabled = false
            isShrinkResources = false
            proguardFiles(getDefaultProguardFile("proguard-android-optimize.txt"), "proguard-rules.pro")
            if (hasReleaseSigning) signingConfig = signingConfigs.getByName("pocRelease")
        }
    }

    buildFeatures {
        buildConfig = true
        viewBinding = true
        compose = true
    }
    composeOptions { kotlinCompilerExtensionVersion = "1.5.14" }
    sourceSets["test"].resources.srcDir("../../shared/fixtures")
    sourceSets["test"].resources.srcDir("../../shared/screens")
    sourceSets["test"].resources.srcDir("schemas")
    sourceSets["main"].assets.srcDir("../../shared/fixtures")
    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }
    kotlinOptions { jvmTarget = "17" }
    testOptions {
        unitTests.isIncludeAndroidResources = true
        unitTests.all {
            it.jvmArgs("-Duser.timezone=UTC")
            it.systemProperty("mobile.recordScreenshots", providers.gradleProperty("mobile.recordScreenshots").orElse("false").get())
            // Screenshot tests need ComponentActivity from the debug-only ui-test-manifest.
            if (it.name.contains("Release")) it.exclude("**/ui/MobileScreensTest*")
        }
    }
    packaging { resources.excludes += setOf("META-INF/AL2.0", "META-INF/LGPL2.1") }
}

dependencies {
    implementation(platform("androidx.compose:compose-bom:2024.06.00"))
    implementation("androidx.activity:activity-compose:1.9.1")
    implementation("androidx.compose.ui:ui")
    implementation("androidx.compose.foundation:foundation")
    implementation("androidx.compose.material3:material3")
    implementation("androidx.lifecycle:lifecycle-runtime-compose:2.8.4")
    debugImplementation("androidx.compose.ui:ui-tooling")
    testImplementation("androidx.compose.ui:ui-test-junit4")
    testImplementation("io.github.takahirom.roborazzi:roborazzi:1.26.0")
    testImplementation("io.github.takahirom.roborazzi:roborazzi-compose:1.26.0")
    debugImplementation("androidx.compose.ui:ui-test-manifest")
    implementation("androidx.core:core-ktx:1.13.1")
    implementation("androidx.appcompat:appcompat:1.7.0")
    implementation("androidx.activity:activity-ktx:1.9.1")
    implementation("androidx.lifecycle:lifecycle-runtime-ktx:2.8.4")
    implementation("androidx.lifecycle:lifecycle-service:2.8.4")
    implementation("androidx.work:work-runtime-ktx:2.9.1")
    implementation("androidx.room:room-runtime:2.6.1")
    implementation("androidx.room:room-ktx:2.6.1")
    kapt("androidx.room:room-compiler:2.6.1")
    implementation("com.squareup.okhttp3:okhttp:4.12.0")
    implementation("com.google.code.gson:gson:2.11.0")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-android:1.8.1")

    testImplementation("junit:junit:4.13.2")
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.8.1")
    testImplementation("androidx.room:room-testing:2.6.1")
    testImplementation("org.robolectric:robolectric:4.13")
    testImplementation("com.squareup.okhttp3:mockwebserver:4.12.0")
    testImplementation("androidx.test:core:1.6.1")
    androidTestImplementation("androidx.test.ext:junit:1.2.1")
    androidTestImplementation("androidx.test.espresso:espresso-core:3.6.1")
}

kover {
    reports {
        filters {
            excludes {
                classes(
                    "*.BuildConfig",
                    "*.R",
                    "*.R$*",
                    "*.*_Factory*",
                    "*.*_Impl*",
                    "*.*Database_Impl*",
                    // Host coverage measures deterministic project logic. These
                    // classes are Android framework, generated UI/Room,
                    // BLE, foreground-service, WorkManager,
                    // or hardware-keystore boundaries covered by lint/build,
                    // instrumentation and signed Galaxy/H10 validation.
                    "com.vineyardfinance.sensors.MainActivity*",
                    "com.vineyardfinance.sensors.auth.SignInActivity*",
                    "com.vineyardfinance.sensors.SetupActivity*",
                    // Native drawing and lifecycle adapters use UI, snapshot, and storage tests.
                    "com.vineyardfinance.sensors.ui.*",
                    "com.vineyardfinance.sensors.wizard.WizardActivity*",
                    "com.vineyardfinance.sensors.SensorsApplication*",
                    "com.vineyardfinance.sensors.AppGraph*",
                    "com.vineyardfinance.sensors.AppSettings*",
                    "com.vineyardfinance.sensors.databinding.*",
                    "com.vineyardfinance.sensors.auth.EncryptedDeviceCredentialStore${'$'}AndroidKeystoreCredentialCipher*",
                    "com.vineyardfinance.sensors.collector.BootReceiver*",
                    "com.vineyardfinance.sensors.collector.CollectorNotificationChannel*",
                    "com.vineyardfinance.sensors.collector.CollectorService*",
                    "com.vineyardfinance.sensors.collector.CollectorStatus*",
                    "com.vineyardfinance.sensors.collector.CollectorSnapshot*",
                    "com.vineyardfinance.sensors.collector.GattConnection*",
                    "com.vineyardfinance.sensors.collector.H10Collector*",
                    "com.vineyardfinance.sensors.collector.RawRecordWriter*",
                    // Location platform shell (LocationManager/SensorManager/GNSS
                    // callbacks); its consent gating, sampling policy, and record
                    // encoding live in the included pure location.* classes.
                    "com.vineyardfinance.sensors.location.LocationCollector*",
                    "com.vineyardfinance.sensors.data.AppDatabase*",
                    "com.vineyardfinance.sensors.data.RawRecord*",
                    "com.vineyardfinance.sensors.network.UploadWorker*",
                    "com.vineyardfinance.sensors.setup.AndroidSetupInspector*",
                    // seca Android UI/Bluetooth/foreground-service/WorkManager and
                    // directory-fsync boundaries: lint/build + signed Galaxy live tests.
                    // Protocol, queue/recovery, and uploader remain measured below.
                    "com.vineyardfinance.sensors.seca.SecaActivity*",
                    "com.vineyardfinance.sensors.seca.SecaService*",
                    "com.vineyardfinance.sensors.seca.SecaBootReceiver*",
                    "com.vineyardfinance.sensors.seca.SecaUploadWorker*",
                    "com.vineyardfinance.sensors.seca.SecaSettings*",
                    "com.vineyardfinance.sensors.seca.SecaStatus*",
                    "com.vineyardfinance.sensors.seca.SecaSnapshot*",
                    "com.vineyardfinance.sensors.seca.SecaRuntimeKt*",
                    "com.vineyardfinance.sensors.seca.SecaTiming*",
                )
            }
        }
        verify { rule { minBound(90) } }
    }
}
