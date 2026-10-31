# seca ct331 live protocol: evidence ledger

Updated 21 October 2026. This documents an independent implementation, not vendor
source. Read-only inspection of exported symbols/machine instructions supplied
hypotheses, then raw USB captures and an independent decoder checked them. The
Windows recording format remains separately documented in the sibling repository.

## Evidence and limits

Device: seca ct331, serial ct0XXXX (redacted), firmware 1.8.0.0, Bluetooth module firmware
6.0.1-001. The owner replaced a defective cable and restarted the device; USB
then enumerated as `/Volumes/ct 331`. The exact local manual is seca ct33x IFU
`5-10-20260115-GEB-0224-DK-2372-en-GB-002`, §2.3 and §2.7. It lists 1/2/4/8 kHz
and USB-only 16/32 kHz, and requires power cycling when changing transport.

On-body USB capture at configured and read-back-confirmed 4000 Hz / gain 6:
933 received packets, 1018 complete groups, 32,576 samples per channel (8.144 s),
zero checksum/decode failures or missing group counters. All eight channels vary;
compression widths 4/8/12 occur. Captures stay in ignored
`data/seca_ct331/20261021/`. The private independent golden fixture contains two
mixed-width groups with all expected samples, decoded with explicit byte/nibble
arithmetic, separately from the Rust generic bit reader. Never commit personal
recordings as test fixtures.

The installed `ecgdevicelib.dll` SHA-256 is
`b89eda773beac0060b3fb19ad34e0eb3687b835d2dc7db81bbc3fed6e8041e46`.
Useful function RVAs: sendInternalCommand `0x22df0`, decodePacket `0x1ec20`,
buildChannel `0x1d9b0`, addECGPacket `0x1c6c0`, getNanoVoltPerBit `0x20a60`.
No vendor code or binary is included in this repository.

## Transport

| Transport | Observed operation |
|---|---|
| USB | Tiny FAT12 volume, `ecg.txt` for identity/configuration/commands; `ecg.dat` for stream |
| USB writes | Frame zero-padded to 512 bytes, uncached write at offset zero of `ecg.txt`, then full fsync |
| USB reads | Uncached offset-zero reads consume device output. Initial 2048 bytes; continuation in 512-byte multiples up to 2048. Only one reader |
| USB idle | Valid 11-byte empty frame; existing bridge suppresses it |
| Bluetooth | Classic Bluetooth SPP/RFCOMM, not BLE. Discover standard SPP service via SDP |
| Commands | Send separately with pacing. 200 ms worked in current USB tests; transport write boundaries matter in historical bridge observations |

The owner's existing native Python/Swift bridge is operational reference code;
it forwards raw bytes to the Windows app under Wine. Its localhost TCP port4168
and UDP `CARDIO300SCAN` replies are emulation, not hardware network capability.
It does not validate samples/checksums. New collectors must retain corrupt data
with explicit errors rather than silently discarding bytes.

### Galaxy Bluetooth continuity observation

The Galaxy A54 paired with the ct331 through standard SPP and confirmed 4 kHz,
gain6, compressed1, openlead0 in standby before starting acquisition. With no
host writes after `mode=run`, approximately five seconds of normal delivery was
followed by roughly 480 ms socket-read delays and eventual loss of framing.
Timing instrumentation isolated the delay to socket reads; journal fsync stayed
in the low milliseconds. Complete frames before the failure had valid checksums.

The vendor client sends periodic LED-status commands every two seconds. The
independent Android collector instead sends the already verified empty read-only
`R` query after two seconds between completed writes (approximately **2.2 seconds**
including command spacing). This is a measured host-traffic workaround, not proof
of a particular Bluetooth controller power state. No speculative LED masks are
used and per-frame journal fsync remains enabled.

An 88-second A/B recording with this workaround retained 11,042 valid RX frames
and decoded 352,064 samples/channel. Four 32-sample groups were absent from the
counter sequence (128 samples/channel, 32 ms at 4 kHz). The catastrophic periodic
stall did not recur, but the recording was **not gap-free**. Preserve and display
these gaps; repeatability and other rates remain acceptance work. Detailed evidence
and cleanup ownership are in `seca_android_bluetooth_keepalive_20261021.md`.

Read-only follow-up found no established ct331 serial baud ceiling. The DLL's
230,400-baud literal belongs to `CSioThreadWorker::openSio` (RVA `0x63e80`,
literal at `0x63fb4`, `setBaudRate` call at `0x63fc3`) in the separate Guard
serial-radio path.
The ct331 Windows Bluetooth connection (`0x3cc60`) uses an RFCOMM socket;
its observed socket option at `0x3cd58` sets a 65,532-byte receive buffer, not a
baud rate. The IFU rate table does not specify transport bitrate. Neither this
literal nor a short compressed-data burst establishes a ct331 throughput limit.

The vendor's two-second LED timer is initialized near RVA `0x1bf17` and calls
`impedanceChangedSetLEDs` (`0x21890`). Its command is `W` with the 44-byte ASCII
payload `ledstatus=` plus two nine-bit banks and a sixteen-bit override (58 bytes
framed). Independent inspection of `calculateAndSetImpedance` (`0x1db40`) and
the timer confirms the default sixteen-zero override uses complementary banks.
For raw `O=0`, all impedance slots are zero. If both low bits are set, all ten
slots are nonzero. Otherwise slot1 is zero, slots2/3 correspond to raw bits0/1,
and slots4–9 correspond to bits2–7. Bank A uses slots1–9 (one for nonzero); bank B
is its complement. Thus raw `O=0` produces nine zeros, nine ones, then sixteen
zeros. An override containing ASCII `1` takes a different branch: **both banks
are zero**, followed by that override. Neither branch produces 34 zero bits.
Physical LED colors and a neutral/contact-unknown setting are not verified.
With `openlead=0`, an observed zero `O` mask cannot justify a good-contact claim.
The default-off [LED traffic experiment](seca_android_led_experiment_20261021.md)
tests this vendor-pattern host traffic; it is not an accepted continuity fix.

### Varying input and host-controller capture

Later varying-input trials separate framing failures from whole-group counter
gaps. Four earlier 4 kHz sessions lost framing, two before any periodic `R`.
After separating continuous reception from the durable archive consumer, four
more 4 kHz sessions still lost framing, again two before periodic `R`. The bounded
queue did not overflow (observed high-water 4,998 bytes). Input was not matched:
earlier valid frames implied approximately 23 kB/s, while three later sessions
implied 44–45 kB/s with six varying channels mostly using 12-bit deltas. This is
compressed ECG frame demand, not measured radio capacity; the comparison cannot
establish a pipeline improvement/regression or a hard baud ceiling.

The pipeline 2 kHz control retained 79,104 samples/channel (39.552 nominal
seconds), with seven missing 32-sample groups. The 1 kHz control retained 67,904
samples/channel (67.904 seconds), with nine missing groups. Both ended normally
without framing/checksum/decode failures. All these gaps followed an `R` query
(43–191 ms at 2 kHz; 83–161 ms at 1 kHz); six and five unanswered queries,
respectively, aligned with gaps. Temporal association does not establish query
causality. Neither lower-rate control is gap-free. Private replay evidence:
`data/seca_ct331/20261021/pipeline_analysis/comparison_report.md`.

A subsequent full Android HCI snoop contains eight failing 4 kHz sessions.
All 345,615 archived RX bytes match unique contiguous reconstructed HCI stream
spans exactly, including every fatal malformed record. Each fatal record is
exactly one complete RFCOMM UIH payload, with a valid RFCOMM header FCS (which does
not checksum the data payload). Independent verification directly reparsed
1,559 raw ACL/L2CAP/UIH data packets. Credit accounting was reset at each negotiated
RFCOMM interval and never exhausted the device's grants: the minimum observed
balance was seven in every interval. The malformed bytes therefore existed at
the phone's host-controller boundary, before application framing/archive; this
does not identify a recorder, module, controller or radio cause. It is not firmware
causal proof. Private metadata-only evidence:
`data/seca_ct331/20261021/hci_capture_1650/independent_confirmation.json`;
original app chunks remain under `hci_app_4k/` in the same date directory.

### LED traffic comparison and freshness race

The explicit LED candidate retained 149,184 samples/channel over 74.592 seconds
at 2 kHz with four varying measured channels, about 19.00 kB/s of frame demand,
and no missing groups or decode errors. Thirty-three LED commands replaced
periodic R; the initial configuration R/A remained. Variation was present before
and after the first LED. Ten varying 4 kHz sessions still failed framing, eight
before any LED, with at least 168 missing groups. Host keepalive replacement is
therefore not an established high-load 4 kHz fix.

A longer 2 kHz run preserved 590.400 nominal seconds across two sessions with no
within-session counter/decode errors, but a false stale-O failure caused a
4.580-second receive-time interruption. Fresh O arrived throughout, with a maximum
89.1 ms interval. The command caller sampled its clock before taking the observation
lock, allowing a newer observation to produce a negative apparent age. The reviewed
fix samples the clock under that lock; the original three-second freshness limit
and exact-observation recheck after durable TX acknowledgment remain unchanged.
Sustained validation of the corrected candidate is pending. All these later trials
used electrical noise/old electrodes, not properly positioned physiological ECG.

Android records mark setup RX and every TX packet `acquisition=false`; RX becomes
`acquisition=true` only after confirmed configuration and successful run write.
This prevents stale setup data being calibrated using the requested rate. Complete
raw bytes remain archived regardless of that flag. Previously captured experimental
TX records may carry the older flag convention and remain immutable.

## Host command frame

All multibyte integer fields below are big-endian.

| Offset | Field |
|---|---|
| 0–1 | `55 AA` |
| 2–3 | Total frame length = payload byte length + 14 |
| 4–7 | Fixed `12 34 56 78` (meaning unknown) |
| 8 | Zero |
| 9 | `W` for setting, `R` for configuration read |
| 10–11 | ASCII payload byte length |
| 12 onward | Payload, no terminator needed in observed commands |
| Final two | Negative sum of all preceding unsigned bytes, modulo65536 |

Verified volatile setting sequence: `mode=standby`, `samplerate=4000`, `gain=6`,
`openlead=0`, configuration read, `mode=run`. Stop with `mode=standby`. Do not
infer persistent-save, calibration or reset commands from these operations.

An empty read command is exactly `55aa000e1234567800520000fd8d`.
Vendor USB, Bluetooth and TCP connection callsites all construct an empty payload
with minimum allocation zero. **The device answered this command in standby**;
an early USB probe during acquisition did not yield an observed A response.
Later periodic Bluetooth queries did produce running-mode A replies, with rare
missing replies associated with missing ECG groups (see the continuity experiment).
Confirm configuration before starting. An A field may follow multiple P/M fields
within the same packet.
Readback text is CRLF-separated key=value, with some space-padded values. Confirm
`samplerate`, `gain`, `compressed` and contact mode. Full configuration also
contains network settings; do not print it to logs or publish it in fixtures.

## Received frame and fields

RX has a different header from TX. Bytes0–3 are magic/total length, bytes4–7 are
an unsigned wrapping32-bit counter, byte8 is observed0x20, body starts at9, and
last2 bytes are the same negative unsigned-byte-sum checksum. Minimum length11.
The counter identifies the **last ECG group in this packet**, not a timestamp.
An idle/status packet can contain no sample group.

| Body tag | Following bytes / meaning |
|---|---|
| `A` | u16 text length, configuration text |
| `O` | One electrode-off bitmask byte; rotate right2 to align with raw channel order |
| `P` | Battery millivolts u16, byte with charging flag0x80 and percentage low7bits; vendor accepts voltage>2000mV |
| `M` | One status byte; observed1 running,3 standby |
| `K` | One button/status byte; interpretation incomplete |
| `S` / `s` | Five pacemaker metadata bytes; retained raw, not interpreted |
| `C1`..`C6` hex | V1..V6 compressed channel blocks |
| `CA`, `CB` hex | I, II compressed channel blocks |

Important correction to historical sibling notes: **O is the lead-status field,
not the compressed sample payload.** Historical A/P/O/R/W evidence is retained
here with this corrected interpretation. Unimplemented field types must produce
a visible decode error while the complete original packet remains archived.

## Compression, counter allocation and leads

One measured channel block is tag, width byte, signed24-bit absolute initial
sample, then31 signed deltas in most-significant-bit-first packed order. Add each
delta cumulatively to reconstruct32 samples. Valid widths and serialized delta
byte counts:

| Bits | 0 | 4 | 8 | 12 | 16 | 20 | 24 |
|---|---|---|---|---|---|---|---|
| Bytes for31 deltas | 0 | 16 | 31 | 48 | 62 | 78 | 93 |

12-bit packing consumes48 bytes, including an extra padding byte after the final
half-byte. Width0 repeats the absolute initial sample. Sign extension is required
for both initial samples and deltas. Each block starts independently; no previous
packet's sample is needed.

Observed group order: C6, CA, CB, C2, C3, C4, C5, C1. C6 starts a group and C1
completes it; require exactly one of each of the eight channels. If a packet has
N groups and header counterL, allocate counters L−N+1 throughL modulo2^32.
Counter advances>1 indicate missing groups; repeats/regressions are separate
sequence errors. Do not join across resets/reconnections without a new session.

Raw order is V1..V6, I, II. Derive III=II−I, aVR=−(I+II)/2,
aVL=I−II/2, aVF=II−I/2. Retain half-count precision in derived leads.
Clinical names agree with vendor output mapping and historical CST export order.
`openlead=0` disables contact checking; zero masks then **do not** imply good
contact. Actual lead-off behavior still needs an enabled-mode live test.

## Rates, amplitude and time

Raw scale is `(9156 / gain) × rate_multiplier` nV/count. For confirmed gain6:

| Sample rate Hz | 1000 | 2000 | 4000 | 8000 | 16000 | 32000 |
|---|---|---|---|---|---|---|
| nV/raw count | 1526 | 1526 | 1526 | 3052 | 6104 | 12208 |

Current ingestion fixes and validates gain6. Independent electrical calibration
is pending; do not silently apply this scale at another gain. The ADC's marketing
resolution is not the transmitted sample scale. Other rate settings require live
throughput/continuity validation. Sample-rate acceptance does not establish the
analog/digital bandwidth or clinical suitability for high-frequency QRS analysis.
Pacemaker detection itself is not implemented or validated by ordinary ECG.

Preserve requested/readback rate, gain, transport, session UUID, packet counter,
host UTC nanoseconds and monotonic receive nanoseconds. No absolute device-time
field has been established. Relative sample times use counter×32/rate; packet
receipt times additionally include transport buffering. Display min/max envelopes
are previews only; raw packet bytes remain the archive of record.

## Remaining verification

- Longer sustained Bluetooth continuity, missing-group investigation and reconnect on Galaxy.
- All six USB rates and four Bluetooth rates; rate transitions and loss accounting.
- Enabled lead-contact flags, electrical scale and filter/bandwidth verification.
- Comparison against a simultaneous/reference Windows export, pacemaker metadata.
- Power loss, disconnects and offline retry of the Android/cloud archive path.
