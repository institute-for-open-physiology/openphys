#!/usr/bin/env python3
"""Probe Polar H10 ECG PMD settings/frame types.

This is a short disruptive test: stop the main collector first.
"""

from __future__ import annotations

import argparse
import asyncio
import time
from dataclasses import dataclass, field
from typing import Any

from bleak import BleakClient, BleakScanner

PMD_CONTROL = "fb005c81-02e7-f387-1cad-8acd2d8df0c8"
PMD_DATA = "fb005c82-02e7-f387-1cad-8acd2d8df0c8"

ECG_STOP = bytes([0x03, 0x00])

# Setting type helpers: [setting_type, count=1, little-endian value]
def u16_setting(setting_type: int, value: int) -> bytes:
    return bytes([setting_type, 0x01, value & 0xFF, (value >> 8) & 0xFF])


def u8_setting(setting_type: int, value: int) -> bytes:
    return bytes([setting_type, 0x01, value & 0xFF])


@dataclass
class ProbeState:
    control: asyncio.Queue[bytes] = field(default_factory=asyncio.Queue)
    data_frames: list[bytes] = field(default_factory=list)


def parse_settings(data: bytes) -> list[tuple[int, list[int]]]:
    out: list[tuple[int, list[int]]] = []
    offset = 0
    sizes = {0: 2, 1: 2, 2: 2, 3: 4, 4: 1, 5: 4, 6: 1}
    while offset + 2 <= len(data):
        typ = data[offset]
        count = data[offset + 1]
        offset += 2
        size = sizes.get(typ)
        if size is None or offset + count * size > len(data):
            out.append((typ, [int.from_bytes(data[offset:], "little") if offset < len(data) else 0]))
            break
        values = []
        for _ in range(count):
            values.append(int.from_bytes(data[offset:offset + size], "little", signed=False))
            offset += size
        out.append((typ, values))
    return out


def setting_name(typ: int) -> str:
    return {
        0: "sample_rate_hz",
        1: "resolution_bits",
        2: "range",
        3: "range_milliunit/legacy",
        4: "channels",
        5: "factor_f32_bits",
        6: "security",
    }.get(typ, f"unknown_{typ}")


def describe_control(data: bytes) -> str:
    if len(data) >= 5 and data[0] == 0xF0:
        op, typ, err, more = data[1], data[2], data[3], data[4]
        params = data[5:]
        settings = parse_settings(params) if err == 0 and params else []
        settings_s = "; ".join(f"{setting_name(k)}={v}" for k, v in settings)
        return f"op={op} type={typ} err={err} more={more} params={params.hex()} {settings_s}"
    return data.hex()


async def wait_control(state: ProbeState, wanted_op: int | None = None, wanted_type: int | None = None, timeout_s: float = 3.0) -> bytes | None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            data = await asyncio.wait_for(state.control.get(), timeout=deadline - time.monotonic())
        except asyncio.TimeoutError:
            return None
        if len(data) >= 5 and data[0] == 0xF0:
            if wanted_op is not None and data[1] != wanted_op:
                continue
            if wanted_type is not None and data[2] != wanted_type:
                continue
        return data
    return None


async def query_settings(client: BleakClient, state: ProbeState, op: int, measurement_type: int, label: str) -> None:
    print(f"\nQUERY {label}: write {[op, measurement_type]}")
    await client.write_gatt_char(PMD_CONTROL, bytes([op, measurement_type]), response=True)
    response = await wait_control(state, wanted_op=op, wanted_type=measurement_type, timeout_s=4.0)
    if response is None:
        print("  no response")
    else:
        print(f"  response {response.hex()} -> {describe_control(response)}")


async def try_start(client: BleakClient, state: ProbeState, label: str, settings: bytes) -> None:
    print(f"\nTRY {label}: settings={settings.hex()}")
    state.data_frames.clear()
    await client.write_gatt_char(PMD_CONTROL, ECG_STOP, response=True)
    await asyncio.sleep(0.3)
    while not state.control.empty():
        try:
            state.control.get_nowait()
        except asyncio.QueueEmpty:
            break
    cmd = bytes([0x02, 0x00]) + settings
    await client.write_gatt_char(PMD_CONTROL, cmd, response=True)
    response = await wait_control(state, wanted_op=2, wanted_type=0, timeout_s=4.0)
    print(f"  start response {response.hex() if response else None} -> {describe_control(response) if response else 'none'}")
    await asyncio.sleep(3.0)
    ecg_frames = [f for f in state.data_frames if len(f) >= 10 and (f[0] & 0x3F) == 0]
    frame_types = sorted({f[9] & 0x7F for f in ecg_frames})
    compressed = sorted({bool(f[9] & 0x80) for f in ecg_frames})
    print(f"  ECG frames={len(ecg_frames)} frame_types={frame_types} compressed_flags={compressed}")
    if ecg_frames:
        first = ecg_frames[0]
        print(f"  first ECG frame len={len(first)} header={first[:10].hex()} payload_prefix={first[10:28].hex()}")
    await client.write_gatt_char(PMD_CONTROL, ECG_STOP, response=True)
    await asyncio.sleep(0.4)


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", default="A1B2C3D4")
    args = parser.parse_args()

    target = args.target.lower()
    print(f"Scanning for {args.target}...")
    device = await BleakScanner.find_device_by_filter(
        lambda d, ad: (d.name and target in d.name.lower()) or target in str(d.address).lower(),
        timeout=15,
    )
    if device is None:
        raise SystemExit("target not found")
    print(f"Found {device.name} {device.address}")

    state = ProbeState()

    def on_control(_: Any, data: bytearray) -> None:
        b = bytes(data)
        print(f"  control notify {b.hex()} -> {describe_control(b)}")
        state.control.put_nowait(b)

    def on_data(_: Any, data: bytearray) -> None:
        state.data_frames.append(bytes(data))

    async with BleakClient(device, timeout=20.0) as client:
        print("Connected")
        await client.start_notify(PMD_CONTROL, on_control)
        await client.start_notify(PMD_DATA, on_data)
        try:
            features = await client.read_gatt_char(PMD_CONTROL)
            print(f"PMD features/read: {bytes(features).hex()}")
        except Exception as exc:
            print(f"PMD read failed: {exc!r}")

        await query_settings(client, state, 0x01, 0x00, "ECG online settings")
        await query_settings(client, state, 0x04, 0x00, "ECG full/SDK-mode settings")

        candidates = [
            ("default sr=130,res=14", u16_setting(0, 130) + u16_setting(1, 14)),
            ("default + channels=1", u16_setting(0, 130) + u16_setting(1, 14) + u8_setting(4, 1)),
            ("sr=130,res=16", u16_setting(0, 130) + u16_setting(1, 16)),
            ("sr=130,res=18", u16_setting(0, 130) + u16_setting(1, 18)),
            ("sr=130,res=22", u16_setting(0, 130) + u16_setting(1, 22)),
            ("sr=130,res=24", u16_setting(0, 130) + u16_setting(1, 24)),
        ]
        for label, settings in candidates:
            await try_start(client, state, label, settings)

        await client.stop_notify(PMD_DATA)
        await client.stop_notify(PMD_CONTROL)


if __name__ == "__main__":
    asyncio.run(main())
