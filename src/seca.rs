//! Independently implemented ct331 wire decoder. See docs/seca_ct331_protocol.md.
use anyhow::{Result, bail, ensure};
use serde::Serialize;

pub const RATES: [u32; 6] = [1000, 2000, 4000, 8000, 16000, 32000];
pub const LEADS: [&str; 12] = [
    "I", "II", "III", "aVR", "aVL", "aVF", "V1", "V2", "V3", "V4", "V5", "V6",
];

#[derive(Clone, Debug, Serialize)]
pub struct Block {
    pub counter: u32,
    /// V1..V6, I, II, in device integer units. Always 32 samples/channel.
    pub channels: Vec<Vec<i32>>,
    pub lead_off: Option<u8>,
}

#[derive(Debug, Default)]
pub struct Packet {
    pub blocks: Vec<Block>,
    pub battery_percent: Option<u8>,
    pub charging: Option<bool>,
    pub configuration: Option<String>,
}

/// Wire scaling at confirmed gain 6 only: 9156/gain nV × rate multiplier.
pub fn nano_volts_per_count(rate: u32) -> Result<u32> {
    ensure!(RATES.contains(&rate), "unsupported seca acquisition rate");
    Ok(1526 * (rate / 4000).max(1))
}

pub fn validate_frame(bytes: &[u8]) -> Result<()> {
    ensure!(
        bytes.len() >= 11 && bytes.len() <= 65535,
        "invalid seca frame size"
    );
    ensure!(bytes[..2] == [0x55, 0xaa], "invalid seca frame magic");
    ensure!(
        usize::from(u16::from_be_bytes([bytes[2], bytes[3]])) == bytes.len(),
        "seca frame length mismatch"
    );
    let end = bytes.len() - 2;
    let sum = bytes[..end]
        .iter()
        .fold(0u16, |a, &b| a.wrapping_add(u16::from(b)));
    ensure!(
        sum.wrapping_add(u16::from_be_bytes([bytes[end], bytes[end + 1]])) == 0,
        "seca checksum mismatch"
    );
    Ok(())
}

fn take<'a>(bytes: &'a [u8], at: &mut usize, count: usize) -> Result<&'a [u8]> {
    let end = at
        .checked_add(count)
        .ok_or_else(|| anyhow::anyhow!("seca offset overflow"))?;
    let result = bytes
        .get(*at..end)
        .ok_or_else(|| anyhow::anyhow!("truncated seca field"))?;
    *at = end;
    Ok(result)
}

fn signed(value: u32, bits: u32) -> i32 {
    ((value << (32 - bits)) as i32) >> (32 - bits)
}

fn channel(bytes: &[u8], at: &mut usize) -> Result<Vec<i32>> {
    let width = u32::from(take(bytes, at, 1)?[0]);
    ensure!(
        [0, 4, 8, 12, 16, 20, 24].contains(&width),
        "unsupported seca delta width {width}"
    );
    let base = take(bytes, at, 3)?;
    let mut value = signed(
        (u32::from(base[0]) << 16) | (u32::from(base[1]) << 8) | u32::from(base[2]),
        24,
    );
    // The 12-bit mode includes one padding byte after its last half-byte.
    let length = if width == 12 {
        48
    } else {
        (31 * width).div_ceil(8) as usize
    };
    let deltas = take(bytes, at, length)?;
    let mut result = vec![value];
    for sample in 0..31 {
        if width != 0 {
            let mut delta = 0u32;
            for bit in 0..width as usize {
                let index = sample * width as usize + bit;
                delta = (delta << 1) | u32::from((deltas[index / 8] >> (7 - index % 8)) & 1);
            }
            value = value
                .checked_add(signed(delta, width))
                .ok_or_else(|| anyhow::anyhow!("seca delta overflow"))?;
        }
        result.push(value);
    }
    Ok(result)
}

pub fn decode(bytes: &[u8]) -> Result<Packet> {
    validate_frame(bytes)?;
    let last_counter = u32::from_be_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
    let payload = &bytes[..bytes.len() - 2];
    let mut at = 9;
    let mut result = Packet::default();
    let mut channels: Vec<Option<Vec<i32>>> = vec![None; 8];
    let mut lead_off = None;
    while at < payload.len() {
        let kind = take(payload, &mut at, 1)?[0];
        match kind {
            b'A' => {
                let n = take(payload, &mut at, 2)?;
                let n = usize::from(u16::from_be_bytes([n[0], n[1]]));
                result.configuration =
                    Some(String::from_utf8_lossy(take(payload, &mut at, n)?).into_owned());
            }
            b'O' => lead_off = Some(take(payload, &mut at, 1)?[0].rotate_right(2)),
            b'P' => {
                let status = take(payload, &mut at, 3)?;
                let millivolts = u16::from_be_bytes([status[0], status[1]]);
                if millivolts > 2000 && status[2] & 0x7f <= 100 {
                    result.battery_percent = Some(status[2] & 0x7f);
                    result.charging = Some(status[2] & 0x80 != 0);
                }
            }
            b'M' | b'K' => {
                take(payload, &mut at, 1)?;
            }
            b'S' | b's' => {
                take(payload, &mut at, 5)?;
            }
            0xc1..=0xc6 | 0xca | 0xcb => {
                let index = if kind >= 0xca {
                    usize::from(kind - 0xc4)
                } else {
                    usize::from(kind - 0xc1)
                };
                if kind == 0xc6 {
                    ensure!(
                        channels.iter().all(Option::is_none),
                        "incomplete seca channel group"
                    );
                }
                ensure!(channels[index].is_none(), "duplicate seca channel");
                channels[index] = Some(channel(payload, &mut at)?);
                if kind == 0xc1 {
                    ensure!(channels.iter().all(Option::is_some), "missing seca channel");
                    let complete = channels
                        .iter_mut()
                        .map(|c| c.take().unwrap_or_default())
                        .collect();
                    result.blocks.push(Block {
                        counter: 0,
                        channels: complete,
                        lead_off,
                    });
                }
            }
            _ => bail!("unsupported seca field 0x{kind:02x} at {}", at - 1),
        }
    }
    ensure!(
        channels.iter().all(Option::is_none),
        "truncated seca channel group"
    );
    let count = result.blocks.len() as u32;
    for (i, block) in result.blocks.iter_mut().enumerate() {
        block.counter = last_counter.wrapping_sub(count - 1).wrapping_add(i as u32);
    }
    Ok(result)
}

/// Derived limb leads retain half-count precision. Raw channel counts remain unchanged.
pub fn lead_sample(block: &Block, sample: usize) -> [Option<f64>; 12] {
    let read = |index: usize| {
        if block.lead_off.is_some_and(|mask| mask & (1 << index) != 0) {
            None
        } else {
            block
                .channels
                .get(index)
                .and_then(|c| c.get(sample))
                .map(|&n| f64::from(n))
        }
    };
    let mut result = [None; 12];
    result[0] = read(6);
    result[1] = read(7);
    if let (Some(i), Some(ii)) = (result[0], result[1]) {
        result[2] = Some(ii - i);
        result[3] = Some(-(i + ii) / 2.0);
        result[4] = Some(i - ii / 2.0);
        result[5] = Some(ii - i / 2.0);
    }
    for index in 0..6 {
        result[index + 6] = read(index);
    }
    result
}

#[cfg(test)]
pub mod tests {
    use super::*;

    pub fn frame(payload: &[u8], counter: u32) -> Vec<u8> {
        let mut b = vec![0x55, 0xaa];
        b.extend_from_slice(&((payload.len() + 11) as u16).to_be_bytes());
        b.extend_from_slice(&counter.to_be_bytes());
        b.push(32);
        b.extend_from_slice(payload);
        let sum = b.iter().fold(0u16, |a, &v| a.wrapping_add(u16::from(v)));
        b.extend_from_slice(&sum.wrapping_neg().to_be_bytes());
        b
    }

    pub fn group(width: u8) -> Vec<u8> {
        let mut p = vec![b'O', 0];
        for id in [0xc6, 0xca, 0xcb, 0xc2, 0xc3, 0xc4, 0xc5, 0xc1] {
            p.extend_from_slice(&[id, width, 0xff, 0xff, 0xfe]);
            let n = if width == 12 {
                48
            } else {
                (31 * usize::from(width)).div_ceil(8)
            };
            // All bits set gives -1 for every signed delta width.
            p.extend(std::iter::repeat_n(0xff, n));
        }
        p
    }

    #[test]
    fn decodes_all_widths_and_multiple_groups_without_cross_packet_state() {
        for width in [0, 4, 8, 12, 16, 20, 24] {
            let mut p = group(width);
            p.extend(group(width));
            let decoded = decode(&frame(&p, 2)).unwrap();
            assert_eq!(decoded.blocks.len(), 2);
            assert_eq!(decoded.blocks[0].counter, 1);
            for block in decoded.blocks {
                assert_eq!(block.channels.len(), 8);
                assert!(
                    block
                        .channels
                        .iter()
                        .all(|c| c[0] == -2 && c[31] == if width == 0 { -2 } else { -33 })
                );
            }
        }
    }

    #[test]
    fn mixed_signed_boundary_vectors_preserve_bit_order_and_padding() {
        // Hand-written, independently inspectable max/min/+1/-1 encodings.
        for (width, prefix) in [
            (4, "781f"),
            (8, "7f8001ff"),
            (12, "7ff800001fff"),
            (16, "7fff80000001ffff"),
            (20, "7ffff8000000001fffff"),
            (24, "7fffff800000000001ffffff"),
        ] {
            let mut encoded = vec![width, 0xff, 0xff, 0xfe];
            let length = if width == 12 {
                48
            } else {
                (31 * usize::from(width)).div_ceil(8)
            };
            encoded.extend(hex::decode(prefix).unwrap());
            encoded.resize(4 + length, 0);
            if width == 12 {
                *encoded.last_mut().unwrap() = 0xa5;
            }
            let mut at = 0;
            let values = channel(&encoded, &mut at).unwrap();
            assert_eq!(at, encoded.len());
            assert_eq!(
                &values[..5],
                &[-2, (1i32 << (width - 1)) - 3, -3, -2, -3],
                "{width}-bit"
            );
            assert!(values[5..].iter().all(|&v| v == -3));
        }
        let mut payload = group(0);
        payload[1] = 0b0000_0001;
        let packet = decode(&frame(&payload, 0)).unwrap();
        assert_eq!(packet.blocks[0].counter, 0);
        assert_eq!(packet.blocks[0].lead_off, Some(0b0100_0000));
        let mut payload = group(0);
        payload.extend(group(0));
        let packet = decode(&frame(&payload, 0)).unwrap();
        assert_eq!(packet.blocks[0].counter, u32::MAX);
        assert_eq!(packet.blocks[1].counter, 0);
        assert!(
            decode(&frame(&[b'P', 0, 0, 0xff], 0))
                .unwrap()
                .battery_percent
                .is_none()
        );
    }

    #[test]
    #[ignore = "requires the private independently decoded on-body fixture via SECA_GOLDEN_PATH"]
    fn private_capture_matches_independent_byte_nibble_decoder() {
        let path = std::env::var("SECA_GOLDEN_PATH").expect("private fixture path required");
        let fixture: serde_json::Value =
            serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        let packet = decode(&hex::decode(fixture["hex"].as_str().unwrap()).unwrap()).unwrap();
        assert_eq!(packet.blocks.len(), 2);
        assert_eq!(packet.blocks[0].counter, 468);
        assert_eq!(packet.blocks[1].counter, 469);
        for (position, expected) in fixture["channels_in_wire_order"]
            .as_array()
            .unwrap()
            .iter()
            .enumerate()
        {
            let id = u8::from_str_radix(
                expected["id"].as_str().unwrap().trim_start_matches("0x"),
                16,
            )
            .unwrap();
            let index = if id >= 0xca {
                usize::from(id - 0xc4)
            } else {
                usize::from(id - 0xc1)
            };
            let values: Vec<i32> = expected["samples"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_i64().unwrap() as i32)
                .collect();
            assert_eq!(packet.blocks[position / 8].channels[index], values);
        }
    }

    #[test]
    fn validates_framing_and_rejects_malformed_data() {
        assert!(decode(&[]).is_err());
        let mut f = frame(&group(8), 1);
        f[0] = 0;
        assert!(decode(&f).is_err());
        f[0] = 0x55;
        f[2] = 0xff;
        assert!(decode(&f).is_err());
        f = frame(&group(8), 1);
        f[12] ^= 1;
        assert!(decode(&f).is_err());
        for payload in [
            vec![b'P'],
            vec![0xc6, 7, 0, 0, 0],
            vec![0xc6, 0, 0],
            vec![0xc1, 0, 0, 0, 0],
            vec![0xc6, 0, 0, 0, 0],
            vec![b'?'],
            vec![b'A', 0, 3, b'x'],
        ] {
            assert!(decode(&frame(&payload, 1)).is_err(), "{payload:?}");
        }
        let mut p = vec![0xc2, 0, 0, 0, 0];
        p.extend_from_slice(&[0xc2, 0, 0, 0, 0]);
        assert!(decode(&frame(&p, 1)).is_err());
        p[5] = 0xc6;
        assert!(decode(&frame(&p, 1)).is_err());
    }

    #[test]
    fn status_configuration_idle_scaling_and_leads() {
        let mut p = vec![
            b'A',
            0,
            3,
            b'x',
            b'=',
            b'1',
            b'P',
            0x10,
            0,
            0x80 | 73,
            b'M',
            1,
            b'K',
            0,
            b'S',
            0,
            0,
            0,
            0,
            0,
            b's',
            0,
            0,
            0,
            0,
            0,
        ];
        p.extend(group(0));
        let d = decode(&frame(&p, 1)).unwrap();
        assert_eq!(d.configuration.as_deref(), Some("x=1"));
        assert_eq!(d.battery_percent, Some(73));
        assert_eq!(d.charging, Some(true));
        assert!(decode(&frame(&[], 0)).unwrap().blocks.is_empty());
        for (rate, scale) in [
            (1000, 1526),
            (2000, 1526),
            (4000, 1526),
            (8000, 3052),
            (16000, 6104),
            (32000, 12208),
        ] {
            assert_eq!(nano_volts_per_count(rate).unwrap(), scale);
        }
        assert!(nano_volts_per_count(130).is_err());
        let mut block = d.blocks[0].clone();
        block.channels[6][0] = 3;
        block.channels[7][0] = 8;
        assert_eq!(
            &lead_sample(&block, 0)[..6],
            &[
                Some(3.),
                Some(8.),
                Some(5.),
                Some(-5.5),
                Some(-1.),
                Some(6.5)
            ]
        );
        block.lead_off = Some(1 << 6);
        assert!(lead_sample(&block, 0)[0].is_none());
        assert!(lead_sample(&block, 0)[2].is_none());
        assert!(lead_sample(&block, 99).iter().all(Option::is_none));
    }
}
