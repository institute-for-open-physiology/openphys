use anyhow::{Result, anyhow};
#[cfg(test)]
use btleplug::api::Characteristic;
use std::future::Future;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::timeout;
use uuid::Uuid;

pub async fn ble_timeout<T, E, F>(operation: &str, duration: Duration, future: F) -> Result<T>
where
    F: Future<Output = std::result::Result<T, E>>,
    E: std::fmt::Display,
{
    match timeout(duration, future).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(err)) => Err(anyhow!("{operation}: {err}")),
        Err(_) => Err(anyhow!(
            "{operation}: timed out after {} ms",
            duration.as_millis()
        )),
    }
}

#[cfg(test)]
pub fn find_char_in<'a, I>(characteristics: I, uuid_str: &str) -> Option<Characteristic>
where
    I: IntoIterator<Item = &'a Characteristic>,
{
    let wanted = uuid(uuid_str);
    characteristics
        .into_iter()
        .find(|ch| ch.uuid == wanted)
        .cloned()
}

pub fn uuid(value: &str) -> Uuid {
    Uuid::parse_str(value).expect("constant UUID parses")
}

#[cfg(test)]
pub fn csv(value: &str) -> String {
    let escaped = value.replace('"', "\"\"");
    format!("\"{escaped}\"")
}

#[cfg(test)]
pub fn fmt_opt_f64(value: Option<f64>) -> String {
    value.map(|v| format!("{v:.3}")).unwrap_or_default()
}

pub fn unix_ns() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}

#[cfg(test)]
mod tests {
    use super::*;
    use btleplug::api::CharPropFlags;
    use std::collections::BTreeSet;
    use tokio::time::sleep;

    #[tokio::test]
    async fn ble_timeout_returns_success_value() {
        let value = ble_timeout("unit success", Duration::from_millis(50), async {
            Ok::<_, &str>(42)
        })
        .await
        .expect("timeout helper returns success");
        assert_eq!(value, 42);
    }

    #[tokio::test]
    async fn ble_timeout_wraps_inner_error() {
        let err = ble_timeout::<(), _, _>("unit error", Duration::from_millis(50), async {
            Err::<(), _>("boom")
        })
        .await
        .expect_err("timeout helper returns error");
        assert!(format!("{err:#}").contains("unit error: boom"));
    }

    #[tokio::test]
    async fn ble_timeout_reports_elapsed_timeout() {
        let err = ble_timeout::<(), _, _>("unit timeout", Duration::from_millis(1), async {
            sleep(Duration::from_millis(50)).await;
            Ok::<(), &str>(())
        })
        .await
        .expect_err("timeout helper times out");
        assert!(format!("{err:#}").contains("unit timeout: timed out after 1 ms"));
    }

    #[test]
    fn csv_quotes_and_escapes_values() {
        assert_eq!(csv("plain"), "\"plain\"");
        assert_eq!(csv("a\"b"), "\"a\"\"b\"");
        assert_eq!(csv("line\nnext"), "\"line\nnext\"");
    }

    #[test]
    fn formats_optional_floats() {
        assert_eq!(fmt_opt_f64(Some(1.23456)), "1.235");
        assert_eq!(fmt_opt_f64(None), "");
    }

    #[test]
    fn parses_uuid_and_finds_characteristic_in_slice() {
        let target_uuid = "00002a37-0000-1000-8000-00805f9b34fb";
        let other_uuid = "00002a38-0000-1000-8000-00805f9b34fb";
        let service_uuid = uuid("0000180d-0000-1000-8000-00805f9b34fb");
        let characteristics = [
            Characteristic {
                uuid: uuid(other_uuid),
                service_uuid,
                properties: CharPropFlags::READ,
                descriptors: BTreeSet::new(),
            },
            Characteristic {
                uuid: uuid(target_uuid),
                service_uuid,
                properties: CharPropFlags::NOTIFY,
                descriptors: BTreeSet::new(),
            },
        ];
        let found =
            find_char_in(characteristics.iter(), target_uuid).expect("characteristic found");
        assert_eq!(found.uuid, uuid(target_uuid));
        assert_eq!(found.properties, CharPropFlags::NOTIFY);
        assert!(
            find_char_in(
                characteristics.iter(),
                "00002a19-0000-1000-8000-00805f9b34fb"
            )
            .is_none()
        );
    }

    #[test]
    fn unix_ns_is_nonzero_and_monotonic_enough_for_tests() {
        let first = unix_ns();
        let second = unix_ns();
        assert!(first > 0);
        assert!(second >= first);
    }
}
