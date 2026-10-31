use anyhow::{Result, anyhow};
use btleplug::api::{
    Central as BtleplugCentral, CentralEvent, CharPropFlags, Characteristic,
    Peripheral as BtleplugPeripheral, PeripheralProperties, ValueNotification, WriteType,
};
use btleplug::platform::{Adapter, Peripheral};
use futures::{
    Stream, StreamExt,
    future::{BoxFuture, FutureExt},
};
use std::{collections::BTreeSet, pin::Pin};
use uuid::Uuid;

pub type BleNotificationStream = Pin<Box<dyn Stream<Item = BleNotification> + Send>>;
pub type BleCentralEventStream = Pin<Box<dyn Stream<Item = BleCentralEvent> + Send>>;

#[cfg(target_vendor = "apple")]
pub const RSSI_READ_METHOD: &str = "corebluetooth_read_rssi";
#[cfg(not(target_vendor = "apple"))]
pub const RSSI_READ_METHOD: &str = "platform_properties_rssi";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BleNotification {
    pub uuid: Uuid,
    pub value: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BleCentralEvent {
    DeviceDisconnected(String),
    StateUpdate(String),
    Other,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlePeripheralProperties {
    pub rssi: Option<i16>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BleCharacteristicProperties {
    pub read: bool,
    pub write: bool,
    pub write_without_response: bool,
    pub notify: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BleCharacteristic {
    pub uuid: Uuid,
    pub service_uuid: Uuid,
    pub properties: BleCharacteristicProperties,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BleWriteType {
    WithResponse,
    WithoutResponse,
}

pub trait GattPeripheral: Send + Sync {
    fn id_string(&self) -> String;
    fn is_connected_gatt(&self) -> BoxFuture<'_, Result<bool>>;
    fn connect_gatt(&self) -> BoxFuture<'_, Result<()>>;
    fn disconnect_gatt(&self) -> BoxFuture<'_, Result<()>>;
    fn discover_services_gatt(&self) -> BoxFuture<'_, Result<()>>;
    fn characteristics_gatt(&self) -> Vec<BleCharacteristic>;
    fn read_gatt(&self, characteristic: &BleCharacteristic) -> BoxFuture<'_, Result<Vec<u8>>>;
    fn write_gatt(
        &self,
        characteristic: &BleCharacteristic,
        data: &[u8],
        write_type: BleWriteType,
    ) -> BoxFuture<'_, Result<()>>;
    fn subscribe_gatt(&self, characteristic: &BleCharacteristic) -> BoxFuture<'_, Result<()>>;
    fn notifications_gatt(&self) -> BoxFuture<'_, Result<BleNotificationStream>>;
    fn read_rssi_gatt(&self) -> BoxFuture<'_, Result<i16>>;
    fn properties_gatt(&self) -> BoxFuture<'_, Result<Option<BlePeripheralProperties>>>;
}

pub trait StreamingCentral: Send + Sync {
    fn central_events_gatt(&self) -> BoxFuture<'_, Result<BleCentralEventStream>>;
}

pub fn find_ble_char(
    characteristics: &[BleCharacteristic],
    uuid_str: &str,
) -> Option<BleCharacteristic> {
    let wanted = crate::util::uuid(uuid_str);
    characteristics.iter().find(|ch| ch.uuid == wanted).cloned()
}

impl BleCharacteristicProperties {
    fn from_flags(flags: CharPropFlags) -> Self {
        Self {
            read: flags.contains(CharPropFlags::READ),
            write: flags.contains(CharPropFlags::WRITE),
            write_without_response: flags.contains(CharPropFlags::WRITE_WITHOUT_RESPONSE),
            notify: flags.contains(CharPropFlags::NOTIFY),
        }
    }

    fn to_flags(self) -> CharPropFlags {
        let mut flags = CharPropFlags::empty();
        if self.read {
            flags |= CharPropFlags::READ;
        }
        if self.write {
            flags |= CharPropFlags::WRITE;
        }
        if self.write_without_response {
            flags |= CharPropFlags::WRITE_WITHOUT_RESPONSE;
        }
        if self.notify {
            flags |= CharPropFlags::NOTIFY;
        }
        flags
    }
}

impl From<&Characteristic> for BleCharacteristic {
    fn from(value: &Characteristic) -> Self {
        Self {
            uuid: value.uuid,
            service_uuid: value.service_uuid,
            properties: BleCharacteristicProperties::from_flags(value.properties),
        }
    }
}

impl BleCharacteristic {
    fn to_btleplug(&self) -> Characteristic {
        Characteristic {
            uuid: self.uuid,
            service_uuid: self.service_uuid,
            properties: self.properties.to_flags(),
            descriptors: BTreeSet::new(),
        }
    }
}

impl From<BleWriteType> for WriteType {
    fn from(value: BleWriteType) -> Self {
        match value {
            BleWriteType::WithResponse => WriteType::WithResponse,
            BleWriteType::WithoutResponse => WriteType::WithoutResponse,
        }
    }
}

impl From<ValueNotification> for BleNotification {
    fn from(value: ValueNotification) -> Self {
        Self {
            uuid: value.uuid,
            value: value.value,
        }
    }
}

impl From<PeripheralProperties> for BlePeripheralProperties {
    fn from(value: PeripheralProperties) -> Self {
        Self { rssi: value.rssi }
    }
}

fn observed_rssi(rssi: Option<i16>) -> Result<i16> {
    rssi.ok_or_else(|| anyhow!("peripheral RSSI is unavailable"))
}

impl From<CentralEvent> for BleCentralEvent {
    fn from(value: CentralEvent) -> Self {
        match value {
            CentralEvent::DeviceDisconnected(id) => Self::DeviceDisconnected(id.to_string()),
            CentralEvent::StateUpdate(state) => Self::StateUpdate(format!("{state:?}")),
            _ => Self::Other,
        }
    }
}

impl GattPeripheral for Peripheral {
    fn id_string(&self) -> String {
        self.id().to_string()
    }

    fn is_connected_gatt(&self) -> BoxFuture<'_, Result<bool>> {
        async move {
            BtleplugPeripheral::is_connected(self)
                .await
                .map_err(|err| anyhow!("{err}"))
        }
        .boxed()
    }

    fn connect_gatt(&self) -> BoxFuture<'_, Result<()>> {
        async move {
            BtleplugPeripheral::connect(self)
                .await
                .map_err(|err| anyhow!("{err}"))
        }
        .boxed()
    }

    fn disconnect_gatt(&self) -> BoxFuture<'_, Result<()>> {
        async move {
            BtleplugPeripheral::disconnect(self)
                .await
                .map_err(|err| anyhow!("{err}"))
        }
        .boxed()
    }

    fn discover_services_gatt(&self) -> BoxFuture<'_, Result<()>> {
        async move {
            BtleplugPeripheral::discover_services(self)
                .await
                .map_err(|err| anyhow!("{err}"))
        }
        .boxed()
    }

    fn characteristics_gatt(&self) -> Vec<BleCharacteristic> {
        BtleplugPeripheral::characteristics(self)
            .iter()
            .map(BleCharacteristic::from)
            .collect()
    }

    fn read_gatt(&self, characteristic: &BleCharacteristic) -> BoxFuture<'_, Result<Vec<u8>>> {
        let characteristic = characteristic.to_btleplug();
        async move {
            BtleplugPeripheral::read(self, &characteristic)
                .await
                .map_err(|err| anyhow!("{err}"))
        }
        .boxed()
    }

    fn write_gatt(
        &self,
        characteristic: &BleCharacteristic,
        data: &[u8],
        write_type: BleWriteType,
    ) -> BoxFuture<'_, Result<()>> {
        let characteristic = characteristic.to_btleplug();
        let data = data.to_vec();
        async move {
            BtleplugPeripheral::write(self, &characteristic, &data, write_type.into())
                .await
                .map_err(|err| anyhow!("{err}"))
        }
        .boxed()
    }

    fn subscribe_gatt(&self, characteristic: &BleCharacteristic) -> BoxFuture<'_, Result<()>> {
        let characteristic = characteristic.to_btleplug();
        async move {
            BtleplugPeripheral::subscribe(self, &characteristic)
                .await
                .map_err(|err| anyhow!("{err}"))
        }
        .boxed()
    }

    fn notifications_gatt(&self) -> BoxFuture<'_, Result<BleNotificationStream>> {
        async move {
            let stream = BtleplugPeripheral::notifications(self)
                .await
                .map_err(|err| anyhow!("{err}"))?;
            Ok(stream.map(BleNotification::from).boxed())
        }
        .boxed()
    }

    fn read_rssi_gatt(&self) -> BoxFuture<'_, Result<i16>> {
        // The collector's RSSI branch wraps this entire future in config.rssi_timeout.
        #[cfg(target_vendor = "apple")]
        {
            async move {
                let rssi = Peripheral::read_rssi(self)
                    .await
                    .map_err(|err| anyhow!("{err}"))?;
                observed_rssi(Some(rssi))
            }
            .boxed()
        }

        #[cfg(not(target_vendor = "apple"))]
        {
            async move {
                let rssi = BtleplugPeripheral::properties(self)
                    .await
                    .map_err(|err| anyhow!("{err}"))?
                    .and_then(|properties| properties.rssi);
                observed_rssi(rssi)
            }
            .boxed()
        }
    }

    fn properties_gatt(&self) -> BoxFuture<'_, Result<Option<BlePeripheralProperties>>> {
        async move {
            BtleplugPeripheral::properties(self)
                .await
                .map(|properties| properties.map(BlePeripheralProperties::from))
                .map_err(|err| anyhow!("{err}"))
        }
        .boxed()
    }
}

impl StreamingCentral for Adapter {
    fn central_events_gatt(&self) -> BoxFuture<'_, Result<BleCentralEventStream>> {
        async move {
            let stream = BtleplugCentral::events(self)
                .await
                .map_err(|err| anyhow!("{err}"))?;
            Ok(stream.map(BleCentralEvent::from).boxed())
        }
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn characteristic_properties_round_trip_needed_flags() {
        let flags = CharPropFlags::READ
            | CharPropFlags::WRITE
            | CharPropFlags::WRITE_WITHOUT_RESPONSE
            | CharPropFlags::NOTIFY;
        let props = BleCharacteristicProperties::from_flags(flags);
        assert!(props.read);
        assert!(props.write);
        assert!(props.write_without_response);
        assert!(props.notify);
        assert!(props.to_flags().contains(CharPropFlags::READ));
        assert!(props.to_flags().contains(CharPropFlags::WRITE));
        assert!(
            props
                .to_flags()
                .contains(CharPropFlags::WRITE_WITHOUT_RESPONSE)
        );
        assert!(props.to_flags().contains(CharPropFlags::NOTIFY));
    }

    #[test]
    fn observed_rssi_is_loud_when_platform_properties_have_no_value() {
        assert_eq!(observed_rssi(Some(-67)).unwrap(), -67);
        assert!(observed_rssi(None).is_err());
    }
}
