"""Async BLE helpers for Shimmer3R LogAndStream v1.0.33 devices."""

import asyncio
import time
from collections.abc import AsyncIterator
from contextlib import suppress
from dataclasses import dataclass, field
from struct import calcsize, pack, unpack

from bleak import BleakClient, BleakScanner
from bleak.backends.device import BLEDevice
from bleak.backends.scanner import AdvertisementData
from pyshimmer.bluetooth import bt_const as btc
from pyshimmer.dev import channels as dch
from pyshimmer.dev.revisions.shimmer3r import Shimmer3RRevision

SHIMMER3R_REVISION = Shimmer3RRevision()

CYSPP_SERVICE_UUID = "65333333-a115-11e2-9e9a-0800200ca100"
CYSPP_ACKED_DATA_UUID = "65333333-a115-11e2-9e9a-0800200ca101"
CYSPP_UNACKED_DATA_UUID = "65333333-a115-11e2-9e9a-0800200ca102"
CYSPP_RX_FLOW_UUID = "65333333-a115-11e2-9e9a-0800200ca103"

DEFAULT_CONNECT_TIMEOUT = 15.0
DEFAULT_SCAN_TIMEOUT = 10.0
DEFAULT_IO_TIMEOUT = 5.0
DEFAULT_NAME_PREFIX = "Shimmer3R-"
DEFAULT_NAME_SUFFIX = "-BLE"

SET_INTERNAL_EXP_POWER_ENABLE_COMMAND = 0x5E
INTERNAL_EXP_POWER_ENABLE_RESPONSE = 0x5F
GET_INTERNAL_EXP_POWER_ENABLE_COMMAND = 0x60

RESET_TO_DEFAULT_CONFIGURATION_COMMAND = 0x5A

SET_GSR_RANGE_COMMAND = 0x21
GSR_RANGE_RESPONSE = 0x22
GET_GSR_RANGE_COMMAND = 0x23

# NB the settings defines the reference resistor, but the variables here
# are named according to the intended skin resistance range to be measured.
GSR_CONFIG_8k_63k = 0
GSR_CONFIG_63k_220k = 1
# Option 2 and 3 are recommended for typical tonic skin resistance.
GSR_CONFIG_220k_680k = 2
GSR_CONFIG_680k_4M7 = 3
GSR_CONFIG_AUTORANGE = 4

GSR_FEEDBACK_RESISTORS_TO_CONFIG = {
    40.2e3: GSR_CONFIG_8k_63k,
    287e3: GSR_CONFIG_63k_220k,
    1e6: GSR_CONFIG_220k_680k,
    3.3e6: GSR_CONFIG_680k_4M7,
    "auto": GSR_CONFIG_AUTORANGE,
}
GSR_CONFIG_TO_FEEDBACK_RESISTORS = {v: k for k, v in GSR_FEEDBACK_RESISTORS_TO_CONFIG.items()}

GSR_VALID_RANGES = {
    GSR_CONFIG_8k_63k: (8e3, 63e3),
    GSR_CONFIG_63k_220k: (63e3, 220e3),
    GSR_CONFIG_220k_680k: (220e3, 680e3),
    GSR_CONFIG_680k_4M7: (680e3, 4.7e6),
}


@dataclass
class Shimmer3BleConnection:
    """Minimal BLE byte-stream wrapper around the Shimmer3R CYSPP service."""

    client: BleakClient
    read_timeout: float | None = DEFAULT_IO_TIMEOUT
    write_uuid: str = CYSPP_ACKED_DATA_UUID
    read_uuids: tuple[str, ...] = (CYSPP_ACKED_DATA_UUID, CYSPP_UNACKED_DATA_UUID)
    flow_uuid: str = CYSPP_RX_FLOW_UUID
    write_with_response: bool = True
    flow_paused: bool = False
    _rx_queue: asyncio.Queue[bytes] = field(default_factory=asyncio.Queue, init=False)
    _rx_buffer: bytearray = field(default_factory=bytearray, init=False)
    _notify_uuids: list[str] = field(default_factory=list, init=False)
    _loop: asyncio.AbstractEventLoop | None = field(default=None, init=False)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc_info: object) -> None:
        await close(self)

    def _queue_bytes(self, data: bytearray) -> None:
        if not data or self._loop is None:
            return
        self._loop.call_soon_threadsafe(self._rx_queue.put_nowait, bytes(data))

    def _update_flow_state(self, data: bytearray) -> None:
        if not data:
            return
        self.flow_paused = data[-1] != 0


def calc_gsr(scale: int, adc: int):
    rf: int = GSR_CONFIG_TO_FEEDBACK_RESISTORS[scale]
    gsr_ohm = rf / ((adc * 3 * 2 / 4095) - 1.0)
    return gsr_ohm


def validate_gsr(scale: int, gsr_ohm: float, tolerance: float = 0.1):
    min_gsr, max_gsr = GSR_VALID_RANGES[scale]
    return min_gsr * (1 - tolerance) < gsr_ohm and gsr_ohm < max_gsr * (1 + tolerance)


def decode_gsr(gsr_raw: int):
    # Top two bits: scale, lower 12 bits: ADC.
    scale = (gsr_raw >> 14) & 3
    adc = gsr_raw & 0x0FFF
    return scale, adc


async def find_device(
    name: str | None = None,
    *,
    timeout: float = DEFAULT_SCAN_TIMEOUT,
) -> BLEDevice:
    """Find a Shimmer3R BLE device by exact name, or the first Shimmer3R BLE advertisement."""

    def matches(device: BLEDevice, advertisement_data: AdvertisementData) -> bool:
        device_name = advertisement_data.local_name or device.name
        if device_name is None:
            return False
        if name is not None:
            return device_name == name
        return device_name.startswith(DEFAULT_NAME_PREFIX) and device_name.endswith(DEFAULT_NAME_SUFFIX)

    device = await BleakScanner.find_device_by_filter(matches, timeout=timeout)
    if device is None:
        if name is None:
            raise TimeoutError("Timed out scanning for a Shimmer3R BLE device")
        raise TimeoutError(f"Timed out scanning for BLE device named {name!r}")
    return device


async def connect(
    address: str | None = None,
    *,
    name: str | None = None,
    scan_timeout: float = DEFAULT_SCAN_TIMEOUT,
    connect_timeout: float = DEFAULT_CONNECT_TIMEOUT,
    read_timeout: float | None = DEFAULT_IO_TIMEOUT,
) -> Shimmer3BleConnection:
    """Connect to the Shimmer3R CYSPP BLE service and enable data notifications."""

    if address is None:
        target: str | BLEDevice = await find_device(name=name, timeout=scan_timeout)
    else:
        target = address

    client = BleakClient(target, timeout=connect_timeout)
    await client.connect()

    conn = Shimmer3BleConnection(client=client, read_timeout=read_timeout)
    try:
        await start_notifications(conn)
        await clear_input(conn)
        # Prevent ACK prefixes on unsolicited in-stream status responses.
        await send_cmd(conn, "BB", btc.ENABLE_STATUS_ACK_COMMAND, False)
    except Exception:
        await close(conn)
        raise

    return conn


async def shimmer3_ble(*args, **kwargs) -> Shimmer3BleConnection:
    """Alias for connect(), kept as a discoverable constructor name."""

    return await connect(*args, **kwargs)


async def start_notifications(conn: Shimmer3BleConnection) -> None:
    conn._loop = asyncio.get_running_loop()

    for uuid in conn.read_uuids:
        if uuid in conn._notify_uuids:
            continue
        await conn.client.start_notify(uuid, lambda _sender, data: conn._queue_bytes(data))
        conn._notify_uuids.append(uuid)

    # RX flow is CYSPP flow control, not Shimmer protocol data.
    with suppress(Exception):
        await conn.client.start_notify(
            conn.flow_uuid,
            lambda _sender, data: conn._update_flow_state(data),
        )
        conn._notify_uuids.append(conn.flow_uuid)


async def stop_notifications(conn: Shimmer3BleConnection) -> None:
    for uuid in reversed(conn._notify_uuids):
        with suppress(Exception):
            await conn.client.stop_notify(uuid)
    conn._notify_uuids.clear()


async def close(conn: Shimmer3BleConnection) -> None:
    await stop_notifications(conn)
    if conn.client.is_connected:
        await conn.client.disconnect()


async def clear_input(conn: Shimmer3BleConnection) -> None:
    conn._rx_buffer.clear()
    while True:
        try:
            conn._rx_queue.get_nowait()
        except asyncio.QueueEmpty:
            return


async def write(conn: Shimmer3BleConnection, data: bytes) -> None:
    try:
        await conn.client.write_gatt_char(
            conn.write_uuid,
            data,
            response=conn.write_with_response,
        )
    except Exception:
        if not conn.write_with_response:
            raise
        await conn.client.write_gatt_char(conn.write_uuid, data, response=False)
        conn.write_with_response = False


async def read(conn: Shimmer3BleConnection, size: int = 1, timeout: float | None = None) -> bytes:
    if size < 0:
        raise ValueError("size must be non-negative")
    if size == 0:
        return b""

    effective_timeout = conn.read_timeout if timeout is None else timeout
    loop = asyncio.get_running_loop()
    deadline = None if effective_timeout is None else loop.time() + effective_timeout

    while len(conn._rx_buffer) < size:
        if deadline is None:
            chunk = await conn._rx_queue.get()
        else:
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise TimeoutError(f"Timed out reading {size} byte(s) from Shimmer3R BLE")
            try:
                chunk = await asyncio.wait_for(conn._rx_queue.get(), timeout=remaining)
            except asyncio.TimeoutError as exc:
                raise TimeoutError(f"Timed out reading {size} byte(s) from Shimmer3R BLE") from exc
        conn._rx_buffer.extend(chunk)

    result = bytes(conn._rx_buffer[:size])
    del conn._rx_buffer[:size]
    return result


async def send_cmd(
    conn: Shimmer3BleConnection,
    fmt: str | bytes,
    *args,
    timeout: float | None = None,
) -> None:
    cmd = pack(fmt, *args)
    await write(conn, cmd)
    await wait_for_byte(conn, timeout=timeout)


async def read_packed(conn: Shimmer3BleConnection, fmt: str | bytes):
    return unpack(fmt, await read(conn, calcsize(fmt)))


async def wait_for_byte(
    conn: Shimmer3BleConnection,
    b: int = btc.ACK_COMMAND_PROCESSED,
    *,
    timeout: float | None = None,
) -> bytes:
    ack = pack("B", b)
    while True:
        read_bytes = await read(conn, 1, timeout=timeout)
        if read_bytes == ack:
            return read_bytes


async def set_gsr_range(conn: Shimmer3BleConnection, gsr_range: int) -> None:
    await send_cmd(conn, "BB", SET_GSR_RANGE_COMMAND, gsr_range)


async def get_gsr_range(conn: Shimmer3BleConnection) -> int:
    await send_cmd(conn, "B", GET_GSR_RANGE_COMMAND)
    await wait_for_byte(conn, GSR_RANGE_RESPONSE)
    return (await read_packed(conn, "B"))[0]


async def get_gsr_feedback_resistor(conn: Shimmer3BleConnection):
    gsr_range_raw = await get_gsr_range(conn)
    return GSR_CONFIG_TO_FEEDBACK_RESISTORS[gsr_range_raw]


async def reset_to_default_config(conn: Shimmer3BleConnection) -> None:
    await send_cmd(conn, "B", RESET_TO_DEFAULT_CONFIGURATION_COMMAND)


async def get_real_world_clock(conn: Shimmer3BleConnection):
    await send_cmd(conn, "B", btc.GET_RWC_COMMAND)
    await wait_for_byte(conn, btc.RWC_RESPONSE)
    ticks = (await read_packed(conn, "<Q"))[0]
    return float(SHIMMER3R_REVISION.ticks2sec(ticks))


async def set_real_world_clock(conn: Shimmer3BleConnection, rwc: float) -> None:
    ticks = SHIMMER3R_REVISION.sec2ticks(rwc)
    await send_cmd(conn, "<BQ", btc.SET_RWC_COMMAND, ticks)


async def set_device_rate(conn: Shimmer3BleConnection, device_rate: int) -> None:
    await send_cmd(conn, "<BH", btc.SET_SAMPLING_RATE_COMMAND, device_rate)


async def set_sampling_rate(conn: Shimmer3BleConnection, rate_hz: float) -> None:
    device_rate = SHIMMER3R_REVISION.sr2dr(rate_hz)
    await set_device_rate(conn, device_rate)


async def get_internal_exp_power(conn: Shimmer3BleConnection) -> bool:
    await send_cmd(conn, "B", GET_INTERNAL_EXP_POWER_ENABLE_COMMAND)
    await wait_for_byte(conn, INTERNAL_EXP_POWER_ENABLE_RESPONSE)
    return bool((await read_packed(conn, "B"))[0])


async def set_internal_exp_power(conn: Shimmer3BleConnection, enable: bool) -> None:
    await send_cmd(conn, "BB", SET_INTERNAL_EXP_POWER_ENABLE_COMMAND, int(enable))


async def set_sensors(
    conn: Shimmer3BleConnection,
    sensors: list[dch.ESensorGroup],
) -> None:
    bitfield = SHIMMER3R_REVISION.serialize_sensorlist(sensors)
    await send_cmd(conn, "<B3s", btc.SET_SENSORS_COMMAND, bitfield)


def decode_channel_types(ch_types_raw: bytes):
    ctypes_index = unpack("B" * len(ch_types_raw), ch_types_raw)
    return [dch.EChannelType.enum_for_id(cti) for cti in ctypes_index]


async def inquire(conn: Shimmer3BleConnection):
    await send_cmd(conn, "B", btc.INQUIRY_COMMAND)
    await wait_for_byte(conn, btc.INQUIRY_RESPONSE)
    # fmt = "<HIBB"  # Shimmer3
    fmt = "<HI3xBB"  # Shimmer3R
    _device_rate, _, n_ch, _buf_size = await read_packed(conn, fmt)
    ch_types_raw = await read(conn, n_ch)
    return [dch.EChannelType.TIMESTAMP] + decode_channel_types(ch_types_raw)


async def start_streaming(conn: Shimmer3BleConnection) -> None:
    await send_cmd(conn, "B", btc.START_STREAMING_COMMAND)

    status_response_len = 4
    status_raw = await read(conn, status_response_len)
    if status_raw[:2] != btc.FULL_STATUS_RESPONSE:
        raise RuntimeError(f"Unexpected start-streaming status response: 0x{status_raw.hex()}")


async def stop_streaming(conn: Shimmer3BleConnection) -> None:
    await send_cmd(conn, "B", btc.STOP_STREAMING_COMMAND)


async def live_stream_raw(conn: Shimmer3BleConnection) -> None:
    while True:
        bb = await read(conn, 1)
        print(f"Live: 0x{bb.hex()}" if bb else "")


async def stream_raw(conn: Shimmer3BleConnection) -> None:
    await start_streaming(conn)
    try:
        await asyncio.sleep(2)  # While the device is streaming, data is buffered.
    finally:
        await stop_streaming(conn)


async def read_data_packet(
    conn: Shimmer3BleConnection,
    ch_types: list[dch.EChannelType],
):
    stream_types = [(t, SHIMMER3R_REVISION.get_channel_dtype(t)) for t in ch_types]
    await wait_for_byte(conn, btc.DATA_PACKET)
    return {
        channel_type: channel_dtype.decode(await read(conn, channel_dtype.size))
        for channel_type, channel_dtype in stream_types
    }


async def data_packets(
    conn: Shimmer3BleConnection,
    ch_types: list[dch.EChannelType],
) -> AsyncIterator[dict[dch.EChannelType, int]]:
    while True:
        yield await read_data_packet(conn, ch_types)


async def live_stream(conn: Shimmer3BleConnection, ch_types: list[dch.EChannelType]) -> None:
    print(f"Channels: {ch_types}")
    ticks_implicit = int(time.time() * SHIMMER3R_REVISION.DEV_CLOCK_RATE) >> (8 * 3) << (8 * 3)
    async for packet in data_packets(conn, ch_types):
        ts = time.time()
        sample = {k.name: v for k, v in packet.items()}
        ts_shimmer = SHIMMER3R_REVISION.ticks2sec(
            packet[dch.EChannelType.TIMESTAMP] + ticks_implicit
        )
        if "GSR_RAW" in sample:
            scale, adc = decode_gsr(sample["GSR_RAW"])
            sample["GSR(Ohm)"] = int(calc_gsr(scale, adc))
            sample["Valid"] = validate_gsr(scale, sample["GSR(Ohm)"])
        print(f"PC {ts:.7f} shimmer {ts_shimmer:.7f} sample {sample}")


async def stream(conn: Shimmer3BleConnection) -> None:
    ch_types = await inquire(conn)
    await start_streaming(conn)
    try:
        await live_stream(conn, ch_types)
    finally:
        await stop_streaming(conn)
