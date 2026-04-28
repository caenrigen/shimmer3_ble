import asyncio
import time

import click
from pyshimmer.dev import channels as dch

import shimmer3_ble.shimmer3 as s3


@click.command()
@click.option(
    "--address",
    "-a",
    default=None,
    help="BLE address, e.g. AA:BB:CC:DD:EE:FF",
)
@click.option(
    "--name",
    "-n",
    default=None,
    help="BLE local name, e.g. Shimmer3R-EEFF-BLE",
)
@click.option(
    "--sampling-rate",
    default=5.12,
    show_default=True,
    help="Sampling rate in Hz",
)
@click.option(
    "--gsr-range",
    default=s3.GSR_CONFIG_220k_680k,
    type=click.IntRange(s3.GSR_CONFIG_8k_63k, s3.GSR_CONFIG_AUTORANGE),
    show_default=True,
    help="GSR range config value: 0, 1, 2, 3, or 4 for autorange",
)
@click.option(
    "--scan-timeout",
    default=s3.DEFAULT_SCAN_TIMEOUT,
    show_default=True,
    help="BLE scan timeout in seconds when resolving by name",
)
@click.option(
    "--read-timeout",
    default=s3.DEFAULT_IO_TIMEOUT,
    show_default=True,
    help="Command/data read timeout in seconds",
)
def main(
    address: str | None,
    name: str | None,
    sampling_rate: float,
    gsr_range: int,
    scan_timeout: float,
    read_timeout: float,
) -> None:
    try:
        asyncio.run(
            async_main(
                address=address,
                name=name,
                sampling_rate=sampling_rate,
                gsr_range=gsr_range,
                scan_timeout=scan_timeout,
                read_timeout=read_timeout,
            )
        )
    except KeyboardInterrupt:
        pass


async def async_main(
    *,
    address: str | None,
    name: str | None,
    sampling_rate: float,
    gsr_range: int,
    scan_timeout: float,
    read_timeout: float,
) -> None:
    async with await s3.connect(
        address=address,
        name=name,
        scan_timeout=scan_timeout,
        read_timeout=read_timeout,
    ) as conn:
        # For reproducibility, start from the same setting every time.
        await s3.reset_to_default_config(conn)

        # Set the real-world clock to the current time.
        await s3.set_real_world_clock(conn, time.time())
        # _ = await s3.get_real_world_clock(conn)

        await s3.set_sampling_rate(conn, rate_hz=sampling_rate)

        # Sensors might induce noise between each other, keep it to the strict minimum.
        sensors = [
            dch.ESensorGroup.INT_CH_A1,  # PPG
            dch.ESensorGroup.GSR,
            # dch.ESensorGroup.ACCEL_LN,
            # dch.ESensorGroup.GYRO,
            # dch.ESensorGroup.MAG_REG,
            # dch.ESensorGroup.TEMP,
            # dch.ESensorGroup.PRESSURE,
            # dch.ESensorGroup.BATTERY,
        ]
        await s3.set_sensors(conn, sensors)
        await s3.set_gsr_range(conn, gsr_range)

        # For PPG we need to enable the internal expansion power.
        await s3.set_internal_exp_power(conn, True)
        # _ = await s3.get_internal_exp_power(conn)

        await s3.stream(conn)


if __name__ == "__main__":
    main()
