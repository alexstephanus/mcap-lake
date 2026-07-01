import heapq
import math
import os
import tempfile
import time
from dataclasses import dataclass
from typing import Any, Iterator

from foxglove.messages import LogLevel
from mcap.writer import CompressionType, Writer
from pyulog import core, ULog

from proto_gen import (
    ned_to_enu,
    LogCodec,
    ParameterChangedCodec,
    ProtobufCodec,
    TopicCodec,
    TrajectoryCodec,
)
from storage import ObjectStore

_CHUNK_SIZE = 4 * 1024 * 1024  # 4 MB
_COMPRESSION_TYPE = CompressionType.ZSTD
_LOGS_TOPIC_NAME = "logged_messages"
_PARAMETER_CHANGES_TOPIC_NAME = "parameter_changes"
_TRAJECTORY_TOPIC_NAME = "vehicle_trajectory_foxglove"
_BYTES_PER_MB = 1024 * 1024


def _mbps(mb: float, seconds: float) -> float:
    return mb / seconds if seconds > 0 else 0.0


# Foxglove uses a different log-level system than
# PX4, this just centralizes that mapping
_PX4_TO_FOXGLOVE_LOG_LEVEL = {
    'EMERGENCY': LogLevel.Fatal,
    'ALERT':     LogLevel.Fatal,
    'CRITICAL':  LogLevel.Fatal,
    'ERROR':     LogLevel.Error,
    'WARNING':   LogLevel.Warning,
    'NOTICE':    LogLevel.Info,
    'INFO':      LogLevel.Info,
    'DEBUG':     LogLevel.Debug,
}

@dataclass
class McapChannel:
    """
    Bundles a codec together with the channel_id that
    the mcap-writer gives it.  We can't know the channel_id
    when we construct the codec, so this just makes sure
    they're tightly-coupled _once_ we get it.
    """
    channel_id: int
    codec: ProtobufCodec

@dataclass
class FileConvertResult:
    outcome: str
    ulog_bytes: int = 0
    mcap_bytes: int = 0
    read_seconds: float = 0.0       # download .ulg from object store
    transform_seconds: float = 0.0  # ulg -> mcap conversion
    write_seconds: float = 0.0      # upload .mcap to object store

    @property
    def total_seconds(self) -> float:
        return self.read_seconds + self.transform_seconds + self.write_seconds

    def log_conversion(self, file_num: int, total_files: int, route: str):
        if self.outcome == "skipped":
            print(f"Skipped {file_num} / {total_files}: {route}")
            return
        ulog_mb = self.ulog_bytes / _BYTES_PER_MB
        mcap_mb = self.mcap_bytes / _BYTES_PER_MB
        ratio = self.mcap_bytes / self.ulog_bytes
        print(
            f"converted {file_num} / {total_files}: {route} "
            f"({ulog_mb:.1f} MB ulg -> {mcap_mb:.1f} MB mcap, {ratio:.2f}x, total {self.total_seconds:.1f}s | "
            f"read {_mbps(ulog_mb, self.read_seconds):.1f} MB/s, "
            f"transform {_mbps(ulog_mb, self.transform_seconds):.1f} MB/s, "
            f"write {_mbps(mcap_mb, self.write_seconds):.1f} MB/s)"
        )

def map_topic_name(topic: core.ULog.Data) -> str:
    return f"{topic.name}_{topic.multi_id}"

def get_topic_fields(ulog: ULog) -> dict[str, list[tuple[str, str]]]:
    """Return {topic_name: [(field_name, type_str), ...]} for each topic.
    topic names are suffixed with the multi_id
    """
    return {
        map_topic_name(topic): [(f.field_name, f.type_str) for f in topic.field_data]
        for topic in ulog.data_list
    }


def print_topic_fields(ulog: ULog) -> None:
    for topic, fields in get_topic_fields(ulog).items():
        print(f"{topic}")
        for name, type_str in fields:
            print(f"  {type_str:12s} {name}")


def stream_topic_records(d: core.ULog.Data, topic_name: str) -> Iterator[tuple[str, int, dict[str, Any]]]:
    """Yield (topic_name, timestamp_us, flat_row_dict) per sample.

    pyulog stores values as numpy arrays per field — we walk by index and pull
    the i-th value from each. Array re-aggregation and char[N] -> string
    stitching are the codec's responsibility downstream.
    """
    record_count = len(d.data["timestamp"])
    fields = list(d.data.keys())
    for i in range(record_count):
        yield (
            topic_name,
            int(d.data["timestamp"][i]),
            {f: d.data[f][i] for f in fields},
        )

def _micros_to_foxglove_time(micros: int) -> dict[str, int]:
    return {
        "sec": micros // 1_000_000,
        "nsec": (micros % 1_000_000) * 1000
    }

def stream_logged_messages(ulog: ULog) -> Iterator[tuple[str, int, dict[str, Any]]]:
    for log_record in ulog.logged_messages:
        formatted_record = {
            "message": log_record.message,
            "level": _PX4_TO_FOXGLOVE_LOG_LEVEL.get(log_record.log_level_str(), LogLevel.Unknown),
            "name": "",
            "timestamp": _micros_to_foxglove_time(log_record.timestamp)
        }
        yield ("logged_messages", log_record.timestamp, formatted_record)

def stream_logged_messages_tagged(ulog: ULog) -> Iterator[tuple[str, int, dict[str, Any]]]:
    logs_by_tag = list(ulog.logged_messages_tagged.items())
    stitchable_log_streams = [[(tag, log) for log in logs] for (tag, logs) in logs_by_tag]
    for stream in stitchable_log_streams:
        stream.sort(key=lambda item: item[1].timestamp)

    for (tag, log_record) in heapq.merge(
        *stitchable_log_streams,
        key=lambda item: item[1].timestamp,
    ):
        formatted_record = {
            "message": log_record.message,
            "level": _PX4_TO_FOXGLOVE_LOG_LEVEL.get(log_record.log_level_str(), LogLevel.Unknown),
            "name": str(tag),
            "timestamp": _micros_to_foxglove_time(log_record.timestamp),
        }
        yield ("logged_messages", log_record.timestamp, formatted_record)

def stream_parameter_changes(ulog: ULog) -> Iterator[tuple[str, int, dict[str, Any]]]:
    for timestamp, name, value in ulog.changed_parameters:
        ts = int(timestamp)
        payload: dict[str, Any] = {"timestamp": ts, "name": name}
        if isinstance(value, int):
            payload["int_value"] = value
        else:
            payload["float_value"] = float(value)
        yield (_PARAMETER_CHANGES_TOPIC_NAME, ts, payload)


def stream_trajectory(ulog: ULog) -> Iterator[tuple[str, int, dict[str, Any]]]:
    """Yield a single SceneUpdate carrying the whole flight track as one polyline.

    Reads vehicle_local_position (instance 0), collates all position points into
    a single Point3 list, and passes that list to the TrajectoryCodec.
    If vehicle_local_position is missing (which it shouldn't be for the firmware version
    that our Foxglove dashboard actually cares about), we simply skip
    """
    try:
        local_position = ulog.get_dataset("vehicle_local_position", multi_instance=0)
    except (KeyError, IndexError, ValueError):
        return

    timestamps = local_position.data["timestamp"]
    xs = local_position.data["x"]
    ys = local_position.data["y"]
    zs = local_position.data["z"]

    points = []
    first_ts: int | None = None
    for i in range(len(timestamps)):
        x, y, z = float(xs[i]), float(ys[i]), float(zs[i])
        if math.isnan(x) or math.isnan(y) or math.isnan(z):
            continue
        point = ned_to_enu(x, y, z)
        if point is None:
            continue
        points.append(point)
        if first_ts is None:
            first_ts = int(timestamps[i])

    if not points or first_ts is None:
        return

    yield (
        _TRAJECTORY_TOPIC_NAME,
        first_ts,
        {
            "points": points,
            "timestamp": _micros_to_foxglove_time(first_ts),
        },
    )


def _register_channel(writer: Writer, topic_name: str, codec: ProtobufCodec) -> McapChannel:
    schema_id = writer.register_schema(
        name=codec.schema_name,
        encoding="protobuf",
        data=codec.schema_bytes,
    )
    channel_id = writer.register_channel(
        schema_id=schema_id,
        topic=topic_name,
        message_encoding="protobuf",
    )
    return McapChannel(channel_id=channel_id, codec=codec)

def convert_ulog_to_mcap(input_path: str, output_path: str):
    if not input_path.endswith(".ulg"):
        raise ValueError(f"Invalid input filepath {input_path}: no `.ulg` ending")
    if not output_path.endswith(".mcap"):
        raise ValueError(f"Invalid output filepath {output_path}: no `.mcap` ending")

    ulog = ULog(input_path)

    # ver_sw is the PX4 firmware git hash (the same field the downloader filters on with --git-hash)
    info = ulog.msg_info_dict
    print(f"Firmware version: {info.get('ver_sw', 'unknown')}")

    with open(output_path, "wb") as fp:
        mcap_writer = Writer(
            fp,
            chunk_size=_CHUNK_SIZE,
            compression=_COMPRESSION_TYPE,
        )
        mcap_writer.start()

        mcap_writer.add_metadata(
            "info",
            {str(k): str(v) for k, v in ulog.msg_info_dict.items()},
        )
        mcap_writer.add_metadata(
            "initial_parameters",
            {str(k): str(v) for k, v in ulog.initial_parameters.items()},
        )
        mcap_writer.add_metadata(
            "default_parameters",
            {str(k): str(v) for k, v in ulog._default_parameters.items()},
        )

        channels: dict[str, McapChannel] = {}
        codecs_by_type: dict[str, TopicCodec] = {}
        schema_ids_by_type: dict[str, int] = {}

        for topic in ulog.data_list:
            channel_topic = map_topic_name(topic)
            type_name = topic.name
            field_types = [(f.field_name, f.type_str) for f in topic.field_data]
            if type_name not in codecs_by_type:
                codec = TopicCodec(type_name, field_types)
                codecs_by_type[type_name] = codec
                schema_ids_by_type[type_name] = mcap_writer.register_schema(
                    name=codec.schema_name,
                    encoding="protobuf",
                    data=codec.schema_bytes,
                )

            channel_id = mcap_writer.register_channel(
                schema_id=schema_ids_by_type[type_name],
                topic=channel_topic,
                message_encoding="protobuf",
            )
            channels[channel_topic] = McapChannel(
                channel_id=channel_id, codec=codecs_by_type[type_name]
            )

        channels[_LOGS_TOPIC_NAME] = _register_channel(
            mcap_writer,
            _LOGS_TOPIC_NAME,
            LogCodec(),
        )

        channels[_PARAMETER_CHANGES_TOPIC_NAME] = _register_channel(
            mcap_writer,
            _PARAMETER_CHANGES_TOPIC_NAME,
            ParameterChangedCodec(),
        )

        channels[_TRAJECTORY_TOPIC_NAME] = _register_channel(
            mcap_writer,
            _TRAJECTORY_TOPIC_NAME,
            TrajectoryCodec(),
        )

        stitched_message_stream = heapq.merge(
            *[stream_topic_records(topic, map_topic_name(topic)) for topic in ulog.data_list],
            stream_logged_messages(ulog),
            stream_logged_messages_tagged(ulog),
            stream_parameter_changes(ulog),
            stream_trajectory(ulog),
            key=lambda message: message[1],
        )

        for channel_name, timestamp_micros, payload in stitched_message_stream:
            message_channel = channels[channel_name]
            mcap_writer.add_message(
                channel_id=message_channel.channel_id,
                data=message_channel.codec.encode(payload),
                log_time=timestamp_micros * 1000,
                publish_time=timestamp_micros * 1000,
            )

        mcap_writer.finish()

def convert_and_measure(ulg_path: str, mcap_path: str) -> FileConvertResult:
    """Convert a local .ulg to a local .mcap, timing the conversion and measuring
    both file sizes. Returns a "converted" FileConvertResult; skip logic (and any
    object transfer) is the caller's concern.
    """
    start = time.perf_counter()
    convert_ulog_to_mcap(ulg_path, mcap_path)
    transform_seconds = time.perf_counter() - start
    ulog_bytes = os.path.getsize(ulg_path)
    mcap_bytes = os.path.getsize(mcap_path)
    return FileConvertResult(
        "converted", ulog_bytes, mcap_bytes, transform_seconds=transform_seconds
    )

def convert_object(
    store: ObjectStore,
    source_bucket: str,
    dest_bucket: str,
    source_key: str,
    dest_key: str,
    overwrite: bool,
) -> FileConvertResult:
    """Convert a single stored object: skip if the destination already exists, else
    download the .ulg, convert + measure it, and upload the resulting .mcap."""
    if not overwrite and store.exists(dest_bucket, dest_key):
        return FileConvertResult("skipped")

    with tempfile.TemporaryDirectory() as tmp_dir:
        local_ulg = os.path.join(tmp_dir, "input.ulg")
        local_mcap = os.path.join(tmp_dir, "output.mcap")

        read_start = time.perf_counter()
        store.download(source_bucket, source_key, local_ulg)
        read_seconds = time.perf_counter() - read_start

        result = convert_and_measure(local_ulg, local_mcap)

        write_start = time.perf_counter()
        store.upload(local_mcap, dest_bucket, dest_key)
        write_seconds = time.perf_counter() - write_start

    result.read_seconds = read_seconds
    result.write_seconds = write_seconds
    return result

def convert_filename_to_mcap(ulg_filename: str):
    if not ulg_filename.endswith(".ulg"):
        raise ValueError(f"Input filename {ulg_filename} does not end with '.ulg'")
    stripped_filename = ulg_filename.removesuffix(".ulg")
    return f"{stripped_filename}.mcap"
