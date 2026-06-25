import logging

import numpy as np
import pytest
from google.protobuf import descriptor_pb2

from proto_gen import (
    LogCodec,
    ParameterChangedCodec,
    TopicCodec,
    _aggregate_fields,
    _PACKAGE,
    build_file_descriptor,
    is_nested_struct_field,
)


_FT = descriptor_pb2.FieldDescriptorProto


# ---------- is_nested_struct_field ----------

def test_is_nested_struct_field_detects_array_of_struct_patterns():
    assert is_nested_struct_field("esc[0].timestamp")
    assert is_nested_struct_field("esc[7]._padding0[3]")
    assert is_nested_struct_field("outer[0].inner")


def test_is_nested_struct_field_ignores_plain_scalars_and_arrays():
    assert not is_nested_struct_field("timestamp")
    assert not is_nested_struct_field("control[0]")      # scalar array, no trailing dot
    assert not is_nested_struct_field("name[5]")         # char array
    assert not is_nested_struct_field("gps.lat")         # dotted but not bracketed


# ---------- _aggregate_fields ----------

def test_aggregate_only_scalars():
    result = _aggregate_fields([("a", "uint8_t"), ("b", "float")])
    assert [f.is_repeated for f in result] == [False, False]
    assert [f.ulog_base for f in result] == ["a", "b"]
    assert [f.proto_type for f in result] == [_FT.TYPE_UINT32, _FT.TYPE_FLOAT]


def test_aggregate_separates_scalars_arrays_and_strings():
    fields = [
        ("timestamp", "uint64_t"),
        ("control[0]", "float"),
        ("control[1]", "float"),
        ("name[0]", "char"),
        ("name[1]", "char"),
        ("name[2]", "char"),
        ("count", "uint16_t"),
    ]
    result = _aggregate_fields(fields)
    # Preserves first-seen order across the input (single-pass aggregation).
    assert [f.proto_name for f in result] == ["timestamp", "control", "name", "count"]

    by_name = {f.proto_name: f for f in result}
    assert not by_name["timestamp"].is_repeated
    assert by_name["control"].is_repeated
    assert by_name["control"].proto_type == _FT.TYPE_FLOAT
    assert by_name["control"].outer_len == 2
    # char[N] -> single string: not repeated, proto type is TYPE_STRING.
    assert not by_name["name"].is_repeated
    assert by_name["name"].proto_type == _FT.TYPE_STRING
    assert by_name["name"].inner_len == 3


def test_aggregate_single_element_array_is_still_array():
    result = _aggregate_fields([("control[0]", "float")])
    assert len(result) == 1
    assert result[0].is_repeated
    assert result[0].proto_name == "control"
    assert result[0].outer_len == 1


def test_aggregate_struct_array_scalar_leaf_becomes_repeated():
    # esc[i].esc_rpm -> one repeated field esc__esc_rpm.
    fields = [
        ("esc[0].esc_rpm", "int32_t"),
        ("esc[1].esc_rpm", "int32_t"),
        ("esc[0].timestamp", "uint64_t"),
        ("esc[1].timestamp", "uint64_t"),
    ]
    by_name = {f.proto_name: f for f in _aggregate_fields(fields)}
    assert set(by_name) == {"esc__esc_rpm", "esc__timestamp"}
    assert by_name["esc__esc_rpm"].is_repeated
    assert by_name["esc__esc_rpm"].proto_type == _FT.TYPE_INT32
    assert by_name["esc__esc_rpm"].outer_len == 2


def test_aggregate_struct_array_char_leaf_becomes_repeated_string():
    # esc[i].name[j] (char) -> repeated string esc__name.
    fields = [
        ("esc[0].name[0]", "char"),
        ("esc[0].name[1]", "char"),
        ("esc[1].name[0]", "char"),
        ("esc[1].name[1]", "char"),
    ]
    result = _aggregate_fields(fields)
    assert len(result) == 1
    field = result[0]
    assert field.proto_name == "esc__name"
    assert field.is_repeated
    assert field.proto_type == _FT.TYPE_STRING
    assert field.outer_len == 2
    assert field.inner_len == 2


def test_aggregate_drops_numeric_array_inside_struct(caplog):
    # esc[i].current[j] (numeric) is a list-of-lists we can't model -> dropped + warned.
    fields = [
        ("esc[0].current[0]", "float"),
        ("esc[0].current[1]", "float"),
        ("esc[0].esc_rpm", "int32_t"),
    ]
    with caplog.at_level(logging.WARNING):
        by_name = {f.proto_name: f for f in _aggregate_fields(fields)}
    assert set(by_name) == {"esc__esc_rpm"}
    assert "esc[0].current[0]" in caplog.text
    assert "esc[0].current[1]" in caplog.text


# ---------- build_file_descriptor ----------

def test_descriptor_package_syntax_and_name():
    fd = build_file_descriptor("vehicle_status_0", [("a", "uint8_t")])
    assert fd.package == _PACKAGE
    assert fd.syntax == "proto3"
    assert fd.name == f"{_PACKAGE.replace('.', '/')}/vehicle_status_0.proto"
    assert len(fd.message_type) == 1
    assert fd.message_type[0].name == "vehicle_status_0"


def test_descriptor_widens_small_integer_types():
    """Protobuf has no uint8/uint16; both widen to uint32. Same for int8/16 -> int32."""
    fd = build_file_descriptor("foo", [
        ("a", "uint8_t"),
        ("b", "uint16_t"),
        ("c", "int8_t"),
        ("d", "int16_t"),
    ])
    types = [f.type for f in fd.message_type[0].field]
    assert types == [
        _FT.TYPE_UINT32, _FT.TYPE_UINT32,
        _FT.TYPE_INT32, _FT.TYPE_INT32,
    ]


def test_descriptor_preserves_full_width_types():
    fd = build_file_descriptor("foo", [
        ("a", "uint64_t"),
        ("b", "int64_t"),
        ("c", "float"),
        ("d", "double"),
        ("e", "bool"),
    ])
    types = [f.type for f in fd.message_type[0].field]
    assert types == [
        _FT.TYPE_UINT64, _FT.TYPE_INT64,
        _FT.TYPE_FLOAT, _FT.TYPE_DOUBLE,
        _FT.TYPE_BOOL,
    ]


def test_descriptor_assigns_sequential_field_numbers():
    fd = build_file_descriptor("foo", [
        ("a", "uint8_t"), ("b", "uint8_t"), ("c", "uint8_t"),
    ])
    assert [f.number for f in fd.message_type[0].field] == [1, 2, 3]


def test_descriptor_arrays_are_repeated_with_element_type():
    fd = build_file_descriptor("foo", [
        ("scalar", "uint8_t"),
        ("arr[0]", "float"),
        ("arr[1]", "float"),
        ("arr[2]", "float"),
    ])
    fields = {f.name: f for f in fd.message_type[0].field}
    assert fields["scalar"].label == _FT.LABEL_OPTIONAL
    assert fields["arr"].label == _FT.LABEL_REPEATED
    assert fields["arr"].type == _FT.TYPE_FLOAT


def test_descriptor_char_array_becomes_single_string_field():
    fd = build_file_descriptor("foo", [
        ("name[0]", "char"),
        ("name[1]", "char"),
        ("name[2]", "char"),
    ])
    fields = fd.message_type[0].field
    assert len(fields) == 1
    assert fields[0].name == "name"
    assert fields[0].type == _FT.TYPE_STRING
    assert fields[0].label == _FT.LABEL_OPTIONAL


def test_descriptor_sanitizes_dots_in_scalar_field_names():
    fd = build_file_descriptor("foo", [("gps.lat", "double")])
    assert fd.message_type[0].field[0].name == "gps_lat"


def test_descriptor_sanitizes_dots_in_array_base_names():
    fd = build_file_descriptor("foo", [
        ("gps.fix[0]", "uint8_t"),
        ("gps.fix[1]", "uint8_t"),
    ])
    fields = fd.message_type[0].field
    assert len(fields) == 1
    assert fields[0].name == "gps_fix"
    assert fields[0].label == _FT.LABEL_REPEATED


def test_descriptor_sanitizes_dots_in_char_array_base_names():
    fd = build_file_descriptor("foo", [
        ("device.name[0]", "char"),
        ("device.name[1]", "char"),
    ])
    fields = fd.message_type[0].field
    assert len(fields) == 1
    assert fields[0].name == "device_name"
    assert fields[0].type == _FT.TYPE_STRING


# ---------- TopicCodec ----------

def test_topic_codec_schema_name_includes_package():
    codec = TopicCodec("vehicle_status_0", [("timestamp", "uint64_t")])
    assert codec.schema_name == f"{_PACKAGE}.vehicle_status_0"


def test_topic_codec_schema_bytes_round_trips_through_descriptor_set():
    codec = TopicCodec("foo", [("timestamp", "uint64_t"), ("count", "uint16_t")])
    fds = descriptor_pb2.FileDescriptorSet()
    fds.ParseFromString(codec.schema_bytes)
    assert len(fds.file) == 1
    assert fds.file[0].package == _PACKAGE
    assert fds.file[0].message_type[0].name == "foo"


def test_topic_codec_encode_round_trip_scalars():
    codec = TopicCodec("foo", [
        ("timestamp", "uint64_t"),
        ("temperature", "float"),
        ("count", "uint16_t"),
    ])
    row = {
        "timestamp": np.uint64(1234567),
        "temperature": np.float32(23.5),
        "count": np.uint16(42),
    }
    parsed = codec._message_class()
    parsed.ParseFromString(codec.encode(row))
    assert parsed.timestamp == 1234567
    assert parsed.temperature == pytest.approx(23.5)
    assert parsed.count == 42


def test_topic_codec_encode_round_trip_array():
    codec = TopicCodec("foo", [
        ("control[0]", "float"),
        ("control[1]", "float"),
        ("control[2]", "float"),
    ])
    row = {
        "control[0]": np.float32(0.5),
        "control[1]": np.float32(1.0),
        "control[2]": np.float32(1.5),
    }
    parsed = codec._message_class()
    parsed.ParseFromString(codec.encode(row))
    assert list(parsed.control) == pytest.approx([0.5, 1.0, 1.5])


def test_topic_codec_encode_char_string_trims_at_null():
    """A char[N] field stops at the first null byte even with garbage after it."""
    codec = TopicCodec("foo", [
        ("name[0]", "char"),
        ("name[1]", "char"),
        ("name[2]", "char"),
        ("name[3]", "char"),
        ("name[4]", "char"),
    ])
    row = {
        "name[0]": np.uint8(ord("h")),
        "name[1]": np.uint8(ord("i")),
        "name[2]": np.uint8(0),       # null terminator
        "name[3]": np.uint8(99),      # garbage past terminator
        "name[4]": np.uint8(100),
    }
    parsed = codec._message_class()
    parsed.ParseFromString(codec.encode(row))
    assert parsed.name == "hi"


def test_topic_codec_encode_char_string_handles_signed_int8():
    """pyulog yields char as signed int8, so bytes >= 128 arrive negative (e.g. the
    uninitialized buffer junk past the terminator); they must mask back to raw bytes."""
    codec = TopicCodec("foo", [
        ("name[0]", "char"),
        ("name[1]", "char"),
        ("name[2]", "char"),
        ("name[3]", "char"),
    ])
    row = {
        "name[0]": np.int8(ord("v")),
        "name[1]": np.int8(ord("s")),
        "name[2]": np.int8(0),         # null terminator
        "name[3]": np.int8(-120),      # byte 136 of buffer junk, signed
    }
    parsed = codec._message_class()
    parsed.ParseFromString(codec.encode(row))
    assert parsed.name == "vs"


def test_topic_codec_encode_struct_array_scalar_leaf():
    """esc[i].esc_rpm round-trips as a repeated scalar esc__esc_rpm, ordered by index."""
    codec = TopicCodec("foo", [
        ("esc[0].esc_rpm", "int32_t"),
        ("esc[1].esc_rpm", "int32_t"),
        ("esc[2].esc_rpm", "int32_t"),
    ])
    row = {
        "esc[0].esc_rpm": np.int32(100),
        "esc[1].esc_rpm": np.int32(200),
        "esc[2].esc_rpm": np.int32(300),
    }
    parsed = codec._message_class()
    parsed.ParseFromString(codec.encode(row))
    assert list(parsed.esc__esc_rpm) == [100, 200, 300]


def test_topic_codec_encode_struct_array_char_leaf():
    """esc[i].name[j] (char) round-trips as a repeated string esc__name, each element
    stitched and trimmed at its own null."""
    codec = TopicCodec("foo", [
        ("esc[0].name[0]", "char"),
        ("esc[0].name[1]", "char"),
        ("esc[1].name[0]", "char"),
        ("esc[1].name[1]", "char"),
    ])
    row = {
        "esc[0].name[0]": np.int8(ord("a")),
        "esc[0].name[1]": np.int8(0),          # null -> first element is "a"
        "esc[1].name[0]": np.int8(ord("b")),
        "esc[1].name[1]": np.int8(ord("c")),   # no null -> second element is "bc"
    }
    parsed = codec._message_class()
    parsed.ParseFromString(codec.encode(row))
    assert list(parsed.esc__name) == ["a", "bc"]


def test_topic_codec_proto3_skips_default_values():
    """proto3 doesn't emit zero-valued scalar fields — the whole tag is absent."""
    codec = TopicCodec("foo", [("count", "uint32_t")])
    assert len(codec.encode({"count": np.uint32(0)})) == 0
    assert len(codec.encode({"count": np.uint32(1)})) > 0


def test_topic_codec_round_trip_with_dotted_scalar_field_name():
    """Lookup key keeps the dot; protobuf attr replaces it with underscore."""
    codec = TopicCodec("foo", [("gps.lat", "double")])
    parsed = codec._message_class()
    parsed.ParseFromString(codec.encode({"gps.lat": 37.5}))
    assert parsed.gps_lat == pytest.approx(37.5)


def test_topic_codec_round_trip_with_dotted_array_base():
    codec = TopicCodec("foo", [
        ("gps.fix[0]", "uint8_t"),
        ("gps.fix[1]", "uint8_t"),
    ])
    parsed = codec._message_class()
    parsed.ParseFromString(codec.encode({
        "gps.fix[0]": np.uint8(1),
        "gps.fix[1]": np.uint8(2),
    }))
    assert list(parsed.gps_fix) == [1, 2]


def test_topic_codec_round_trip_with_dotted_string_base():
    codec = TopicCodec("foo", [
        ("device.name[0]", "char"),
        ("device.name[1]", "char"),
        ("device.name[2]", "char"),
    ])
    parsed = codec._message_class()
    parsed.ParseFromString(codec.encode({
        "device.name[0]": np.uint8(ord("h")),
        "device.name[1]": np.uint8(ord("i")),
        "device.name[2]": np.uint8(0),
    }))
    assert parsed.device_name == "hi"


# ---------- LogCodec ----------

def test_log_codec_schema_name_is_foxglove_log():
    assert LogCodec().schema_name == "foxglove.Log"


def test_log_codec_schema_bytes_parses_as_descriptor_set():
    fds = descriptor_pb2.FileDescriptorSet()
    fds.ParseFromString(LogCodec().schema_bytes)
    # foxglove.Log imports google.protobuf.Timestamp, so the set has >=2 files.
    file_names = [f.name for f in fds.file]
    assert any("Log" in n for n in file_names)


def test_log_codec_encode_returns_nonempty_bytes():
    from foxglove.messages import LogLevel
    encoded = LogCodec().encode({
        "timestamp": {"sec": 100, "nsec": 200},
        "level": LogLevel.Info,
        "message": "hello",
        "name": "commander",
    })
    assert isinstance(encoded, bytes)
    assert len(encoded) > 0


def test_log_codec_encode_is_deterministic():
    from foxglove.messages import LogLevel
    payload = {
        "timestamp": {"sec": 100, "nsec": 200},
        "level": LogLevel.Info,
        "message": "hello",
        "name": "",
    }
    codec = LogCodec()
    assert codec.encode(payload) == codec.encode(payload)


def test_log_codec_encode_varies_with_input():
    from foxglove.messages import LogLevel
    base = {
        "timestamp": {"sec": 0, "nsec": 0},
        "level": LogLevel.Info,
        "message": "a",
        "name": "",
    }
    codec = LogCodec()
    assert codec.encode(base) != codec.encode({**base, "message": "b"})


def test_log_codec_encode_omits_optional_file_and_line():
    """Payloads without `file`/`line` keys should still encode (defaults applied)."""
    from foxglove.messages import LogLevel
    LogCodec().encode({
        "timestamp": {"sec": 1, "nsec": 0},
        "level": LogLevel.Info,
        "message": "minimal",
    })  # shouldn't raise


# ---------- ParameterChangedCodec ----------

def test_parameter_changed_codec_schema_name():
    assert ParameterChangedCodec().schema_name == f"{_PACKAGE}.ParameterChanged"


def test_parameter_changed_codec_schema_bytes_carries_oneof():
    fds = descriptor_pb2.FileDescriptorSet()
    fds.ParseFromString(ParameterChangedCodec().schema_bytes)
    assert len(fds.file) == 1
    msg = fds.file[0].message_type[0]
    assert msg.name == "ParameterChanged"
    assert [o.name for o in msg.oneof_decl] == ["value"]
    by_name = {f.name: f for f in msg.field}
    assert by_name["int_value"].oneof_index == 0
    assert by_name["float_value"].oneof_index == 0


def test_parameter_changed_codec_encode_int_value_round_trip():
    codec = ParameterChangedCodec()
    parsed = codec._message_class()
    parsed.ParseFromString(codec.encode({
        "timestamp": 100,
        "name": "MC_PITCH_P",
        "int_value": 42,
    }))
    assert parsed.timestamp == 100
    assert parsed.name == "MC_PITCH_P"
    assert parsed.WhichOneof("value") == "int_value"
    assert parsed.int_value == 42


def test_parameter_changed_codec_encode_float_value_round_trip():
    codec = ParameterChangedCodec()
    parsed = codec._message_class()
    parsed.ParseFromString(codec.encode({
        "timestamp": 200,
        "name": "MC_PITCH_P",
        "float_value": 1.5,
    }))
    assert parsed.timestamp == 200
    assert parsed.name == "MC_PITCH_P"
    assert parsed.WhichOneof("value") == "float_value"
    assert parsed.float_value == pytest.approx(1.5)


def test_parameter_changed_codec_int_branch_excludes_float_branch():
    """Setting int_value should leave float_value unset and absent from the oneof."""
    codec = ParameterChangedCodec()
    parsed = codec._message_class()
    parsed.ParseFromString(codec.encode({
        "timestamp": 1,
        "name": "foo",
        "int_value": 7,
    }))
    assert parsed.WhichOneof("value") == "int_value"
    assert parsed.float_value == 0.0


def test_parameter_changed_codec_encode_omits_value_when_neither_provided():
    """Payloads with neither int_value nor float_value still encode (oneof unset)."""
    codec = ParameterChangedCodec()
    parsed = codec._message_class()
    parsed.ParseFromString(codec.encode({
        "timestamp": 5,
        "name": "foo",
    }))
    assert parsed.timestamp == 5
    assert parsed.name == "foo"
    assert parsed.WhichOneof("value") is None
