import numpy as np
import pytest

from convert import (
    get_topic_fields,
    map_topic_name,
    stream_topic_records,
)


class FakeField:
    def __init__(self, name: str, type_str: str):
        self.field_name = name
        self.type_str = type_str


class FakeData:
    def __init__(self, name, multi_id, field_data, data):
        self.name = name
        self.multi_id = multi_id
        self.field_data = field_data
        self.data = data


class FakeULog:
    def __init__(self, data_list):
        self.data_list = data_list


def _fields(*pairs):
    return [FakeField(n, t) for n, t in pairs]


# ---- map_topic_name ----

def test_map_topic_name_appends_multi_id():
    assert map_topic_name(FakeData("sensor_accel", 0, [], {})) == "sensor_accel_0"
    assert map_topic_name(FakeData("sensor_accel", 1, [], {})) == "sensor_accel_1"


###### get_topic_fields ######

def test_get_topic_fields_basic():
    d = FakeData(
        "action_request", 0,
        _fields(("timestamp", "uint64_t"), ("action", "uint8_t")),
        {},
    )
    assert get_topic_fields(FakeULog([d])) == {
        "action_request_0": [("timestamp", "uint64_t"), ("action", "uint8_t")],
    }


def test_get_topic_fields_separates_multi_instances():
    a = FakeData("sensor_accel", 0, _fields(("timestamp", "uint64_t")), {})
    b = FakeData("sensor_accel", 1, _fields(("timestamp", "uint64_t")), {})
    assert set(get_topic_fields(FakeULog([a, b]))) == {"sensor_accel_0", "sensor_accel_1"}


# ---- stream_topic_records ----

def _motors_data():
    return FakeData(
        "motors", 0,
        _fields(
            ("timestamp", "uint64_t"),
            ("control[0]", "float"),
            ("control[1]", "float"),
        ),
        {
            "timestamp": np.array([100, 200], dtype=np.uint64),
            "control[0]": np.array([0.5, 1.5], dtype=np.float32),
            "control[1]": np.array([0.25, 1.25], dtype=np.float32),
        },
    )


def test_stream_records_yields_once_per_row():
    rows = list(stream_topic_records(_motors_data(), "motors_0"))
    assert len(rows) == 2


def test_stream_records_emits_topic_name_and_timestamp():
    rows = list(stream_topic_records(_motors_data(), "motors_0"))
    assert [name for name, _, _ in rows] == ["motors_0", "motors_0"]
    assert [t for _, t, _ in rows] == [100, 200]


def test_stream_records_yields_flat_dict_with_indexed_array_keys():
    """Aggregation isn't this function's job — `control[0]` stays as-is."""
    rows = list(stream_topic_records(_motors_data(), "motors_0"))
    _, _, first = rows[0]
    assert set(first.keys()) == {"timestamp", "control[0]", "control[1]"}
    assert first["control[0]"] == pytest.approx(0.5)
    assert first["control[1]"] == pytest.approx(0.25)


def test_stream_records_scalar_passthrough():
    d = FakeData(
        "action", 0,
        _fields(("timestamp", "uint64_t"), ("action", "uint8_t")),
        {
            "timestamp": np.array([10, 20, 30], dtype=np.uint64),
            "action": np.array([1, 2, 3], dtype=np.uint8),
        },
    )
    rows = list(stream_topic_records(d, "action_0"))
    assert [r["action"] for _, _, r in rows] == [1, 2, 3]
