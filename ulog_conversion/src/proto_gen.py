"""Generate protobuf descriptors and per-topic encoders for PX4 ULog topics.

Each topic gets a FileDescriptorProto with one message under the a global
package. We un-flatten name[k] fields into protobuf
repeated fields (`char[N]` fields become strings)
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
import logging
import re
from typing import Optional

from google.protobuf import descriptor_pb2, descriptor_pool, message_factory
from foxglove.messages import (
    Color,
    Duration,
    LinePrimitive,
    LinePrimitiveLineType,
    Log,
    Point3,
    Pose,
    Quaternion,
    SceneEntity,
    SceneUpdate,
    Timestamp,
    Vector3,
)

_log = logging.getLogger(__name__)

# I don't _love_ regexes, but it's either this or a bunch of equally-horrifying
# suffix-checking and string-splitting
_ARRAY_REGEX = re.compile(r"^(?P<base>.*)\[(?P<idx>\d+)\]$")
_NESTED_FIELD_REGEX = re.compile(r"\[\d+\]\.")
_STRUCT_ARRAY_REGEX = re.compile(r"^(?P<base>.+?)\[(?P<idx>\d+)\]\.(?P<leaf>.+)$")

# The Foxglove PX4 extension matches raw topic names,
# e.g. `vehicle_local_position`.  Using a package name
# (like 'px4.ulog') breaks that assumption and thus the extension. 
# Since this pipeline only works on PX4 ulogs and processes them individually,
# there's no risk of a name collision
_PACKAGE = ""


def _format_proto_message_name(name: str) -> str:
    return f"{_PACKAGE}.{name}" if _PACKAGE else name


def _format_proto_path(name: str) -> str:
    return f"{_PACKAGE.replace('.', '/')}/{name}.proto" if _PACKAGE else f"{name}.proto"


def is_nested_struct_field(field_name: str) -> bool:
    """True for array-of-struct fields (e.g. `esc[0].timestamp`).
    We don't have support for these fields right now
    """
    return bool(_NESTED_FIELD_REGEX.search(field_name))


_FieldType = descriptor_pb2.FieldDescriptorProto

_ULOG_TO_PROTO_TYPE = {
    "int8_t":   _FieldType.TYPE_INT32,
    "int16_t":  _FieldType.TYPE_INT32,
    "int32_t":  _FieldType.TYPE_INT32,
    "int64_t":  _FieldType.TYPE_INT64,
    "uint8_t":  _FieldType.TYPE_UINT32,
    "uint16_t": _FieldType.TYPE_UINT32,
    "uint32_t": _FieldType.TYPE_UINT32,
    "uint64_t": _FieldType.TYPE_UINT64,
    "float":    _FieldType.TYPE_FLOAT,
    "double":   _FieldType.TYPE_DOUBLE,
    "bool":     _FieldType.TYPE_BOOL,
    # 'char' gets special handling, but still needs to be in here
    "char":     _FieldType.TYPE_STRING,
}

# Think of this as a trait, not an inheritance hierarchy
class ProtobufCodec(ABC):

    @property
    @abstractmethod
    def schema_name(self) -> str:
        pass

    @property
    @abstractmethod
    def schema_bytes(self) -> bytes:
        pass

    @abstractmethod
    def encode(self, record: dict) -> bytes:
        pass


@dataclass
class UlogField:
    """One proto field, with everything needed to both declare it in a
    descriptor and pull its value back out of a pyulog row dict.

    `leaf` distinguishes a struct-array field (`esc[i].rpm`, packed into a repeated
    `esc__rpm`) from a flat field (`leaf` is None). A char[N] leaf becomes a repeated
    string; other arrays-inside-structs are unsupported and dropped in _aggregate_fields.
    """
    ulog_base: str           # leftmost row-dict base ("esc", "control", "gps.fix")
    proto_type: int          # FieldDescriptorProto type enum
    is_repeated: bool = False
    leaf: str | None = None  # struct-array leaf segment; None for flat/scalar fields
    outer_len: int = 1       # repeated dimension length (struct array or flat array)
    inner_len: int = 1       # char[N] length, for string stitching

    @property
    def proto_name(self) -> str:
        """PX4 uses periods for struct fields (`esc.timestamp`).  This is illegal in proto
        descriptor names, so periods become underscores. Struct-array leaves are joined with a
        double-underscore (`esc__rpm`) to flag that the field was unpacked from a struct array.
        """
        name = self.ulog_base if self.leaf is None else f"{self.ulog_base}__{self.leaf}"
        return name.replace('.', '_')


def _aggregate_fields(fields: list[tuple[str, str]]) -> list[UlogField]:
    """Turn the (name, type_str) pairs pyulog gives us into UlogFields, un-flattening
    arrays. ULogs flatten plain arrays (`control[0]`, `control[1]`, ...) and struct
    arrays (`esc[0].rpm`, `esc[1].rpm`, ...); we repack each into one repeated field —
    or a string, for char arrays. A char[N] leaf of a struct array becomes a repeated
    string. Any other array inside a struct (e.g. a numeric `esc[i].current[j]`, or nesting
    deeper than one struct level) is a list-of-lists we can't model without nested
    message support, so it's dropped with a warning.
    """

    proto_fields: dict[tuple[str, str | None], UlogField] = {}

    for name, type_str in fields:
        struct = _STRUCT_ARRAY_REGEX.match(name)
        if struct:
            base, idx, leaf = struct["base"], int(struct["idx"]), struct["leaf"]
            if "." in leaf:
                _log.warning("dropping %r: nested deeper than one struct level", name)
                continue
            leaf_array = _ARRAY_REGEX.match(leaf)
            if leaf_array:
                if type_str != "char":
                    if "padding" not in leaf:
                        _log.warning("dropping %r: numeric array inside a struct array "
                                     "(list-of-lists, needs nested messages)", name)
                    continue
                leaf_base, inner_idx = leaf_array.group(1), int(leaf_array.group(2))
                key = (base, leaf_base)
                if key in proto_fields:
                    proto_fields[key].outer_len = max(proto_fields[key].outer_len, idx + 1)
                    proto_fields[key].inner_len = max(proto_fields[key].inner_len, inner_idx + 1)
                else:
                    proto_fields[key] = UlogField(
                        ulog_base=base, leaf=leaf_base,
                        proto_type=_FieldType.TYPE_STRING,
                        is_repeated=True, outer_len=idx + 1, inner_len=inner_idx + 1,
                    )
            else:
                key = (base, leaf)
                if key in proto_fields:
                    proto_fields[key].outer_len = max(proto_fields[key].outer_len, idx + 1)
                else:
                    proto_fields[key] = UlogField(
                        ulog_base=base, leaf=leaf,
                        proto_type=_ULOG_TO_PROTO_TYPE[type_str],
                        is_repeated=True, outer_len=idx + 1,
                    )
            continue

        flat = _ARRAY_REGEX.match(name)
        if not flat:
            proto_fields[(name, None)] = UlogField(
                ulog_base=name, proto_type=_ULOG_TO_PROTO_TYPE[type_str],
            )
            continue

        base, idx = flat.group(1), int(flat.group(2))
        key = (base, None)
        if key in proto_fields:
            if type_str == "char":
                proto_fields[key].inner_len = max(proto_fields[key].inner_len, idx + 1)
            else:
                proto_fields[key].outer_len = max(proto_fields[key].outer_len, idx + 1)
        elif type_str == "char":
            proto_fields[key] = UlogField(
                ulog_base=base, proto_type=_FieldType.TYPE_STRING, inner_len=idx + 1,
            )
        else:
            proto_fields[key] = UlogField(
                ulog_base=base, proto_type=_ULOG_TO_PROTO_TYPE[type_str],
                is_repeated=True, outer_len=idx + 1,
            )

    return list(proto_fields.values())


def build_file_descriptor(topic_name: str, fields: list[tuple[str, str]]) -> descriptor_pb2.FileDescriptorProto:
    """
    Builds the proto descriptor for serializing one channel's worth of events.
    This descriptor generation happens for every file we convert, rather than
    using pre-defined event schemas.  So, we're eating the descriptor-creation cost
    every time we convert a .ulg (and again on the mcap ingestion side),
    and lose statically-compiled parsers on the ingesiton side.
    But it does mean that the mcap-ingest side isn't tightly-coupled to
    existing definitions.
    """
    return _file_descriptor(
        _format_proto_path(topic_name),
        topic_name,
        _aggregate_fields(fields),
    )


def _file_descriptor(
    file_name: str, message_name: str, ulog_fields: list[UlogField]
) -> descriptor_pb2.FileDescriptorProto:
    """Assemble a single-message FileDescriptorProto from a UlogField list."""
    descriptor = descriptor_pb2.FileDescriptorProto()
    descriptor.name = file_name
    descriptor.package = _PACKAGE
    descriptor.syntax = "proto3"

    msg = descriptor.message_type.add()
    msg.name = message_name

    for number, ulog_field in enumerate(ulog_fields, start=1):
        f = msg.field.add()
        f.name = ulog_field.proto_name
        f.number = number
        f.type = ulog_field.proto_type
        f.label = (
            _FieldType.LABEL_REPEATED
            if ulog_field.is_repeated
            else _FieldType.LABEL_OPTIONAL
        )

    return descriptor


def _numpy_to_python(v):
    """numpy scalar -> Python native, so protobuf field assignment accepts it."""
    return v.item() if hasattr(v, "item") else v


def _decode_char_string(record: dict, base: str, length: int) -> str:
    """Stitch a char[length] array (keys `base[0..length]`) into a string. pyulog stores
    char as signed int8, so mask each element to a raw byte (& 0xFF) and trim at the first
    null (anything after it is uninitialized buffer padding)."""
    chars = bytes(int(record[f"{base}[{i}]"]) & 0xFF for i in range(length))
    return chars.split(b"\x00", 1)[0].decode("utf-8", errors="replace")


class TopicCodec(ProtobufCodec):
    """Schema + encoder for one PX4 message type, shared across multi_id instances."""

    def __init__(self, type_name: str, fields: list[tuple[str, str]]):
        self.type_name = type_name
        self._fields = _aggregate_fields(fields)
        self.file_descriptor = _file_descriptor(
            _format_proto_path(type_name),
            type_name,
            self._fields,
        )

        pool = descriptor_pool.DescriptorPool()
        pool.Add(self.file_descriptor)
        msg_descriptor = pool.FindMessageTypeByName(_format_proto_message_name(type_name))
        self._message_class = message_factory.GetMessageClass(msg_descriptor)

    @property
    def schema_name(self) -> str:
        return _format_proto_message_name(self.type_name)

    @property
    def schema_bytes(self) -> bytes:
        """Wraps our descriptor in a FileDescriptor set, which is what MCAP expects.
        """
        file_descriptor_set = descriptor_pb2.FileDescriptorSet()
        file_descriptor_set.file.append(self.file_descriptor)
        return file_descriptor_set.SerializeToString()

    def encode(self, record: dict) -> bytes:
        """Encode a row dict (extracted via pyulog) to protobuf bytes"""
        msg = self._message_class()

        for f in self._fields:
            if f.is_repeated:
                # Can't setattr a repeated field; extend the accessor instead.
                getattr(msg, f.proto_name).extend(self._repeated_values(record, f))
            elif f.proto_type == _FieldType.TYPE_STRING:
                setattr(msg, f.proto_name, _decode_char_string(record, f.ulog_base, f.inner_len))
            else:
                setattr(msg, f.proto_name, _numpy_to_python(record[f.ulog_base]))

        return msg.SerializeToString()

    @staticmethod
    def _repeated_values(record: dict, f: UlogField):
        """Yield each element of a repeated field. A struct-array leaf is keyed
        `base[i].leaf`, a plain array `base[i]`; a char[N] leaf is stitched per element."""
        for i in range(f.outer_len):
            prefix = f"{f.ulog_base}[{i}]"
            if f.leaf is not None:
                prefix = f"{prefix}.{f.leaf}"
            if f.proto_type == _FieldType.TYPE_STRING:
                yield _decode_char_string(record, prefix, f.inner_len)
            else:
                yield _numpy_to_python(record[prefix])


class ParameterChangedCodec(ProtobufCodec):
    """Fixed-schema codec for ParameterChanged events.

    Unlike topic descriptors, this isn't derived from ULog fields: it's a
    hand-rolled schema with a `value` oneof, so it builds its own descriptor.
    """

    _NAME = "ParameterChanged"
    # (field_name, proto_type, oneof_index or None), in field-number order.
    _FIELDS = [
        ("timestamp",   _FieldType.TYPE_UINT64,  None),
        ("name",        _FieldType.TYPE_STRING,  None),
        ("int_value",   _FieldType.TYPE_INT64,   0),
        ("float_value", _FieldType.TYPE_DOUBLE,  0),
    ]
    _ONEOFS = ["value"]

    def __init__(self):
        self.file_descriptor = self._build_descriptor()
        pool = descriptor_pool.DescriptorPool()
        pool.Add(self.file_descriptor)
        msg_descriptor = pool.FindMessageTypeByName(_format_proto_message_name(self._NAME))
        self._message_class = message_factory.GetMessageClass(msg_descriptor)

    @classmethod
    def _build_descriptor(cls) -> descriptor_pb2.FileDescriptorProto:
        fd = descriptor_pb2.FileDescriptorProto()
        fd.name = _format_proto_path("parameter_changed")
        fd.package = _PACKAGE
        fd.syntax = "proto3"

        msg = fd.message_type.add()
        msg.name = cls._NAME
        for oneof_name in cls._ONEOFS:
            msg.oneof_decl.add().name = oneof_name

        for number, (name, proto_type, oneof_index) in enumerate(cls._FIELDS, start=1):
            f = msg.field.add()
            f.name = name
            f.number = number
            f.type = proto_type
            f.label = _FieldType.LABEL_OPTIONAL
            if oneof_index is not None:
                f.oneof_index = oneof_index

        return fd

    @property
    def schema_name(self) -> str:
        return _format_proto_message_name(self._NAME)

    @property
    def schema_bytes(self) -> bytes:
        fds = descriptor_pb2.FileDescriptorSet()
        fds.file.append(self.file_descriptor)
        return fds.SerializeToString()

    def encode(self, payload: dict) -> bytes:
        msg = self._message_class()
        msg.timestamp = payload["timestamp"]
        msg.name = payload["name"]
        if "int_value" in payload:
            msg.int_value = payload["int_value"]
        elif "float_value" in payload:
            msg.float_value = payload["float_value"]
        return msg.SerializeToString()


class LogCodec(ProtobufCodec):
    """Codec for foxglove.Log.

    We're using the built-in Foxglove log schema, but wrapping it in a
    Codec so it shares an interface with our other codecs.
    """

    def __init__(self):
        schema = Log.get_schema()
        self._schema_name = schema.name
        self._schema_bytes = schema.data

    @property
    def schema_name(self) -> str:
        return self._schema_name

    @property
    def schema_bytes(self) -> bytes:
        return self._schema_bytes

    def encode(self, payload: dict) -> bytes:
        return Log(
            timestamp=Timestamp(**payload["timestamp"]),
            level=payload["level"],
            message=payload["message"],
            name=payload.get("name", ""),
            file=payload.get("file", ""),
            line=payload.get("line", 0),
        ).encode()


def ned_to_enu(x: float, y: float, z: float) -> Optional[Point3]:
    """PX4 uses North-East-Down internally, but Foxglove prefers East-North-Up
    """
    return Point3(
        x=float(y),
        y=float(x),
        z=float(-z),
    )


class TrajectoryCodec(ProtobufCodec):
    """Codec that encodes the flight's trajectory.

    Without this, the 3D view is just a single vector that moves around,
    which gives basically zero meaningful information.  This emits the entire trajectory
    as a single SceneUpdate with appropriate ENU coordinates for better rendering.
    We timestamp it to the log start time, so we can seek through it from the start,
    and give it a lifetime of 0 so it persists until it's overwritten.
    """

    _ENTITY_ID = "trajectory"
    _LINE_THICKNESS = 2.0
    # Foxglove uses RGBA color
    _LINE_COLOR = Color(
        r=1.0,
        g=0.55,
        b=0.0,
        a=1.0
    )

    def __init__(self):
        schema = SceneUpdate.get_schema()
        self._schema_name = schema.name
        self._schema_bytes = schema.data

    @property
    def schema_name(self) -> str:
        return self._schema_name

    @property
    def schema_bytes(self) -> bytes:
        return self._schema_bytes

    def encode(self, payload: dict) -> bytes:
        lines = [
            LinePrimitive(
                type=LinePrimitiveLineType.LineStrip,
                pose=Pose(
                    position=Vector3(x=0.0, y=0.0, z=0.0),
                    orientation=Quaternion(x=0.0, y=0.0, z=0.0, w=1.0),
                ),
                thickness = self._LINE_THICKNESS,
                scale_invariant=True,
                points=payload["points"],
                color=self._LINE_COLOR,
                colors=[],
            )
        ]
        scene_entity = SceneEntity(
            timestamp=Timestamp(**payload["timestamp"]),
            frame_id="local_origin",
            id=self._ENTITY_ID,
            lifetime=Duration(sec=0, nsec=0),
            lines=lines,
        )
        return SceneUpdate(
            entities=[scene_entity],
            deletions=[],
        ).encode()
