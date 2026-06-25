//! Build the Arrow schema for a topic from its proto descriptor.
//!
//! The schema is the standard columns, followed by one column per supported proto field.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use prost_reflect::{FieldDescriptor, Kind, MessageDescriptor};

pub fn build_arrow_schema(descriptor: &MessageDescriptor) -> Schema {
    let mut fields: Vec<Field> = standard_columns();
    for f in descriptor.fields().filter(is_supported_field) {
        fields.push(Field::new(f.name(), proto_field_to_arrow(&f), true));
    }

    let mut next_id = 1;
    let fields: Vec<Field> = fields.iter().map(|f| add_field_id(f, &mut next_id)).collect();
    Schema::new(fields)
}

// This mutable next_id thing is a little awkward, but because a List
// requires 2 sequential id's (one for the list, the other for the list field)
// this handles that relatively simply
pub fn add_field_id(field: &Field, next_id: &mut i32) -> Field {
    let id = *next_id;
    *next_id += 1;

    let mut metadata = field.metadata().clone();
    metadata.insert(PARQUET_FIELD_ID_META_KEY.to_string(), id.to_string());

    let data_type = match field.data_type() {
        DataType::List(item) => DataType::List(Arc::new(add_field_id(item, next_id))),
        other => other.clone(),
    };

    Field::new(field.name(), data_type, field.is_nullable()).with_metadata(metadata)
}

pub fn standard_columns() -> Vec<Field> {
    vec![
        // Iceberg V1 only supports microseconds
        Field::new(
            "log_time",
            DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
            false,
        ),
        Field::new(
            "publish_time",
            DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
            false,
        ),
        Field::new("sequence", DataType::Int64, false),
        Field::new("mcap_id", DataType::Utf8, false),
        Field::new("multi_id", DataType::Int32, false),
        Field::new("offset_us", DataType::Int64, false),
        Field::new("log_date", DataType::Date32, false),
        Field::new("time_source", DataType::Utf8, false),
    ]
}

pub fn is_supported_field(field: &FieldDescriptor) -> bool {
    !field.is_map() && !matches!(field.kind(), Kind::Message(_))
}

pub fn proto_field_to_arrow(field: &FieldDescriptor) -> DataType {
    let scalar = proto_kind_to_scalar_type(&field.kind());
    if field.is_list() {
        DataType::List(Arc::new(Field::new("item", scalar, true)))
    } else if field.is_map() {
        panic!("map fields not supported (field={})", field.name());
    } else {
        scalar
    }
}

pub fn type_suffix(data_type: &DataType) -> String {
    match data_type {
        DataType::Int64 => "i64".to_string(),
        DataType::Float64 => "f64".to_string(),
        DataType::Boolean => "bool".to_string(),
        DataType::Utf8 => "str".to_string(),
        DataType::Binary => "bytes".to_string(),
        DataType::List(field) => format!("list_{}", type_suffix(field.data_type())),
        other => panic!("no type suffix for unexpected proto column type {other:?}"),
    }
}

/// Arrow list elements are named "element", vs. "item" straight out of a proto,
/// Structural type equality that ignores list element field names/metadata: the table's
/// list element is named "element" with a field id, a freshly proto-derived one is "item"
/// with none, so plain `==` would spuriously differ. Compares element data types instead.
pub fn check_type_compatibility(a: &DataType, b: &DataType) -> bool {
    match (a, b) {
        (DataType::List(fa), DataType::List(fb)) => {
            check_type_compatibility(fa.data_type(), fb.data_type())
        }
        _ => a == b,
    }
}

pub fn map_column_name(
    name: &str,
    field_type: &DataType,
    table_columns: &HashMap<String, DataType>,
) -> String {
    match table_columns.get(name) {
        Some(existing) if !check_type_compatibility(existing, field_type) => {
            format!("{name}__as_{}", type_suffix(field_type))
        }
        _ => name.to_string(),
    }
}

/// Non-obvious thing we do here is size every int and float to Int64 and Float64.
/// There's some space cost to this, but fields can be widened or shrunk across
/// PX4 firmware versions, so this stops us from having e.g. an Int64 and Int32
/// column for the same underlying field.
fn proto_kind_to_scalar_type(kind: &Kind) -> DataType {
    match kind {
        Kind::Double | Kind::Float => DataType::Float64,
        Kind::Int32 | Kind::Sint32 | Kind::Sfixed32
        | Kind::Int64 | Kind::Sint64 | Kind::Sfixed64
        | Kind::Uint32 | Kind::Fixed32
        | Kind::Uint64 | Kind::Fixed64 => DataType::Int64,
        Kind::Bool => DataType::Boolean,
        Kind::String => DataType::Utf8,
        Kind::Bytes => DataType::Binary,
        Kind::Enum(_) => DataType::Utf8,
        Kind::Message(_) => panic!("nested message fields not yet supported"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn list_of(dt: DataType) -> DataType {
        DataType::List(Arc::new(Field::new("item", dt, true)))
    }

    #[test]
    fn type_suffix_codes() {
        assert_eq!(type_suffix(&DataType::Int64), "i64");
        assert_eq!(type_suffix(&DataType::Float64), "f64");
        assert_eq!(type_suffix(&list_of(DataType::Int64)), "list_i64");
    }

    #[test]
    fn check_type_compatibility_ignores_list_element_wrapper() {
        // Same element type, different element field name/nullability -> compatible.
        let proto_side = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
        let table_side = DataType::List(Arc::new(Field::new("element", DataType::Int64, false)));
        assert!(check_type_compatibility(&proto_side, &table_side));
        assert!(!check_type_compatibility(&DataType::Int64, &DataType::Float64));
        assert!(!check_type_compatibility(&list_of(DataType::Int64), &list_of(DataType::Float64)));
    }

    #[test]
    fn effective_name_uses_base_when_absent_or_compatible() {
        let cols = HashMap::from([("vbatt".to_string(), DataType::Int64)]);
        assert_eq!(map_column_name("rpm", &DataType::Int64, &cols), "rpm");
        assert_eq!(map_column_name("vbatt", &DataType::Int64, &cols), "vbatt");
    }

    #[test]
    fn effective_name_suffixes_on_type_conflict() {
        let cols = HashMap::from([("vbatt".to_string(), DataType::Int64)]);
        assert_eq!(
            map_column_name("vbatt", &DataType::Float64, &cols),
            "vbatt__as_f64"
        );
    }
}
