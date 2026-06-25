//! Builds an Arrow `RecordBatch` for a given TopicGroup.
//!
//! It handles type conversion between proto and Arrow types, as well as
//! table schema management
//! The batch is built against a *target* schema (the Iceberg table's current schema,
//! which may have evolved beyond what this MCAP carries), projecting by field name:
//! present fields are decoded from the messages, absent ones are null-filled. Value
//! conversions mirror `arrow_schema`'s type choices (unsigned -> signed, ns -> us
//! timestamps), and list columns are rebuilt to match the table's element field.

use std::sync::Arc;

use std::collections::HashMap;

use arrow::array::{
    Array, ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder, Float64Builder,
    Int32Builder, Int64Builder, ListArray, ListBuilder, RecordBatch, StringBuilder,
    TimestampMicrosecondBuilder, new_null_array,
};
use arrow::datatypes::{DataType, Field, Schema};
use chrono::NaiveDate;
use prost_reflect::{FieldDescriptor, Kind, Value};

use crate::arrow_schema::{map_column_name, is_supported_field, proto_field_to_arrow};

use crate::topics::{TopicGroup, TopicRow};

/// Arrow/Iceberg store dates as the number of days from the unix epioch
pub fn date_to_days(log_date: NaiveDate) -> i32 {
    (log_date - NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch is a valid date")).num_days()
        as i32
}

/// Converts the decoded proto records to an Arrow RecordBatch.
/// Adds default columns, maps proto types to arrow types,
pub fn build_record_batch(
    schema: &Schema,
    group: &TopicGroup,
    mcap_id: &str,
    log_date: NaiveDate,
) -> RecordBatch {
    let n = group.rows.len();

    let anchor_us = log_date
        .and_hms_opt(0, 0, 0)
        .expect("midnight is a valid time")
        .and_utc()
        .timestamp_micros();
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

    arrays.push(build_log_time(&group.rows, anchor_us));
    arrays.push(build_publish_time(&group.rows, anchor_us));
    arrays.push(build_sequence(&group.rows));
    arrays.push(build_mcap_id(n, mcap_id));
    arrays.push(build_multi_id(&group.rows));
    arrays.push(build_offset_us(&group.rows));
    arrays.push(build_log_date(n, date_to_days(log_date)));
    arrays.push(build_time_source(n, "log_date"));

    let standard_count = arrays.len();

    let table_columns: HashMap<String, DataType> = schema
        .fields()
        .iter()
        .map(|f| (f.name().to_string(), f.data_type().clone()))
        .collect();

    let mut built: HashMap<String, ArrayRef> = HashMap::new();
    for proto_field in group.descriptor.fields().filter(is_supported_field) {
        let col = map_column_name(
            proto_field.name(),
            &proto_field_to_arrow(&proto_field),
            &table_columns,
        );
        if let Ok(arrow_field) = schema.field_with_name(&col) {
            built.insert(col, build_proto_field(&proto_field, arrow_field, &group.rows));
        }
    }

    for arrow_field in schema.fields().iter().skip(standard_count) {
        match built.remove(arrow_field.name()) {
            Some(array) => arrays.push(array),
            None => arrays.push(new_null_array(arrow_field.data_type(), n)),
        }
    }

    RecordBatch::try_new(Arc::new(schema.clone()), arrays).expect("RecordBatch::try_new failed")
}

/// MCAP log_time is in nanoseconds, convert to micros for Iceberg
/// (nanos are only supported in V2+)
fn build_log_time(rows: &[TopicRow], anchor_us: i64) -> ArrayRef {
    let mut b = TimestampMicrosecondBuilder::with_capacity(rows.len());
    for r in rows {
        b.append_value(anchor_us + (r.log_time / 1000) as i64);
    }
    Arc::new(b.finish().with_timezone("+00:00"))
}

fn build_publish_time(rows: &[TopicRow], anchor_us: i64) -> ArrayRef {
    let mut b = TimestampMicrosecondBuilder::with_capacity(rows.len());
    for r in rows {
        b.append_value(anchor_us + (r.publish_time / 1000) as i64);
    }
    Arc::new(b.finish().with_timezone("+00:00"))
}

fn build_offset_us(rows: &[TopicRow]) -> ArrayRef {
    let mut b = Int64Builder::with_capacity(rows.len());
    for r in rows {
        b.append_value((r.log_time / 1000) as i64);
    }
    Arc::new(b.finish())
}

/// All logs in the same mcap have the same flight date
fn build_log_date(n: usize, days: i32) -> ArrayRef {
    let mut b = Date32Builder::with_capacity(n);
    for _ in 0..n {
        b.append_value(days);
    }
    Arc::new(b.finish())
}

fn build_time_source(n: usize, source: &str) -> ArrayRef {
    let mut b = StringBuilder::with_capacity(n, n * source.len());
    for _ in 0..n {
        b.append_value(source);
    }
    Arc::new(b.finish())
}

fn build_sequence(rows: &[TopicRow]) -> ArrayRef {
    // sequence is u32; widened to Int64 because Iceberg has no unsigned types.
    let mut b = Int64Builder::with_capacity(rows.len());
    for r in rows {
        b.append_value(r.sequence as i64);
    }
    Arc::new(b.finish())
}

fn build_mcap_id(n: usize, mcap_id: &str) -> ArrayRef {
    let mut b = StringBuilder::with_capacity(n, n * mcap_id.len());
    for _ in 0..n {
        b.append_value(mcap_id);
    }
    Arc::new(b.finish())
}

fn build_multi_id(rows: &[TopicRow]) -> ArrayRef {
    // multi_id is u8; Int32 is the narrowest signed Iceberg type (int).
    let mut b = Int32Builder::with_capacity(rows.len());
    for r in rows {
        b.append_value(r.multi_id as i32);
    }
    Arc::new(b.finish())
}

fn build_proto_field(field: &FieldDescriptor, arrow_field: &Field, rows: &[TopicRow]) -> ArrayRef {
    if field.is_map() {
        panic!("map fields not supported (field={})", field.name());
    }
    if field.is_list() {
        // ListBuilder names the element "item" with no metadata; the Iceberg-derived
        // schema names it "element" and carries a field id. Rebuild the list with the
        // schema's element field so RecordBatch::try_new and the writer accept it.
        let array = build_list_field(field, rows);
        coerce_list_element_field(array, arrow_field)
    } else {
        build_scalar_field(field, rows)
    }
}


fn coerce_list_element_field(array: ArrayRef, arrow_field: &Field) -> ArrayRef {
    let DataType::List(target_element) = arrow_field.data_type() else {
        return array;
    };
    let list = array
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("expected a ListArray for a list field");
    Arc::new(ListArray::new(
        target_element.clone(),
        list.offsets().clone(),
        list.values().clone(),
        list.nulls().cloned(),
    ))
}

// Generates both build_scalar_field and build_list_field from one table of the following macro inputs
//  proto_kind: the proto field(s) to match on
//  make: the Arrow builder it maps to
//  variant/bind: the expected match for the field name when we pull it out of a particular row
//  conversion: the proto->arrow converison logic (mostly identity)
macro_rules! proto_field_builders {
    ($($proto_kind:pat => $make:expr, $variant:path, $bind:ident => $conversion:expr;)+) => {
        fn build_scalar_field(field: &FieldDescriptor, rows: &[TopicRow]) -> ArrayRef {
            match field.kind() {
                $(
                    $proto_kind => {
                        let mut b = $make;
                        for r in rows {
                            match r.message.get_field(field).as_ref() {
                                $variant($bind) => b.append_value($conversion),
                                v => mismatch(field, v),
                            }
                        }
                        Arc::new(b.finish())
                    }
                )*
                Kind::Message(_) => {
                    panic!("nested message fields not supported (field={})", field.name())
                }
            }
        }

        fn build_list_field(field: &FieldDescriptor, rows: &[TopicRow]) -> ArrayRef {
            match field.kind() {
                $(
                    $proto_kind => {
                        let mut b = ListBuilder::new($make);
                        for r in rows {
                            match r.message.get_field(field).as_ref() {
                                Value::List(items) => {
                                    for item in items {
                                        match item {
                                            $variant($bind) => b.values().append_value($conversion),
                                            v => mismatch(field, v),
                                        }
                                    }
                                    b.append(true);
                                }
                                v => mismatch(field, v),
                            }
                        }
                        Arc::new(b.finish())
                    }
                )*
                Kind::Message(_) => {
                    panic!("nested message lists not supported (field={})", field.name())
                }
            }
        }
    };
}

proto_field_builders! {
    Kind::Double => Float64Builder::new(), Value::F64, x => *x;
    Kind::Float => Float64Builder::new(), Value::F32, x => *x as f64;
    Kind::Int32 | Kind::Sint32 | Kind::Sfixed32 => Int64Builder::new(), Value::I32, x => *x as i64;
    Kind::Int64 | Kind::Sint64 | Kind::Sfixed64 => Int64Builder::new(), Value::I64, x => *x;
    Kind::Uint32 | Kind::Fixed32 => Int64Builder::new(), Value::U32, x => *x as i64;
    Kind::Uint64 | Kind::Fixed64 => Int64Builder::new(), Value::U64, x => *x as i64;
    Kind::Bool => BooleanBuilder::new(), Value::Bool, x => *x;
    Kind::String => StringBuilder::new(), Value::String, s => s;
    Kind::Bytes => BinaryBuilder::new(), Value::Bytes, bytes => bytes.as_ref();
    Kind::Enum(enum_desc) => StringBuilder::new(), Value::EnumNumber, n =>
        enum_desc.get_value(*n).map(|v| v.name().to_string()).unwrap_or_else(|| format!("UNKNOWN_{n}"));
}

fn mismatch(field: &FieldDescriptor, got: &Value) -> ! {
    panic!(
        "type mismatch for field {}: kind={:?}, got Value::{:?}",
        field.name(),
        field.kind(),
        std::mem::discriminant(got),
    )
}
