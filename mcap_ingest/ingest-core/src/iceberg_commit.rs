//! Write a topic group to its Iceberg table.
//!
//! iceberg-rust doesn't have update_schema support in a cargo release yet, so this
//! depends on the `main` git dependency and is, thus, a little fragile.
//! Also, this uses Iceberg V1 since the rust library doesn't support update_table
//! for V2+

use std::collections::HashMap;
use std::time::Instant;

use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use chrono::NaiveDate;
use iceberg::arrow::{arrow_schema_to_schema, schema_to_arrow_schema};
use iceberg::spec::{
    DataFileFormat, FormatVersion, Literal, PartitionKey, Struct, Transform, Type,
    UnboundPartitionField, UnboundPartitionSpec,
};
use iceberg::table::Table;
use iceberg::transaction::{AddColumn, ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
use iceberg_catalog_rest::RestCatalog;
use prost_reflect::FieldDescriptor;

use crate::arrow_encode::{build_record_batch, date_to_days};
use crate::arrow_schema::{
    build_arrow_schema, map_column_name, is_supported_field, proto_field_to_arrow,
    add_field_id,
};
use crate::parquet::sorted_parquet_props;
use crate::topics::TopicGroup;


#[derive(Default)]
pub struct CommitStats {
    pub parquet_bytes: u64,
    pub transform_seconds: f64,
    pub write_seconds: f64,
}

impl CommitStats {
    fn add(&mut self, other: CommitStats) {
        self.parquet_bytes += other.parquet_bytes;
        self.transform_seconds += other.transform_seconds;
        self.write_seconds += other.write_seconds;
    }
}

pub async fn commit_mcap(
    catalog: &RestCatalog,
    namespace: &str,
    mcap_id: &str,
    log_date: NaiveDate,
    groups: HashMap<String, TopicGroup>,
) -> CommitStats {
    let mut mcap_commit_stats = CommitStats::default();
    for (_base, group) in groups {
        let topic_commit_stats = commit_topic_group(catalog, namespace, mcap_id, log_date, group).await;
        mcap_commit_stats.add(topic_commit_stats);
    }
    mcap_commit_stats
}

/// Writes a TopicGroup's rows to Iceberg:
///   1. Add any required new fields to the table (or create the table if it doesn't exist)
///   2. null-fill any columns present in the table but not in the TopicGroup
///   3. Write the data out to a parquet file
///   4. Commit that data file to Iceberg
pub async fn commit_topic_group(
    catalog: &RestCatalog,
    namespace: &str,
    mcap_id: &str,
    log_date: NaiveDate,
    mut group: TopicGroup,
) -> CommitStats {
    group.rows.sort_by_key(|r| r.publish_time);

    let namespace_ident = NamespaceIdent::new(namespace.to_string());
    let table_ident = TableIdent::new(namespace_ident.clone(), group.base_topic.clone());

    let write_start = Instant::now();
    let unevolved_table = ensure_table_exists(catalog, &namespace_ident, &table_ident, &group).await;
    let table = evolve_table_schema(catalog, unevolved_table, &group).await;
    let mut write_seconds = write_start.elapsed().as_secs_f64();

    let transform_start = Instant::now();
    let table_schema = table.metadata().current_schema().clone();
    let annotated_arrow_schema = schema_to_arrow_schema(&table_schema)
        .expect("failed to derive arrow schema from table");
    let batch = build_record_batch(&annotated_arrow_schema, &group, mcap_id, log_date);
    let transform_seconds = transform_start.elapsed().as_secs_f64();

    let persist_start = Instant::now();
    let location_generator = DefaultLocationGenerator::new(table.metadata().clone())
        .expect("failed to build location generator");
    let file_name_generator =
        DefaultFileNameGenerator::new(mcap_id.to_string(), None, DataFileFormat::Parquet);

    let partition_key = PartitionKey::new(
        table.metadata().default_partition_spec().as_ref().clone(),
        table_schema.clone(),
        Struct::from_iter([Some(Literal::date(date_to_days(log_date)))]),
    );

    let parquet_builder =
        ParquetWriterBuilder::new(sorted_parquet_props(&annotated_arrow_schema), table_schema);
    let rolling = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_builder,
        table.file_io().clone(),
        location_generator,
        file_name_generator,
    );
    let data_file_builder = DataFileWriterBuilder::new(rolling);

    let mut writer = data_file_builder
        .build(Some(partition_key))
        .await
        .expect("failed to build data file writer");
    writer
        .write(batch)
        .await
        .expect("failed to write record batch");
    let data_files = writer.close().await.expect("failed to close writer");
    let parquet_bytes: u64 = data_files.iter().map(|f| f.file_size_in_bytes()).sum();

    let tx = Transaction::new(&table);
    let action = tx.fast_append().add_data_files(data_files);
    let tx = action.apply(tx).expect("failed to stage append");
    tx.commit(catalog).await.expect("failed to commit snapshot");
    write_seconds += persist_start.elapsed().as_secs_f64();

    CommitStats {
        parquet_bytes,
        transform_seconds,
        write_seconds,
    }
}


/// Updates the Iceberg table's schema to contain all columns in the current TopicGroup.
/// In the case of a field-type conflict, the new type gets put into a new, type-postfixed column 
async fn evolve_table_schema(catalog: &RestCatalog, table: Table, group: &TopicGroup) -> Table {
    let table_arrow = schema_to_arrow_schema(table.metadata().current_schema())
        .expect("failed to derive arrow schema from table");
    let table_columns: HashMap<String, DataType> = table_arrow
        .fields()
        .iter()
        .map(|f| (f.name().to_string(), f.data_type().clone()))
        .collect();

    let additions: Vec<(String, FieldDescriptor)> = group
        .descriptor
        .fields()
        .filter(is_supported_field)
        .filter_map(|f| {
            let col = map_column_name(f.name(), &proto_field_to_arrow(&f), &table_columns);
            (!table_columns.contains_key(&col)).then_some((col, f))
        })
        .collect();

    if additions.is_empty() {
        return table;
    }

    let tx = Transaction::new(&table);
    let mut action = tx.update_schema();
    for (col, field) in &additions {
        // This indicates a column type collision has occurred.
        // We handle this, but still want to log
        if col.as_str() != field.name() {
            eprintln!(
                "WARNING: schema drift in topic '{}': field '{}' is {:?} here but the table \
                 column is {:?}; writing to sibling column '{}'",
                group.base_topic,
                field.name(),
                proto_field_to_arrow(field),
                table_columns[field.name()],
                col,
            );
        }
        action = action.add_column(AddColumn::optional(col, iceberg_type_for_proto_field(field)));
    }
    let tx = action.apply(tx).expect("failed to stage schema evolution");
    tx.commit(catalog)
        .await
        .expect("failed to commit schema evolution")
}

/// Derive the Iceberg column type for a new proto field by round-tripping a one-field
/// arrow schema through arrow_schema_to_schema, so the type mapping matches exactly what
/// build_arrow_schema would have produced at table-creation time. update_schema assigns
/// the real field id, so the placeholder here is irrelevant.
fn iceberg_type_for_proto_field(field: &FieldDescriptor) -> Type {
    let raw = Field::new(field.name(), proto_field_to_arrow(field), true);
    let mut next_id = 1;
    let arrow_field = add_field_id(&raw, &mut next_id);
    let iceberg_schema = arrow_schema_to_schema(&ArrowSchema::new(vec![arrow_field]))
        .expect("failed to convert field type");
    iceberg_schema.as_struct().fields()[0]
        .field_type
        .as_ref()
        .clone()
}

async fn ensure_table_exists(
    catalog: &RestCatalog,
    namespace_ident: &NamespaceIdent,
    table_ident: &TableIdent,
    group: &TopicGroup,
) -> Table {
    if !catalog
        .namespace_exists(namespace_ident)
        .await
        .expect("namespace_exists failed")
    {
        catalog
            .create_namespace(namespace_ident, HashMap::new())
            .await
            .expect("create_namespace failed");
    }

    if catalog
        .table_exists(table_ident)
        .await
        .expect("table_exists failed")
    {
        return catalog
            .load_table(table_ident)
            .await
            .expect("load_table failed");
    }

    let arrow_schema = build_arrow_schema(&group.descriptor);
    let iceberg_schema = arrow_schema_to_schema(&arrow_schema)
        .expect("failed to convert schema for table creation");

    let log_date_field_id = iceberg_schema
        .field_id_by_name("log_date")
        .expect("schema missing log_date field id");
    let partition_spec = UnboundPartitionSpec::builder()
        .with_spec_id(0)
        .add_partition_fields([UnboundPartitionField {
            source_id: log_date_field_id,
            field_id: Some(1000),
            name: "log_date".to_string(),
            transform: Transform::Identity,
        }])
        .expect("failed to add log_date partition field")
        .build();

    // We're locked into V1 as iceberg-rust doesn't support V2+.  But, schema evolution
    // (update_schema specifically) is supported for V1 and we don't need row-level deletions.
    let creation = TableCreation::builder()
        .name(group.base_topic.clone())
        .schema(iceberg_schema)
        .partition_spec(partition_spec)
        .format_version(FormatVersion::V1)
        .build();
    catalog
        .create_table(namespace_ident, creation)
        .await
        .expect("create_table failed")
}
