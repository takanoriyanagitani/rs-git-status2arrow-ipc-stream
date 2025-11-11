use arrow::array::{
    ArrayRef, DictionaryArray, StringBuilder, TimestampSecondBuilder, UInt64Builder,
};
use arrow::datatypes::{DataType, Field, Int32Type, Schema, TimeUnit};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use gix::bstr::ByteSlice;
use std::io;
use std::path::Path;
use std::sync::Arc;

use io::Write;

use gix::Progress;
use gix::Repository;

use gix::status::Item as GixStatusItem;
use gix::status::Platform;
use gix::status::index_worktree::Item as GixStatusWorkTreeItem;
use gix::status::index_worktree::iter::Summary as GixSummary;

use gix::diff::index::Change as GixChange;

use serde::Serialize;

#[derive(Debug, Serialize)]
pub enum StatusDto {
    Removed,
    Added,
    Modified,
    TypeChange,
    Renamed,
    Copied,
    IntentToAdd,
    Conflict,
    Untracked,
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum StatusItemDto {
    IndexWorktree { path: String, status: StatusDto },
    TreeIndex { path: String, status: StatusDto },
}

impl From<&GixStatusItem> for StatusItemDto {
    fn from(item: &GixStatusItem) -> Self {
        match item {
            GixStatusItem::IndexWorktree(iw_item) => {
                let status = match iw_item.summary() {
                    Some(GixSummary::Removed) => StatusDto::Removed,
                    Some(GixSummary::Added) => StatusDto::Added,
                    Some(GixSummary::Modified) => StatusDto::Modified,
                    Some(GixSummary::TypeChange) => StatusDto::TypeChange,
                    Some(GixSummary::Renamed) => StatusDto::Renamed,
                    Some(GixSummary::Copied) => StatusDto::Copied,
                    Some(GixSummary::IntentToAdd) => StatusDto::IntentToAdd,
                    Some(GixSummary::Conflict) => StatusDto::Conflict,
                    None => StatusDto::Untracked,
                };
                StatusItemDto::IndexWorktree {
                    path: iw_item.rela_path().to_string(),
                    status,
                }
            }
            GixStatusItem::TreeIndex(ti_change) => {
                let (path, status) = match ti_change {
                    GixChange::Addition { location, .. } => {
                        (location.to_string(), StatusDto::Added)
                    }
                    GixChange::Deletion { location, .. } => {
                        (location.to_string(), StatusDto::Removed)
                    }
                    GixChange::Modification { location, .. } => {
                        (location.to_string(), StatusDto::Modified)
                    }
                    GixChange::Rewrite { location, .. } => {
                        (location.to_string(), StatusDto::Renamed)
                    }
                };
                StatusItemDto::TreeIndex { path, status }
            }
        }
    }
}

pub struct GitDir<P>(pub P);

impl<P> GitDir<P>
where
    P: AsRef<Path>,
{
    pub fn discover(&self) -> Result<Repository, io::Error> {
        gix::discover(self.0.as_ref()).map_err(io::Error::other)
    }
}

pub struct GitRepo(pub Repository);

impl GitRepo {
    pub fn status<P>(&self, progress: P) -> Result<Platform<'_, P>, io::Error>
    where
        P: Progress,
    {
        self.0.status(progress).map_err(io::Error::other)
    }
}

pub struct GitStatus<'a, P>(pub Platform<'a, P>)
where
    P: Progress + 'static;

impl<'a, P> GitStatus<'a, P>
where
    P: Progress + 'static,
{
    pub fn iter(self) -> Result<impl Iterator<Item = Result<GixStatusItem, io::Error>>, io::Error> {
        self.0
            .into_iter(vec![])
            .map_err(io::Error::other)
            .map(|i| i.map(|r| r.map_err(io::Error::other)))
    }
}

pub struct GitStatusItemWorktree(pub GixStatusWorkTreeItem);

pub struct GitStatusIndexChange(pub GixChange);

pub fn status2json2writer<W>(status: &GixStatusItem, wtr: &mut W) -> Result<(), io::Error>
where
    W: Write,
{
    let dto = StatusItemDto::from(status);
    serde_json::to_writer(&mut *wtr, &dto)?;
    writeln!(wtr)?; // Add a newline for pretty printing each JSON object
    Ok(())
}

pub fn get_arrow_schema() -> Schema {
    Schema::new(vec![
        Field::new("path", DataType::Utf8, false),
        Field::new(
            "status",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        ),
        Field::new(
            "item_type",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        ),
        Field::new("extension", DataType::Utf8, true),
        Field::new("size", DataType::UInt64, true),
        Field::new(
            "last_modification_time",
            DataType::Timestamp(TimeUnit::Second, None),
            true,
        ),
    ])
}

pub fn status2arrow_ipc_stream_writer<W>(
    items: &[GixStatusItem],
    wtr: &mut W,
) -> Result<(), io::Error>
where
    W: Write,
{
    let schema = get_arrow_schema();
    let mut path_builder = StringBuilder::new();
    let mut extension_builder = StringBuilder::new();
    let mut size_builder = UInt64Builder::new();
    let mut mtime_builder = TimestampSecondBuilder::new();

    for item in items {
        match item {
            GixStatusItem::IndexWorktree(iw_item) => {
                let path = iw_item.rela_path();
                path_builder.append_value(path.to_string());
                let extension = path
                    .to_path()
                    .ok()
                    .and_then(|p| p.extension())
                    .and_then(|s| s.to_str())
                    .unwrap_or("");
                extension_builder.append_value(extension);
                if let Ok(metadata) = std::fs::metadata(path.to_string()) {
                    size_builder.append_value(metadata.len());
                    if let Ok(mtime) = metadata.modified() {
                        if let Ok(duration) = mtime.duration_since(std::time::UNIX_EPOCH) {
                            mtime_builder.append_value(duration.as_secs() as i64);
                        } else {
                            mtime_builder.append_null();
                        }
                    } else {
                        mtime_builder.append_null();
                    }
                } else {
                    size_builder.append_null();
                    mtime_builder.append_null();
                }
            }
            GixStatusItem::TreeIndex(ti_change) => {
                let path = match ti_change {
                    GixChange::Addition { location, .. } => location,
                    GixChange::Deletion { location, .. } => location,
                    GixChange::Modification { location, .. } => location,
                    GixChange::Rewrite { location, .. } => location,
                };
                path_builder.append_value(path.to_string());
                let extension = path
                    .to_path()
                    .ok()
                    .and_then(|p| p.extension())
                    .and_then(|s| s.to_str())
                    .unwrap_or("");
                extension_builder.append_value(extension);
                size_builder.append_null();
                mtime_builder.append_null();
            }
        }
    }
    let path_array = Arc::new(path_builder.finish()) as ArrayRef;
    let extension_array = Arc::new(extension_builder.finish()) as ArrayRef;
    let size_array = Arc::new(size_builder.finish()) as ArrayRef;
    let mtime_array = Arc::new(mtime_builder.finish()) as ArrayRef;

    let status_values: Vec<_> = items
        .iter()
        .map(|item| {
            let dto = StatusItemDto::from(item);
            match dto {
                StatusItemDto::IndexWorktree { status, .. } => format!("{:?}", status),
                StatusItemDto::TreeIndex { status, .. } => format!("{:?}", status),
            }
        })
        .collect();
    let status_array = Arc::new(DictionaryArray::<Int32Type>::from_iter(
        status_values.iter().map(|s| s.as_str()),
    )) as ArrayRef;

    let item_type_values: Vec<_> = items
        .iter()
        .map(|item| match item {
            GixStatusItem::IndexWorktree(_) => "IndexWorktree",
            GixStatusItem::TreeIndex(_) => "TreeIndex",
        })
        .collect();
    let item_type_array = Arc::new(DictionaryArray::<Int32Type>::from_iter(
        item_type_values.iter().copied(),
    )) as ArrayRef;

    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            path_array,
            status_array,
            item_type_array,
            extension_array,
            size_array,
            mtime_array,
        ],
    )
    .map_err(io::Error::other)?;

    let mut writer = StreamWriter::try_new(wtr, &schema).map_err(io::Error::other)?;
    writer.write(&batch).map_err(io::Error::other)?;
    writer.finish().map_err(io::Error::other)?;

    Ok(())
}
