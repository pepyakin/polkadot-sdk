// This file is part of Substrate.

// Copyright (C) Parity Technologies (UK) Ltd.
// SPDX-License-Identifier: Apache-2.0

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use anyhow::{bail, Result};
use clap::{Parser, Subcommand};
use parity_scale_codec::Decode;
use std::{
	collections::BTreeMap,
	fs::File,
	io::{Read, Seek, SeekFrom},
	path::{Path, PathBuf},
};

#[derive(Decode)]
struct MetadataEntry {
	block_num: u32,
	block_hash: [u8; 32],
	data_offset: u64,
	data_len: u64,
}

type StorageKey = Vec<u8>;
type StorageValue = Vec<u8>;
type StorageCollection = Vec<(StorageKey, Option<StorageValue>)>;
type ChildStorageCollection = Vec<(StorageKey, StorageCollection)>;

#[derive(Decode)]
struct DataEntry {
	storage: StorageCollection,
	child_storage: ChildStorageCollection,
}

struct ChangesLog {
	data: File,
	metadata: File,
}

const METADATA_ENTRY_LEN: u64 = 52;

impl ChangesLog {
	fn open(dir: &Path) -> Result<Self> {
		if !dir.exists() {
			bail!("{} doesn't exist", dir.display());
		}
		let metadata = dir.join("metadata");
		let data = dir.join("data");
		Ok(Self { data: File::open(data)?, metadata: File::open(metadata)? })
	}

	fn len(&mut self) -> Result<u64> {
		let metadata_len = self.metadata.seek(SeekFrom::End(0))?;
		let remainder = metadata_len % METADATA_ENTRY_LEN;
		let len = metadata_len / METADATA_ENTRY_LEN;
		if remainder != 0 {
			bail!("The number of entries is not an integer")
		}
		Ok(len)
	}

	fn read_metadata(&mut self, index: usize) -> Result<MetadataEntry> {
		let len = self.len()?;
		if index > len as usize {
			bail!("the requested index {} is out of range. There are only {} entries", index, len);
		}
		let offset = METADATA_ENTRY_LEN * index as u64;
		let mut buf = [0; METADATA_ENTRY_LEN as usize];
		self.metadata.seek(SeekFrom::Start(offset))?;
		self.metadata.read_exact(&mut buf)?;
		let metadata_entry = MetadataEntry::decode(&mut &buf[..])?;
		Ok(metadata_entry)
	}

	fn read_data(&mut self, metadata_entry: &MetadataEntry) -> Result<DataEntry> {
		let mut buf = vec![0u8; metadata_entry.data_len as usize];
		self.data.seek(SeekFrom::Start(metadata_entry.data_offset))?;
		self.data.read_exact(&mut buf)?;
		let data_entry = DataEntry::decode(&mut &buf[..])?;
		Ok(data_entry)
	}
}

fn print_info(dir: &Path) -> Result<()> {
	let mut changes_log = ChangesLog::open(dir)?;
	let len = changes_log.len()?;
	println!("Number of entries: {}", len);
	Ok(())
}

/// Note on serialization
///
/// If the storage or child storage values are `None` they will be serialized as `null`. This is
/// by design, because `null` represents a removed value. Admitedly, this can cause problems for
/// some languages that are lenient with deserialization. But hopefully they will manage.
///
/// Then, we don't serialize the top objects (`storage` and `child_storage`) if they are empty.
/// This is to conserve space in the output.
///
/// However, this doesn't affect the serialization of the nested objects for `child_storage` and
/// `storage`.
#[derive(serde::Serialize)]
struct EntryJson {
	block_num: u32,
	block_hash: String,
	#[serde(skip_serializing_if = "BTreeMap::is_empty")]
	storage: BTreeMap<String, Option<String>>,
	#[serde(skip_serializing_if = "BTreeMap::is_empty")]
	child_storage: BTreeMap<String, BTreeMap<String, Option<String>>>,
}

fn to_hex_string(data: &[u8]) -> String {
	format!("0x{}", hex::encode(data))
}

impl EntryJson {
	fn new(metadata: &MetadataEntry, data: &DataEntry) -> Self {
		let block_num = metadata.block_num;
		let block_hash = to_hex_string(&metadata.block_hash[..]);
		let storage = data
			.storage
			.iter()
			.map(|&(ref k, ref v)| {
				let k = to_hex_string(k);
				let v = v.as_ref().map(|v| to_hex_string(v));
				(k, v)
			})
			.collect();
		let child_storage = data
			.child_storage
			.iter()
			.map(|&(ref child_key, ref storage)| {
				let child_key = to_hex_string(child_key);
				let storage = storage
					.iter()
					.map(|&(ref k, ref v)| {
						let k = to_hex_string(k);
						let v = v.as_ref().map(|v| to_hex_string(v));
						(k, v)
					})
					.collect();
				(child_key, storage)
			})
			.collect();
		Self { block_num, block_hash, storage, child_storage }
	}
}

fn print_entry(dir: &Path, index: usize, json: bool) -> Result<()> {
	let mut changes_log = ChangesLog::open(dir)?;
	let metadata_entry = changes_log.read_metadata(index)?;
	let data_entry = changes_log.read_data(&metadata_entry)?;

	if json {
		let output = serde_json::to_string(&EntryJson::new(&metadata_entry, &data_entry)).unwrap();
		println!("{}", output);
	} else {
		println!("Entry #{}", index);
		println!("  Block Number: {}", metadata_entry.block_num);
		println!("  Block Hash: 0x{}", hex::encode(&metadata_entry.block_hash));
		println!("  Storage Updates:");
		for (index, (key, value)) in data_entry.storage.iter().enumerate() {
			println!(
				"    {}: {} -> {}",
				index,
				hex::encode(key),
				value.as_ref().map(|v| hex::encode(&v)).unwrap_or("none".to_string()),
			);
		}
		for (child_storage_key, child_storage) in &data_entry.child_storage {
			println!("  Child Storage {}:", hex::encode(child_storage_key));
			for (index, (key, value)) in child_storage.iter().enumerate() {
				println!(
					"    {}: {} -> {}",
					index,
					hex::encode(key),
					value.as_ref().map(|v| hex::encode(&v)).unwrap_or("none".to_string()),
				);
			}
		}
	}
	Ok(())
}

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
	#[command(subcommand)]
	command: Commands,
}

#[derive(Subcommand)]
enum Commands {
	/// Displays the information about the given changes log.
	Info {
		/// The `changes_log` directory located in the data folder of a substrate chain.
		#[arg(env = "CHANGES_LOG_DIR")]
		dir: PathBuf,
	},
	/// Shows a specified entry of the given changes log.
	Show {
		/// The index of the entry. If the file is not malformed, then the entry at that index will
		/// contain the finalized block of the same number.
		index: usize,
		/// The `changes_log` directory located in the data folder of a substrate chain.
		#[arg(env = "CHANGES_LOG_DIR")]
		dir: PathBuf,
		/// Specify this flag to use  the JSON format instead of human readable.
		#[arg(short, long)]
		json: bool,
	},
}

fn main() -> Result<()> {
	let cli = Cli::parse();
	match cli.command {
		Commands::Info { dir } => {
			print_info(&dir)?;
		},
		Commands::Show { dir, index, json } => {
			print_entry(&dir, index, json)?;
		},
	}
	Ok(())
}
