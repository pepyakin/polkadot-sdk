//! This is a database for storing changes to the state.

// Implementation detail: this db tracks changes to the state made in each block. Those changes
// are kept in memory until the block is finalized, at which point they are written to the
// the database.

// There are two files:
// - metadata file
// - data file
//
// metadata is basically an array of:
// - block_number: u32
// - block_hash: [u8; 32]
// - offset: u64
// - length: u64
//
// data file is basically an array of:
// - main_trie_changes*
//   - key
//   - value?
// - storage trie changes*
//   - key
//   - value?
//
// The state changes of only finalized blocks are stored in the log.

use codec::Encode;
use parking_lot::Mutex;
use sp_runtime::traits::Block as BlockT;
use sp_state_machine::{ChildStorageCollection, StorageCollection};
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::mpsc::{self, TrySendError};
use std::time::Duration;
use std::{collections::HashMap, sync::Arc};

struct Changes {
	hash: [u8; 32],
	number: u32,
	storage_updates: Arc<StorageCollection>,
	child_storage_updates: Arc<ChildStorageCollection>,
}

pub struct ChangesLog<Block: BlockT> {
	inner: Arc<Mutex<Inner<Block>>>,
}

struct Inner<Block: BlockT> {
	changes: HashMap<Block::Hash, Changes>,
	tx: mpsc::SyncSender<BackgroundEvent>,
}

impl<Block: BlockT> ChangesLog<Block> {
	pub fn new(path: PathBuf) -> Self {
		// check if the path exists. If not create all the directories leading to that path.
		if !path.exists() {
			std::fs::create_dir_all(&path).expect("Failed to create changes log directory");
		}
		let data_path = path.join("data");
		let metadata_path = path.join("metadata");

		let data_file = OpenOptions::new().append(true).create(true).open(data_path).unwrap();
		let metadata_file =
			OpenOptions::new().append(true).create(true).open(metadata_path).unwrap();
		let log = Log { metadata_file, data_file };
		let tx = start_background(log);
		Self { inner: Arc::new(Mutex::new(Inner { changes: HashMap::new(), tx })) }
	}

	pub fn insert(
		&self,
		block_hash: Block::Hash,
		number: u32,
		storage_updates: Arc<StorageCollection>,
		child_storage_updates: Arc<ChildStorageCollection>,
	) {
		let mut inner = self.inner.lock();
		let prev = inner.changes.insert(
			block_hash,
			Changes {
				hash: block_hash.as_ref().try_into().expect("only 256 bit hashes are supported"),
				number,
				storage_updates,
				child_storage_updates,
			},
		);
		assert!(prev.is_none());
	}

	pub fn note_finalized(&self, block_hash: Block::Hash) {
		let mut inner = self.inner.lock();
		let changes = match inner.changes.remove(&block_hash) {
			Some(changes) => changes,
			None => {
				log::warn!("Finalized block not found in changes log: {:?}", block_hash);
				return;
			},
		};
		// send to the background thread to persist in the log.
		let _ = inner.tx.send(BackgroundEvent::Changes(changes));
	}

	pub fn note_pruned(&self, block_hash: Block::Hash) {
		let mut inner = self.inner.lock();
		inner.changes.remove(&block_hash);
	}
}

#[derive(codec::Encode, codec::Decode)]
struct ChangesEntry {
	storage_updates: Arc<StorageCollection>,
	child_storage_updates: Arc<ChildStorageCollection>,
}

struct Log {
	metadata_file: File,
	data_file: File,
}

enum BackgroundEvent {
	Timer,
	Changes(Changes),
}

fn timer_task(duration: Duration, tx: mpsc::SyncSender<BackgroundEvent>) {
	loop {
		std::thread::sleep(duration);
		match tx.try_send(BackgroundEvent::Timer) {
			Ok(()) => (),
			// if the channel is full, don't spam it with the timer events.
			Err(TrySendError::Full(_)) => (),
			Err(TrySendError::Disconnected(_)) => {
				// normal shutdown behavior.
				break;
			},
		}
	}
}

fn background_task(mut log: Log, rx: mpsc::Receiver<BackgroundEvent>) {
	// read from the channel and persist the changes to the log.
	let mut data_ofs = log.data_file.seek(SeekFrom::End(0)).unwrap();
	let mut metadata_ofs = log.metadata_file.seek(SeekFrom::End(0)).unwrap();
	let mut metadata_file = BufWriter::new(log.metadata_file);
	let mut data_file = BufWriter::new(log.data_file);
	while let Ok(event) = rx.recv() {
		match event {
			BackgroundEvent::Timer => {
				// time to flush and sync
				if let Err(err) = metadata_file.flush() {
					log::error!("Failed to flush metadata file: {:?}", err);
				}
				if let Err(err) = data_file.flush() {
					log::error!("Failed to flush data file: {:?}", err);
				}
				if let Err(err) = metadata_file.get_ref().sync_all() {
					log::error!("Failed to sync metadata file: {:?}", err);
				}
				if let Err(err) = data_file.get_ref().sync_all() {
					log::error!("Failed to sync data file: {:?}", err);
				}
			},
			BackgroundEvent::Changes(changes) => {
				let changes_entry = ChangesEntry {
					storage_updates: changes.storage_updates.clone(),
					child_storage_updates: changes.child_storage_updates.clone(),
				};
				let changes_entry_bytes = changes_entry.encode();
				let changes_entry_len = changes_entry_bytes.len() as u64;

				let metadata_entry = (changes.number, changes.hash, data_ofs, changes_entry_len);
				let metadata_entry_bytes = metadata_entry.encode();
				let metadata_entry_len = metadata_entry_bytes.len() as u64;

				data_ofs += changes_entry_len;
				metadata_ofs += metadata_entry_len;

				if let Err(err) = data_file.write_all(&changes_entry_bytes) {
					log::error!("Failed to write changes entry: {:?}", err);
					break;
				}
				if let Err(err) = metadata_file.write_all(&metadata_entry_bytes) {
					log::error!("Failed to write metadata entry: {:?}", err);
					break;
				}
			},
		}
	}
	if let Err(err) = metadata_file.flush() {
		log::error!("Failed to flush metadata file: {:?}", err);
	}
	if let Err(err) = data_file.flush() {
		log::error!("Failed to flush data file: {:?}", err);
	}
}

fn start_background(log: Log) -> mpsc::SyncSender<BackgroundEvent> {
	use std::thread::spawn;
	const SYNC_INTERVAL: Duration = Duration::from_secs(1);
	const CHANNEL_SIZE: usize = 100;
	let (tx, rx) = mpsc::sync_channel(CHANNEL_SIZE);
	spawn(move || background_task(log, rx));
	spawn({
		let tx = tx.clone();
		move || timer_task(SYNC_INTERVAL, tx)
	});
	tx
}
