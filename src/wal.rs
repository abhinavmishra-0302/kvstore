// ============================================================
// wal.rs - Stage 2: write-ahead log for durability.
//
// Every mutating command (SET/DEL) is appended here *before* it's
// applied to the in-memory store. On restart, replaying the log
// from the start reconstructs the store exactly as it was.
// ============================================================

use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufReader, Write};
use std::path::PathBuf;

pub struct WriteAheadLog {
    file: File,
    path: PathBuf,
    writes_since_sync: u64,
}

impl WriteAheadLog {
    /// Opens (creating if necessary) the log file in append mode.
    pub fn new(path: &str) -> io::Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(WriteAheadLog {
            file,
            path: PathBuf::from(path),
            writes_since_sync: 0,
        })
    }

    /// Appends one entry as a line. Does NOT fsync - callers batch
    /// syncs for performance (see `maybe_sync`/`sync`).
    pub fn append(&mut self, entry: &str) -> io::Result<()> {
        self.file.write_all(entry.as_bytes())?;
        if !entry.ends_with('\n') {
            self.file.write_all(b"\n")?;
        }
        self.writes_since_sync += 1;
        Ok(())
    }

    /// Durably flushes all writes to disk. This is the expensive part.
    pub fn sync(&mut self) -> io::Result<()> {
        self.file.sync_all()?;
        self.writes_since_sync = 0;
        Ok(())
    }

    /// Syncs only if `threshold` writes have accumulated since the
    /// last sync, so callers can batch fsyncs instead of paying the
    /// cost on every single command.
    pub fn maybe_sync(&mut self, threshold: u64) -> io::Result<()> {
        if self.writes_since_sync >= threshold {
            self.sync()?;
        }
        Ok(())
    }

    /// Reads the log from the start, yielding each entry in order.
    /// Used on startup to replay history into a fresh store.
    pub fn iter_entries(&self) -> impl Iterator<Item = String> {
        let file = File::open(&self.path).expect("failed to open WAL file for reading");
        let reader = BufReader::new(file);
        reader.lines().map_while(Result::ok)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Each test uses its own log file (named after the test) so tests
    // running in parallel don't stomp on each other, and cleans up
    // after itself.
    struct TempWal {
        path: String,
        wal: WriteAheadLog,
    }

    impl TempWal {
        fn new(name: &str) -> Self {
            let path = format!("test_wal_{name}.log");
            std::fs::remove_file(&path).ok();
            let wal = WriteAheadLog::new(&path).unwrap();
            TempWal { path, wal }
        }
    }

    impl Drop for TempWal {
        fn drop(&mut self) {
            std::fs::remove_file(&self.path).ok();
        }
    }

    #[test]
    fn test_wal_append_and_read() {
        let mut t = TempWal::new("append_and_read");

        t.wal.append("SET key1 value1").unwrap();
        t.wal.append("SET key2 value2").unwrap();
        t.wal.sync().unwrap();

        let entries: Vec<_> = t.wal.iter_entries().collect();
        assert_eq!(entries.len(), 2);
        assert!(entries[0].contains("key1"));
        assert!(entries[1].contains("key2"));
    }

    #[test]
    fn test_append_without_trailing_newline_still_yields_one_line() {
        let mut t = TempWal::new("no_trailing_newline");
        t.wal.append("SET a 1").unwrap();
        t.wal.append("SET b 2").unwrap();

        let entries: Vec<_> = t.wal.iter_entries().collect();
        assert_eq!(entries, vec!["SET a 1", "SET b 2"]);
    }

    #[test]
    fn test_maybe_sync_respects_threshold() {
        let mut t = TempWal::new("maybe_sync_threshold");
        t.wal.append("SET a 1").unwrap();
        t.wal.append("SET b 2").unwrap();

        // Below threshold: writes_since_sync should stay nonzero.
        t.wal.maybe_sync(10).unwrap();
        assert_eq!(t.wal.writes_since_sync, 2);

        // At/above threshold: should sync and reset the counter.
        t.wal.maybe_sync(2).unwrap();
        assert_eq!(t.wal.writes_since_sync, 0);
    }

    #[test]
    fn test_recovery_from_wal() {
        let path = "test_wal_recovery.log";
        std::fs::remove_file(path).ok();

        // Simulate a crash: write to the WAL and sync.
        {
            let mut wal = WriteAheadLog::new(path).unwrap();
            wal.append("SET name alice").unwrap();
            wal.sync().unwrap();
        }

        // Recover: reopen and replay the WAL into a fresh store.
        let mut store = std::collections::HashMap::new();
        let wal = WriteAheadLog::new(path).unwrap();
        for entry in wal.iter_entries() {
            let mut parts = entry.splitn(3, ' ');
            if parts.next() == Some("SET") {
                if let (Some(k), Some(v)) = (parts.next(), parts.next()) {
                    store.insert(k.to_string(), v.to_string());
                }
            }
        }

        assert_eq!(store.get("name"), Some(&"alice".to_string()));

        std::fs::remove_file(path).ok();
    }
}
