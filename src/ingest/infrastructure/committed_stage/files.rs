//! The file operations each durability step runs on: the fixed-buffer copy and
//! its SHA-256 digest, directory preparation, opens that refuse symlinks and
//! non-regular files, syncs, and renames that never replace an existing name.

use std::fs::{self, File, OpenOptions};
use std::io::{self, ErrorKind, Read, Write};
use std::path::Path;

use sha2::{Digest, Sha256};

use crate::ingest::infrastructure::committed_stage::{invalid, COPY_BUFFER_BYTES};

pub(super) fn copy_reader_fixed<R: Read>(reader: &mut R, output: &mut dyn Write) -> io::Result<()> {
    let mut buffer = [0u8; COPY_BUFFER_BYTES];
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        output.write_all(&buffer[..read])?;
    }
    Ok(())
}

pub(super) fn write_and_hash<F>(target: &Path, write: &mut F) -> io::Result<(u64, [u8; 32])>
where
    F: FnMut(&mut dyn Write) -> io::Result<()>,
{
    let mut output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(target)?;
    let mut hasher = Sha256::new();
    let mut bytes = 0u64;
    {
        let mut writer = HashingWriter {
            output: &mut output,
            hasher: &mut hasher,
            bytes: &mut bytes,
        };
        write(&mut writer)?;
    }
    Ok((bytes, hasher.finalize().into()))
}

struct HashingWriter<'a> {
    output: &'a mut File,
    hasher: &'a mut Sha256,
    bytes: &'a mut u64,
}

impl Write for HashingWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.output.write_all(bytes)?;
        self.hasher.update(bytes);
        *self.bytes = self
            .bytes
            .checked_add(u64::try_from(bytes.len()).expect("slice length fits u64"))
            .ok_or_else(|| invalid("committed-stage payload length overflow"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        self.output.flush()
    }
}

pub(super) fn digest_reader<R: Read>(mut reader: R) -> io::Result<(u64, [u8; 32])> {
    let mut hasher = Sha256::new();
    let mut bytes = 0u64;
    let mut buffer = [0u8; COPY_BUFFER_BYTES];
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        bytes = bytes
            .checked_add(u64::try_from(read).expect("buffer length fits u64"))
            .ok_or_else(|| invalid("committed-stage payload length overflow"))?;
    }
    Ok((bytes, hasher.finalize().into()))
}

pub(super) fn prepare_root(root: &Path) -> io::Result<()> {
    if !root.exists() {
        fs::create_dir_all(root)?;
    }
    ensure_directory(root)?;
    let records = root.join("records");
    let markers = root.join("markers");
    if !records.exists() {
        fs::create_dir(&records)?;
    }
    if !markers.exists() {
        fs::create_dir(&markers)?;
    }
    ensure_directory(&records)?;
    ensure_directory(&markers)?;
    Ok(())
}

pub(super) fn ensure_directory(path: &Path) -> io::Result<()> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(invalid("committed-stage path is not a real directory"));
    }
    Ok(())
}

pub(super) fn ensure_regular_file(path: &Path) -> io::Result<()> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(invalid("committed-stage path is not a real regular file"));
    }
    Ok(())
}

pub(super) fn sync_regular_file(path: &Path) -> io::Result<()> {
    open_regular_readonly(path)?.sync_all()
}

pub(super) fn sync_directory(path: &Path) -> io::Result<()> {
    open_directory_readonly(path)?.sync_all()
}

/// The stage root is private to this process, but a checked metadata result is
/// still not sufficient: a hostile or accidental concurrent rename can replace
/// the final component before `File::open`.  On supported Unix targets,
/// `O_NOFOLLOW` makes that race fail instead of dereferencing the replacement.
/// The post-open metadata check also rejects non-regular descriptors.
pub(super) fn open_regular_readonly(path: &Path) -> io::Result<File> {
    ensure_regular_file(path)?;
    let file = open_readonly_nofollow(path)?;
    if !file.metadata()?.is_file() {
        return Err(invalid("committed-stage path opened as non-regular file"));
    }
    Ok(file)
}

fn open_directory_readonly(path: &Path) -> io::Result<File> {
    ensure_directory(path)?;
    let file = open_readonly_nofollow(path)?;
    if !file.metadata()?.is_dir() {
        return Err(invalid("committed-stage path opened as non-directory"));
    }
    Ok(file)
}

fn open_readonly_nofollow(path: &Path) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(any(target_os = "macos", target_os = "ios"))]
    {
        use std::os::unix::fs::OpenOptionsExt;
        const O_NOFOLLOW: i32 = 0x0100;
        options.custom_flags(O_NOFOLLOW);
    }
    #[cfg(any(target_os = "linux", target_os = "android"))]
    {
        use std::os::unix::fs::OpenOptionsExt;
        const O_NOFOLLOW: i32 = 0o400000;
        options.custom_flags(O_NOFOLLOW);
    }
    let file = options.open(path)?;
    Ok(file)
}

pub(super) fn rename_new(from: &Path, to: &Path) -> io::Result<()> {
    if path_exists(to)? {
        return Err(io::Error::new(
            ErrorKind::AlreadyExists,
            "committed-stage target exists",
        ));
    }
    ensure_regular_file(from)?;
    fs::rename(from, to)
}

pub(super) fn path_exists(path: &Path) -> io::Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}
