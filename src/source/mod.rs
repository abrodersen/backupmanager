
mod lvm;

use std::fs::{self, Metadata, ReadDir};
use std::mem;

use std::path::{Path, PathBuf};

use failure::Error;

pub trait Source {
    type S: Snapshot;

    fn snapshot(&self) -> Result<Self::S, Error>;
}

pub trait Snapshot {
    fn files<'a>(&'a self) -> Result<Files<'a>, Error>;
    fn destroy(self) -> Result<(), Error>;
}

pub struct Files<'a> {
    base: &'a Path,
    current: ReadDir,
    stack: Vec<ReadDir>,
}

impl<'a> Files<'a> {
    fn new(base: &'a Path) -> Result<Files<'a>, Error> {
        let start = fs::read_dir(base)?;
        Ok(Files {
            base: base,
            current: start,
            stack: Vec::new(),
        })
    }

    fn next_file(&mut self) -> Option<Result<(PathBuf, Metadata), Error>> {
        loop {
            let entry = match self.current.next() {
                Some(Ok(e)) => e,
                Some(Err(e)) => return Some(Err(e.into())),
                None => match self.stack.pop() {
                    Some(parent) => {
                        self.current = parent;
                        continue
                    },
                    None => return None,
                },
            };
            
            let metadata = match entry.metadata() {
                Ok(m) => m,
                Err(e) => return Some(Err(e.into())),
            };

            let file_type = metadata.file_type();

            if file_type.is_file() || file_type.is_symlink() {
                return Some(Ok((entry.path(), metadata)));
            }

            if file_type.is_dir() {
                let child = match fs::read_dir(entry.path()) {
                    Ok(list) => list,
                    Err(e) => return Some(Err(e.into())),
                };
                let parent = mem::replace(&mut self.current, child);
                self.stack.push(parent);
                return Some(Ok((entry.path(), metadata)));
            }
        }
    }
}

impl<'a> Iterator for Files<'a> {
    type Item = Result<(PathBuf, Metadata), Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_file()
    }
}

#[cfg(test)]
mod test {

    use super::*;

    use std::fs::{self, File, OpenOptions};
    use std::io::Write;
    use std::os::unix::fs::symlink;
    use std::path::PathBuf;

    use tempfile::TempDir;

    enum PathType {
        File,
        Directory,
        Symlink,
    }

    fn generate_fs_structure(paths: Vec<(&'static str, PathType)>) -> TempDir {
        let dir = TempDir::new().unwrap();

        for (path, kind) in paths {
            let path = dir.path().join(path);

            match kind {
                PathType::Directory => {
                    fs::create_dir_all(&path).unwrap()
                },
                PathType::File => {
                    let parent = path.parent().unwrap();
                    fs::create_dir_all(parent).unwrap();
                    let mut file = File::create(&path).unwrap();
                    write!(file, "").unwrap();
                },
                PathType::Symlink => {
                    symlink("/dev/null", &path).unwrap();
                }
            }
        }

        dir
    }

    fn enumerate(path: &Path) -> Vec<(PathBuf, Metadata)> {
        Files::new(path)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    }

    fn get_relative_paths<'a>(dir: &'a TempDir, files: &'a [(PathBuf, Metadata)]) -> Vec<&'a str> {
        files.iter().map(|x| {
            x.0.strip_prefix(dir.path())
                .unwrap()
                .to_str()
                .unwrap()
        })
        .collect::<Vec<_>>()
    }

    #[test]
    fn list_empty_dir() {
        let dir = generate_fs_structure(vec![]);
        let files = enumerate(dir.path());
        assert!(files.is_empty(), "files should be empty");
    }

    #[test]
    fn list_single_empty_dir() {
        let dirs = vec![
            ("foo", PathType::Directory)
        ];
        let dir = generate_fs_structure(dirs);
        let files = enumerate(dir.path());
        let names = get_relative_paths(&dir, &files);
        assert_eq!(names.len(), 1);
        assert_eq!(names[0], "foo");
    }

    #[test]
    fn list_single_file() {
        let dirs = vec![
            ("foo.bar", PathType::File)
        ];
        let dir = generate_fs_structure(dirs);
        let files = enumerate(dir.path());
        let names = get_relative_paths(&dir, &files);
        assert_eq!(names.len(), 1);
        assert_eq!(names[0], "foo.bar");
    }

    #[test]
    fn list_single_file_in_dir() {
        let dirs = vec![
            ("foo/bar.baz", PathType::File)
        ];
        let dir = generate_fs_structure(dirs);
        let files = enumerate(dir.path());
        let names = get_relative_paths(&dir, &files);
        assert_eq!(names.len(), 2);
        assert_eq!(names[0], "foo");
        assert_eq!(names[1], "foo/bar.baz");
    }

    #[test]
    fn list_multiple_files() {
        let dirs = vec![
            ("bar.baz", PathType::File),
            ("foo.baz", PathType::File),
        ];
        let dir = generate_fs_structure(dirs);
        let files = enumerate(dir.path());
        let names = get_relative_paths(&dir, &files);
        assert_eq!(names.len(), 2);
        assert!(names.contains(&"bar.baz"));
        assert!(names.contains(&"foo.baz"));
    }

    #[test]
    fn list_multiple_files_in_dir() {
        let dirs = vec![
            ("foo/bar", PathType::File),
            ("foo/baz", PathType::File),
        ];
        let dir = generate_fs_structure(dirs);
        let files = enumerate(dir.path());
        let names = get_relative_paths(&dir, &files);
        assert_eq!(names.len(), 3);
        assert_eq!(names[0], "foo");
        assert!(names.contains(&"foo/bar"));
        assert!(names.contains(&"foo/baz"));
    }

    #[test]
    fn list_multiple_files_in_multiple_dirs() {
        let dirs = vec![
            ("foobar/baz", PathType::File),
            ("foo/barbaz", PathType::File),
        ];
        let dir = generate_fs_structure(dirs);
        let files = enumerate(dir.path());
        let names = get_relative_paths(&dir, &files);
        assert_eq!(names.len(), 4);
        assert!(names.contains(&"foobar"));
        assert!(names.contains(&"foobar/baz"));
        assert!(names.contains(&"foo"));
        assert!(names.contains(&"foo/barbaz"));
    }

    #[test]
    fn list_multiple_dirs() {
        let dirs = vec![
            ("foo", PathType::Directory),
            ("bar", PathType::Directory),
        ];
        let dir = generate_fs_structure(dirs);
        let files = enumerate(dir.path());
        let names = get_relative_paths(&dir, &files);
        assert_eq!(names.len(), 2);
    }

    #[test]
    fn list_symlink() {
        let dirs = vec![
            ("foo", PathType::Symlink),
        ];
        let dir = generate_fs_structure(dirs);
        let files = enumerate(dir.path());
        let names = get_relative_paths(&dir, &files);
        assert_eq!(names.len(), 1);
        assert!(names.contains(&"foo"));
    }

    #[test]
    fn list_multiple_symlinks() {
        let dirs = vec![
            ("foo", PathType::Symlink),
            ("bar", PathType::Symlink),
        ];
        let dir = generate_fs_structure(dirs);
        let files = enumerate(dir.path());
        let names = get_relative_paths(&dir, &files);
        assert_eq!(names.len(), 2);
        assert!(names.contains(&"foo"));
        assert!(names.contains(&"bar"));
    }
}
