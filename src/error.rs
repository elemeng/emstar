use std::fmt;
use std::path::PathBuf;

pub type Result<T> = std::result::Result<T, StarError>;

/// Errors that can occur during STAR file operations.
///
/// Three variants: I/O errors, file-not-found, and parse errors.
///
/// Implements `std::error::Error` and `From<std::io::Error>`.
///
/// ```
/// use emstar::StarError;
///
/// let err = StarError::FileNotFound("/tmp/foo.star".into());
/// assert!(err.to_string().contains("not found"));
/// ```
#[derive(Debug)]
pub enum StarError {
    /// Wraps `std::io::Error` (file read/write failures).
    Io(std::io::Error),
    /// The specified file path does not exist.
    FileNotFound(PathBuf),
    /// A syntax error was found at a specific line.
    Parse {
        /// 1-based line number where the error occurred.
        line: usize,
        /// Description of what went wrong.
        message: String,
    },
}

impl fmt::Display for StarError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StarError::Io(e) => write!(f, "{}", e),
            StarError::FileNotFound(p) => write!(f, "File not found: {}", p.display()),
            StarError::Parse { line, message } => {
                write!(f, "Parse error at line {}: {}", line, message)
            }
        }
    }
}

impl std::error::Error for StarError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            StarError::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for StarError {
    fn from(e: std::io::Error) -> Self {
        StarError::Io(e)
    }
}
