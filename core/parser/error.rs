use std::error;
use std::fmt;
use std::io;

/// SQL lexer and parser errors
#[non_exhaustive]
#[derive(Debug, miette::Diagnostic)]
#[diagnostic()]
pub enum Error {
    /// I/O Error
    Io(io::Error),
    /// Lexer error
    UnrecognizedToken(usize, #[label("here")] Option<miette::SourceSpan>),
    /// Missing quote or double-quote or backtick
    UnterminatedLiteral(usize, #[label("here")] Option<miette::SourceSpan>),
    /// Missing `]`
    UnterminatedBracket(usize, #[label("here")] Option<miette::SourceSpan>),
    /// Missing `*/`
    UnterminatedBlockComment(usize, #[label("here")] Option<miette::SourceSpan>),
    /// Invalid parameter name
    BadVariableName(usize, #[label("here")] Option<miette::SourceSpan>),
    /// Invalid number format
    #[diagnostic(help("Invalid digit at `{0}`"))]
    BadNumber(
        usize,
        #[label("here")] Option<miette::SourceSpan>,
        String, // Holds the offending number as a string
    ),
    #[diagnostic(help("Invalid digit at `{0}`"))]
    BadFractionalPart(
        usize,
        #[label("here")] Option<miette::SourceSpan>,
        String, // Holds the offending number as a string
    ),
    #[diagnostic(help("Invalid digit at `{0}`"))]
    BadExponentPart(
        usize,
        #[label("here")] Option<miette::SourceSpan>,
        String, // Holds the offending number as a string
    ),
    /// Invalid or missing sign after `!`
    ExpectedEqualsSign(usize, #[label("here")] Option<miette::SourceSpan>),
    /// BLOB literals are string literals containing hexadecimal data and preceded by a single "x" or "X" character.
    MalformedBlobLiteral(usize, #[label("here")] Option<miette::SourceSpan>),
    /// Hexadecimal integer literals follow the C-language notation of "0x" or "0X" followed by hexadecimal digits.
    MalformedHexInteger(
        usize,
        #[label("here")] Option<miette::SourceSpan>,
        #[help] Option<&'static str>,
    ),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Self::Io(ref err) => err.fmt(f),
            Self::UnrecognizedToken(pos, _) => {
                write!(f, "unrecognized token at {:?}", pos)
            }
            Self::UnterminatedLiteral(pos, _) => {
                write!(f, "non-terminated literal at {:?}", pos)
            }
            Self::UnterminatedBracket(pos, _) => {
                write!(f, "non-terminated bracket at {:?}", pos)
            }
            Self::UnterminatedBlockComment(pos, _) => {
                write!(f, "non-terminated block comment at {:?}", pos)
            }
            Self::BadVariableName(pos, _) => write!(f, "bad variable name at {:?}", pos),
            Self::BadNumber(pos, _, _) => write!(f, "bad number at {:?}", pos),
            Self::BadFractionalPart(pos, _, _) => {
                write!(f, "bad fractional part at {:?}", pos)
            }
            Self::BadExponentPart(pos, _, _) => {
                write!(f, "bad exponent part at {:?}", pos)
            }
            Self::ExpectedEqualsSign(pos, _) => write!(f, "expected = sign at {:?}", pos),
            Self::MalformedBlobLiteral(pos, _) => {
                write!(f, "malformed blob literal at {:?}", pos)
            }
            Self::MalformedHexInteger(pos, _, _) => {
                write!(f, "malformed hex integer at {:?}", pos)
            }
        }
    }
}

impl error::Error for Error {}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Self::Io(err)
    }
}
