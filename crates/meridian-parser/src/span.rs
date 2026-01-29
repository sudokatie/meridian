//! Source location tracking.

/// A span in the source code.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Span {
    /// Byte offset of the start.
    pub start: usize,
    /// Byte offset of the end (exclusive).
    pub end: usize,
}

impl Span {
    /// Create a new span.
    pub fn new(start: usize, end: usize) -> Self {
        Self { start, end }
    }

    /// Merge two spans into one that covers both.
    pub fn merge(self, other: Span) -> Span {
        Span::new(self.start.min(other.start), self.end.max(other.end))
    }

    /// Get the length of the span.
    pub fn len(&self) -> usize {
        self.end - self.start
    }

    /// Check if the span is empty.
    pub fn is_empty(&self) -> bool {
        self.start == self.end
    }
}
