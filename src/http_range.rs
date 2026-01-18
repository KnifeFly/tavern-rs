#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RangeSpec {
    pub start: u64,
    pub end: u64,
}

#[derive(Debug)]
pub enum RangeError {
    Invalid,
    Unsatisfiable,
}

pub fn parse_range(header: &str, size: u64) -> Result<RangeSpec, RangeError> {
    let header = header.trim();
    if !header.starts_with("bytes=") {
        return Err(RangeError::Invalid);
    }
    let range = &header[6..];
    if range.is_empty() {
        return Err(RangeError::Invalid);
    }

    let parts: Vec<&str> = range.split('-').collect();
    if parts.len() != 2 {
        return Err(RangeError::Invalid);
    }

    let start_str = parts[0].trim();
    let end_str = parts[1].trim();

    if start_str.is_empty() {
        // suffix range: bytes=-N
        let suffix: u64 = end_str.parse().map_err(|_| RangeError::Invalid)?;
        if suffix == 0 || size == 0 {
            return Err(RangeError::Unsatisfiable);
        }
        let start = size.saturating_sub(suffix);
        let end = size.saturating_sub(1);
        return Ok(RangeSpec { start, end });
    }

    let start: u64 = start_str.parse().map_err(|_| RangeError::Invalid)?;
    if size > 0 && start >= size {
        return Err(RangeError::Unsatisfiable);
    }

    let end = if end_str.is_empty() {
        if size == 0 {
            return Err(RangeError::Invalid);
        }
        size - 1
    } else {
        let end_val: u64 = end_str.parse().map_err(|_| RangeError::Invalid)?;
        if size > 0 && end_val >= size {
            size - 1
        } else {
            end_val
        }
    };

    if end < start {
        return Err(RangeError::Unsatisfiable);
    }

    Ok(RangeSpec { start, end })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContentRange {
    pub start: u64,
    pub end: u64,
    pub size: u64,
}

pub fn parse_content_range(header: &str) -> Option<ContentRange> {
    // bytes start-end/size
    let header = header.trim();
    if !header.starts_with("bytes") {
        return None;
    }
    let parts: Vec<&str> = header.split_whitespace().collect();
    if parts.len() != 2 {
        return None;
    }
    let range_and_size = parts[1];
    let mut iter = range_and_size.split('/');
    let range_part = iter.next()?;
    let size_part = iter.next()?;
    let mut range_iter = range_part.split('-');
    let start: u64 = range_iter.next()?.parse().ok()?;
    let end: u64 = range_iter.next()?.parse().ok()?;
    let size: u64 = size_part.parse().ok()?;
    Some(ContentRange { start, end, size })
}

pub fn build_content_range(start: u64, end: u64, size: u64) -> String {
    format!("bytes {}-{}/{}", start, end, size)
}
