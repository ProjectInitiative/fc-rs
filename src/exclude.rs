use globset::{GlobBuilder, GlobMatcher};

pub struct ExcludePattern {
    inner: GlobMatcher,
}

impl ExcludePattern {
    pub fn new(pattern: &str) -> Result<Self, String> {
        let glob = GlobBuilder::new(pattern)
            .case_insensitive(true)
            .literal_separator(true)
            .build()
            .map_err(|e| format!("invalid exclude pattern: {}", e))?;
        Ok(ExcludePattern {
            inner: glob.compile_matcher(),
        })
    }

    pub fn matches(&self, name: &str) -> bool {
        self.inner.is_match(name)
    }
}

pub struct ExcludeList {
    patterns: Vec<ExcludePattern>,
}

impl ExcludeList {
    pub fn new() -> Self {
        ExcludeList {
            patterns: Vec::new(),
        }
    }

    pub fn add(&mut self, pattern: &str) -> Result<(), String> {
        self.patterns.push(ExcludePattern::new(pattern)?);
        Ok(())
    }

    pub fn is_excluded(&self, name: &str) -> bool {
        self.patterns.iter().any(|p| p.matches(name))
    }

    pub fn is_empty(&self) -> bool {
        self.patterns.is_empty()
    }
}

impl Default for ExcludeList {
    fn default() -> Self {
        Self::new()
    }
}
