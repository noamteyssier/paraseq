pub trait Record {
    fn id(&self) -> &[u8];
    fn seq(&self) -> &[u8];
    fn qual(&self) -> Option<&[u8]>;

    /// Convert ID to string reference (UTF-8)
    ///
    /// # Safety
    /// Will panic if ID is not valid UTF-8
    fn id_str(&self) -> &str {
        std::str::from_utf8(self.id()).unwrap()
    }

    /// Convert sequence to string reference (UTF-8)
    ///
    /// # Safety
    /// Will panic if sequence is not valid UTF-8
    fn seq_str(&self) -> &str {
        std::str::from_utf8(self.seq()).unwrap()
    }

    /// Convert quality to string reference (UTF-8)
    ///
    /// # Safety
    /// Will panic if quality is not valid UTF-8
    fn qual_str(&self) -> &str {
        if let Some(qual) = self.qual() {
            std::str::from_utf8(qual).unwrap()
        } else {
            ""
        }
    }
}
