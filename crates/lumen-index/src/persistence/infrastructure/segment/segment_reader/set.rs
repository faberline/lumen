//! Set lookups: per-doc members, element postings and counts.

use std::borrow::Cow;

use crate::persistence::infrastructure::segment::codecs::{decode_docid_block, read_varint};
use crate::persistence::infrastructure::segment::{
    SegmentReader, ROLE_DICT, ROLE_SET_OFFSETS, ROLE_SET_PACKED, ROLE_SET_POSTINGS,
};

impl SegmentReader {
    /// The set members of doc `id` as owned strings (ascending), or `None` if
    /// `id` is out of range, the doc has no set value (present-bit clear), or
    /// any column is torn — never panics. An empty `Some(vec![])` is a
    /// present-but-empty set. The CSR offsets + packed dict-id columns are read
    /// zero-copy off the mmap; only the dictionary strings are materialized
    /// (from the moka-cached blocks). See [`Self::keyword_at`] for why the
    /// strings are owned rather than `&str`.
    pub fn set_at(&self, id: u32) -> Option<Vec<String>> {
        if !self.is_present(id) {
            return None;
        }
        let offsets = self.u32_column(ROLE_SET_OFFSETS)?;
        let packed = self.u32_column(ROLE_SET_PACKED)?;
        // CSR: doc i's members are packed[offsets[i]..offsets[i+1]]. Guard the
        // off-by-one — offsets has n_docs+1 entries, so offsets[id+1] exists.
        let lo = *offsets.get(id as usize)? as usize;
        let hi = *offsets.get(id as usize + 1)? as usize;
        if hi < lo || hi > packed.len() {
            return None;
        }
        let mut out = Vec::with_capacity(hi - lo);
        for member in 0..(hi - lo) {
            // A torn dict entry aborts the whole doc rather than silently
            // dropping a member (a partial member set would be a wrong answer).
            out.push(self.set_member_at_cow(id, member as u32)?.into_owned());
        }
        Some(out)
    }

    /// Internal raw-or-legacy access to one set member. The ordinal is the
    /// member's position within this row, not its dictionary id.
    pub(crate) fn set_member_at_cow(&self, id: u32, member: u32) -> Option<Cow<'_, str>> {
        if !self.is_present(id) {
            return None;
        }
        let offsets = self.u32_column(ROLE_SET_OFFSETS)?;
        let packed = self.u32_column(ROLE_SET_PACKED)?;
        let lo = *offsets.get(id as usize)? as usize;
        let hi = *offsets.get(id as usize + 1)? as usize;
        let index = lo.checked_add(member as usize)?;
        if index >= hi || hi > packed.len() {
            return None;
        }
        self.dict_value(*packed.get(index)?)
    }

    /// The dict-id of set element `value` in the shared sorted [`ROLE_DICT`]
    /// dictionary, or `None` if absent. The Set, Keyword and Text dicts all
    /// share [`ROLE_DICT`] and the identical sorted layout, so this delegates to
    /// [`Self::text_dict_id`]. Phase 2h-2. Never panics.
    /// `(present, member_count)` without copying the CSR member row. `None`
    /// means its index, offsets or packed range is invalid.
    pub(crate) fn set_row_member_count(&self, id: u32) -> Option<(bool, u32)> {
        if id >= self.n_docs {
            return None;
        }
        if !self.is_present(id) {
            return Some((false, 0));
        }
        let offsets = self.u32_column(ROLE_SET_OFFSETS)?;
        let packed = self.u32_column(ROLE_SET_PACKED)?;
        let lo = *offsets.get(id as usize)? as usize;
        let hi = *offsets.get(id as usize + 1)? as usize;
        if hi < lo || hi > packed.len() {
            return None;
        }
        Some((true, u32::try_from(hi - lo).ok()?))
    }

    /// Decode one Set member for a streaming checkpoint projection.
    pub(crate) fn set_member_at(&self, id: u32, member: u32) -> Option<String> {
        let (present, count) = self.set_row_member_count(id)?;
        if !present || member >= count {
            return None;
        }
        let offsets = self.u32_column(ROLE_SET_OFFSETS)?;
        let packed = self.u32_column(ROLE_SET_PACKED)?;
        let index = (*offsets.get(id as usize)?).checked_add(member)? as usize;
        self.dict_string(*packed.get(index)?)
    }

    fn set_dict_id(&self, value: &str) -> Option<u32> {
        self.text_dict_id(value)
    }

    /// Set element `value`'s INVERTED posting list (the docids whose set
    /// contains `value`) as an ascending docid `RoaringBitmap`, decoded from the
    /// parallel [`ROLE_SET_POSTINGS`] column located by the value's dict-id.
    /// `None` if the value is not in this segment's dictionary or the posting
    /// block is torn — never panics. The docid stream is byte-identical to what
    /// the live `elements[value]` bitmap held at seal, so the segment-driven
    /// membership / Terms / boolean algebra matches the in-RAM path exactly.
    /// Phase 2h-2. The Set analogue of [`Self::keyword_postings`].
    pub fn set_postings(&self, value: &str) -> Option<roaring::RoaringBitmap> {
        let dict_id = self.set_dict_id(value)?;
        // Phase 2m: served through the BOUNDED posting cache, same RAW-immutable +
        // tombstone-after-fetch discipline as `keyword_postings`.
        self.cached_docid_postings(ROLE_SET_POSTINGS, dict_id)
            .map(|p| (*p).clone())
    }

    /// Set element `value`'s document frequency (`df`) = the stored posting
    /// length, or `None` if the value is absent from the dictionary. Decodes
    /// ONLY the LEB128 `count` prefix of the posting blob (cheap — keeps the
    /// boolean planner's rarest-first clause ordering off the full decode).
    /// Phase 2h-2. The Set analogue of [`Self::keyword_df`]. Never panics.
    pub fn set_df(&self, value: &str) -> Option<u64> {
        let dict_id = self.set_dict_id(value)?;
        let (block, within) = self.dict_block_at(ROLE_SET_POSTINGS, dict_id)?;
        let blob = block.get(within)?;
        let mut pos = 0usize;
        read_varint(blob, &mut pos)
    }

    /// Materialize EVERY set element in the [`ROLE_DICT`] string dictionary of a
    /// Set segment paired with its stored INVERTED postings, in dict-id
    /// (ascending lexical) order. `None` if any dict entry or posting block is
    /// torn — never panics. The Set analogue of [`Self::keyword_terms_all`]:
    /// used by the segment-aware duplicate / unique-element enumeration (Phase
    /// 2h-2 FIX), since a sealed Set field dropped its in-RAM `elements` driver
    /// and the only way to walk distinct values is the on-disk dict. The docid
    /// stream is byte-identical to what the live `elements[value]` bitmap held
    /// at seal.
    pub fn set_elements_all(&self) -> Option<Vec<(String, roaring::RoaringBitmap)>> {
        let col = self.column(ROLE_DICT)?;
        let n = u32::try_from(col.elem_count).ok()?;
        let mut out = Vec::with_capacity(n as usize);
        for dict_id in 0..n {
            let el = self.dict_value(dict_id)?.into_owned();
            let (pblock, pwithin) = self.dict_block_at(ROLE_SET_POSTINGS, dict_id)?;
            let blob = pblock.get(pwithin)?;
            let docids = decode_docid_block(blob)?;
            out.push((el, docids.into_iter().collect()));
        }
        Some(out)
    }
}
