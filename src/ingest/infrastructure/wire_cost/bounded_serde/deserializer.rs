//! `QuietDeserializer` as a serde `Deserializer`: each method forwards to the
//! wrapped deserializer through a quiet visitor, JSON through
//! `deserialize_any`.

use serde::de::Visitor;
use serde::Deserializer;

use crate::ingest::infrastructure::wire_cost::bounded_serde::{
    quiet, JsonEnumVisitor, QuietDeserializer, QuietVisitor,
};

impl<'de, D: Deserializer<'de>> Deserializer<'de> for QuietDeserializer<D> {
    type Error = D::Error;
    fn is_human_readable(&self) -> bool {
        self.1
    }
    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, D::Error> {
        if self.1 {
            quiet(self.0.deserialize_any(QuietVisitor(visitor, true)))
        } else {
            quiet(self.0.deserialize_any(QuietVisitor(visitor, false)))
        }
    }
    fn deserialize_bool<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, D::Error> {
        if self.1 {
            quiet(self.0.deserialize_any(QuietVisitor(visitor, true)))
        } else {
            quiet(self.0.deserialize_bool(QuietVisitor(visitor, false)))
        }
    }
    fn deserialize_i8<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, D::Error> {
        if self.1 {
            quiet(self.0.deserialize_any(QuietVisitor(visitor, true)))
        } else {
            quiet(self.0.deserialize_i8(QuietVisitor(visitor, false)))
        }
    }
    fn deserialize_i16<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, D::Error> {
        if self.1 {
            quiet(self.0.deserialize_any(QuietVisitor(visitor, true)))
        } else {
            quiet(self.0.deserialize_i16(QuietVisitor(visitor, false)))
        }
    }
    fn deserialize_i32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, D::Error> {
        if self.1 {
            quiet(self.0.deserialize_any(QuietVisitor(visitor, true)))
        } else {
            quiet(self.0.deserialize_i32(QuietVisitor(visitor, false)))
        }
    }
    fn deserialize_i64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, D::Error> {
        if self.1 {
            quiet(self.0.deserialize_any(QuietVisitor(visitor, true)))
        } else {
            quiet(self.0.deserialize_i64(QuietVisitor(visitor, false)))
        }
    }
    fn deserialize_i128<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, D::Error> {
        if self.1 {
            quiet(self.0.deserialize_any(QuietVisitor(visitor, true)))
        } else {
            quiet(self.0.deserialize_i128(QuietVisitor(visitor, false)))
        }
    }
    fn deserialize_u8<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, D::Error> {
        if self.1 {
            quiet(self.0.deserialize_any(QuietVisitor(visitor, true)))
        } else {
            quiet(self.0.deserialize_u8(QuietVisitor(visitor, false)))
        }
    }
    fn deserialize_u16<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, D::Error> {
        if self.1 {
            quiet(self.0.deserialize_any(QuietVisitor(visitor, true)))
        } else {
            quiet(self.0.deserialize_u16(QuietVisitor(visitor, false)))
        }
    }
    fn deserialize_u32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, D::Error> {
        if self.1 {
            quiet(self.0.deserialize_any(QuietVisitor(visitor, true)))
        } else {
            quiet(self.0.deserialize_u32(QuietVisitor(visitor, false)))
        }
    }
    fn deserialize_u64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, D::Error> {
        if self.1 {
            quiet(self.0.deserialize_any(QuietVisitor(visitor, true)))
        } else {
            quiet(self.0.deserialize_u64(QuietVisitor(visitor, false)))
        }
    }
    fn deserialize_u128<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, D::Error> {
        if self.1 {
            quiet(self.0.deserialize_any(QuietVisitor(visitor, true)))
        } else {
            quiet(self.0.deserialize_u128(QuietVisitor(visitor, false)))
        }
    }
    fn deserialize_f32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, D::Error> {
        if self.1 {
            quiet(self.0.deserialize_any(QuietVisitor(visitor, true)))
        } else {
            quiet(self.0.deserialize_f32(QuietVisitor(visitor, false)))
        }
    }
    fn deserialize_f64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, D::Error> {
        if self.1 {
            quiet(self.0.deserialize_any(QuietVisitor(visitor, true)))
        } else {
            quiet(self.0.deserialize_f64(QuietVisitor(visitor, false)))
        }
    }
    fn deserialize_char<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, D::Error> {
        if self.1 {
            quiet(self.0.deserialize_any(QuietVisitor(visitor, true)))
        } else {
            quiet(self.0.deserialize_char(QuietVisitor(visitor, false)))
        }
    }
    fn deserialize_str<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, D::Error> {
        if self.1 {
            quiet(self.0.deserialize_any(QuietVisitor(visitor, true)))
        } else {
            quiet(self.0.deserialize_str(QuietVisitor(visitor, false)))
        }
    }
    fn deserialize_string<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, D::Error> {
        if self.1 {
            quiet(self.0.deserialize_any(QuietVisitor(visitor, true)))
        } else {
            quiet(self.0.deserialize_string(QuietVisitor(visitor, false)))
        }
    }
    fn deserialize_bytes<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, D::Error> {
        if self.1 {
            quiet(self.0.deserialize_any(QuietVisitor(visitor, true)))
        } else {
            quiet(self.0.deserialize_bytes(QuietVisitor(visitor, false)))
        }
    }
    fn deserialize_byte_buf<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, D::Error> {
        if self.1 {
            quiet(self.0.deserialize_any(QuietVisitor(visitor, true)))
        } else {
            quiet(self.0.deserialize_byte_buf(QuietVisitor(visitor, false)))
        }
    }
    fn deserialize_unit<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, D::Error> {
        if self.1 {
            quiet(self.0.deserialize_any(QuietVisitor(visitor, true)))
        } else {
            quiet(self.0.deserialize_unit(QuietVisitor(visitor, false)))
        }
    }
    fn deserialize_seq<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, D::Error> {
        if self.1 {
            quiet(self.0.deserialize_any(QuietVisitor(visitor, true)))
        } else {
            quiet(self.0.deserialize_seq(QuietVisitor(visitor, false)))
        }
    }
    fn deserialize_map<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, D::Error> {
        if self.1 {
            quiet(self.0.deserialize_any(QuietVisitor(visitor, true)))
        } else {
            quiet(self.0.deserialize_map(QuietVisitor(visitor, false)))
        }
    }
    fn deserialize_identifier<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, D::Error> {
        if self.1 {
            quiet(self.0.deserialize_any(QuietVisitor(visitor, true)))
        } else {
            quiet(self.0.deserialize_identifier(QuietVisitor(visitor, false)))
        }
    }
    fn deserialize_unit_struct<V: Visitor<'de>>(
        self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value, D::Error> {
        if self.1 {
            quiet(self.0.deserialize_any(QuietVisitor(visitor, true)))
        } else {
            quiet(
                self.0
                    .deserialize_unit_struct(name, QuietVisitor(visitor, false)),
            )
        }
    }
    fn deserialize_tuple<V: Visitor<'de>>(
        self,
        len: usize,
        visitor: V,
    ) -> Result<V::Value, D::Error> {
        if self.1 {
            quiet(self.0.deserialize_any(QuietVisitor(visitor, true)))
        } else {
            quiet(self.0.deserialize_tuple(len, QuietVisitor(visitor, false)))
        }
    }
    fn deserialize_tuple_struct<V: Visitor<'de>>(
        self,
        name: &'static str,
        len: usize,
        visitor: V,
    ) -> Result<V::Value, D::Error> {
        if self.1 {
            quiet(self.0.deserialize_any(QuietVisitor(visitor, true)))
        } else {
            quiet(
                self.0
                    .deserialize_tuple_struct(name, len, QuietVisitor(visitor, false)),
            )
        }
    }
    fn deserialize_struct<V: Visitor<'de>>(
        self,
        name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, D::Error> {
        if self.1 {
            quiet(self.0.deserialize_any(QuietVisitor(visitor, true)))
        } else {
            quiet(
                self.0
                    .deserialize_struct(name, fields, QuietVisitor(visitor, false)),
            )
        }
    }
    fn deserialize_enum<V: Visitor<'de>>(
        self,
        name: &'static str,
        variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, D::Error> {
        if self.1 {
            quiet(self.0.deserialize_any(JsonEnumVisitor(visitor)))
        } else {
            quiet(
                self.0
                    .deserialize_enum(name, variants, QuietVisitor(visitor, false)),
            )
        }
    }
    fn deserialize_option<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, D::Error> {
        quiet(self.0.deserialize_option(QuietVisitor(visitor, self.1)))
    }
    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value, D::Error> {
        quiet(
            self.0
                .deserialize_newtype_struct(name, QuietVisitor(visitor, self.1)),
        )
    }
    fn deserialize_ignored_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, D::Error> {
        quiet(
            self.0
                .deserialize_ignored_any(QuietVisitor(visitor, self.1)),
        )
    }
}
