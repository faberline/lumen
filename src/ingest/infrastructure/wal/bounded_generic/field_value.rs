//! A field value's wire form: an array that grows only with the elements it
//! actually sees, and the untagged visitor that moves serde's owned strings
//! into the decoded `FieldValue`.

use std::fmt;

use serde::de::{self, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};

use crate::shared_kernel::types::document::FieldValue;

pub(super) struct GrowVec<T>(pub(super) Vec<T>);

impl<'de, T: Deserialize<'de>> Deserialize<'de> for GrowVec<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        struct GrowVecVisitor<T>(std::marker::PhantomData<T>);

        impl<'de, T: Deserialize<'de>> Visitor<'de> for GrowVecVisitor<T> {
            type Value = GrowVec<T>;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("an array")
            }

            fn visit_seq<A: SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> std::result::Result<Self::Value, A::Error> {
                // serde's normal Vec visitor trusts size_hint. WAL input is
                // durable but untrusted at replay, so only observed elements
                // may make this buffer grow.
                let mut values = Vec::new();
                while let Some(value) = seq.next_element()? {
                    values.push(value);
                }
                Ok(GrowVec(values))
            }
        }

        deserializer.deserialize_seq(GrowVecVisitor(std::marker::PhantomData))
    }
}

pub(super) struct WireFieldValue(pub(super) FieldValue);

enum Scalar {
    Number(f32),
    String(String),
}

impl<'de> Deserialize<'de> for Scalar {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        struct ScalarVisitor;

        impl<'de> Visitor<'de> for ScalarVisitor {
            type Value = Scalar;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a number or string")
            }

            fn visit_str<E: de::Error>(self, value: &str) -> std::result::Result<Self::Value, E> {
                Ok(Scalar::String(value.to_owned()))
            }

            fn visit_string<E: de::Error>(
                self,
                value: String,
            ) -> std::result::Result<Self::Value, E> {
                Ok(Scalar::String(value))
            }

            fn visit_f64<E: de::Error>(self, value: f64) -> std::result::Result<Self::Value, E> {
                Ok(Scalar::Number(value as f32))
            }

            fn visit_i64<E: de::Error>(self, value: i64) -> std::result::Result<Self::Value, E> {
                Ok(Scalar::Number(value as f32))
            }

            fn visit_u64<E: de::Error>(self, value: u64) -> std::result::Result<Self::Value, E> {
                Ok(Scalar::Number(value as f32))
            }
        }

        deserializer.deserialize_any(ScalarVisitor)
    }
}

impl<'de> Deserialize<'de> for WireFieldValue {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        struct FieldValueVisitor;

        impl<'de> Visitor<'de> for FieldValueVisitor {
            type Value = WireFieldValue;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string, number, or homogeneous array of strings or numbers")
            }

            fn visit_str<E: de::Error>(self, value: &str) -> std::result::Result<Self::Value, E> {
                Ok(WireFieldValue(FieldValue::String(value.to_owned())))
            }

            fn visit_string<E: de::Error>(
                self,
                value: String,
            ) -> std::result::Result<Self::Value, E> {
                // This is the ownership boundary: an owned serde token becomes
                // the final FieldValue string without Content or a second copy.
                Ok(WireFieldValue(FieldValue::String(value)))
            }

            fn visit_f64<E: de::Error>(self, value: f64) -> std::result::Result<Self::Value, E> {
                Ok(WireFieldValue(FieldValue::Number(value)))
            }

            fn visit_i64<E: de::Error>(self, value: i64) -> std::result::Result<Self::Value, E> {
                self.visit_f64(value as f64)
            }

            fn visit_u64<E: de::Error>(self, value: u64) -> std::result::Result<Self::Value, E> {
                self.visit_f64(value as f64)
            }

            fn visit_seq<A: SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> std::result::Result<Self::Value, A::Error> {
                // Do not trust `size_hint`: the input can claim an arbitrary
                // count.  Grow only after each observed element.
                let mut numbers = Vec::new();
                let mut strings = Vec::new();
                let mut kind = None;
                while let Some(value) = seq.next_element::<Scalar>()? {
                    match value {
                        Scalar::Number(value) => {
                            if matches!(kind, Some(false)) {
                                return Err(de::Error::custom("mixed field-value array"));
                            }
                            kind = Some(true);
                            numbers.push(value);
                        }
                        Scalar::String(value) => {
                            if matches!(kind, Some(true)) {
                                return Err(de::Error::custom("mixed field-value array"));
                            }
                            kind = Some(false);
                            strings.push(value);
                        }
                    }
                }
                Ok(WireFieldValue(match kind {
                    Some(true) | None => FieldValue::Vector(numbers),
                    Some(false) => FieldValue::StringList(strings),
                }))
            }
        }

        deserializer.deserialize_any(FieldValueVisitor)
    }
}
