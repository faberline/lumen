//! The cost-only shapes a field value takes: text, lists, nested fields, and
//! the scalar or container `Value` that picks between them.
#![allow(dead_code)]

use std::fmt;
use std::marker::PhantomData;
use std::mem::size_of;

use anyhow::Result;
use serde::de::{self, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};

use crate::ingest::infrastructure::wire_cost::generic::{as_de, grow_array, HeapCost};
use crate::ingest::infrastructure::wire_cost::{add, allocation, mul};

pub(super) struct Text;

impl HeapCost for Text {
    fn heap(&self) -> Result<usize> {
        Ok(0)
    }
}
impl<'de> Deserialize<'de> for Text {
    fn deserialize<D: Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        struct TextVisitor;
        impl Visitor<'_> for TextVisitor {
            type Value = Text;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a string")
            }
            fn visit_str<E: de::Error>(self, s: &str) -> std::result::Result<Text, E> {
                let _ = s;
                Ok(Text)
            }
            fn visit_string<E: de::Error>(self, s: String) -> std::result::Result<Text, E> {
                // Structural preflight already prices this complete token.
                drop(s);
                Ok(Text)
            }
        }
        d.deserialize_string(TextVisitor)
    }
}

pub(super) struct List<T, const WIDTH: usize> {
    heap: usize,
    marker: PhantomData<T>,
}
impl<T, const WIDTH: usize> HeapCost for List<T, WIDTH> {
    fn heap(&self) -> Result<usize> {
        Ok(self.heap)
    }
}
impl<'de, T: Deserialize<'de> + HeapCost, const WIDTH: usize> Deserialize<'de> for List<T, WIDTH> {
    fn deserialize<D: Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        struct ListVisitor<T, const WIDTH: usize>(PhantomData<T>);
        impl<'de, T: Deserialize<'de> + HeapCost, const WIDTH: usize> Visitor<'de>
            for ListVisitor<T, WIDTH>
        {
            type Value = List<T, WIDTH>;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("an array")
            }
            fn visit_seq<A: SeqAccess<'de>>(
                self,
                mut a: A,
            ) -> std::result::Result<Self::Value, A::Error> {
                let (mut count, mut heap) = (0, 0);
                // Do not allocate from an untrusted size_hint.
                while let Some(value) = a.next_element::<T>()? {
                    count = add(count, 1).map_err(as_de)?;
                    heap = add(heap, value.heap().map_err(as_de)?).map_err(as_de)?;
                }
                // Wire item vectors can coexist with the public target vector
                // during move conversion. Count both even if collect reuses it.
                heap = add(heap, grow_array(count, WIDTH).map_err(as_de)?).map_err(as_de)?;
                heap = add(
                    heap,
                    allocation(mul(count, WIDTH).map_err(as_de)?).map_err(as_de)?,
                )
                .map_err(as_de)?;
                Ok(List {
                    heap,
                    marker: PhantomData,
                })
            }
        }
        d.deserialize_seq(ListVisitor::<T, WIDTH>(PhantomData))
    }
}

pub(super) struct Fields<T, const WIDTH: usize> {
    heap: usize,
    marker: PhantomData<T>,
}
impl<T, const WIDTH: usize> HeapCost for Fields<T, WIDTH> {
    fn heap(&self) -> Result<usize> {
        Ok(self.heap)
    }
}
impl<'de, T: Deserialize<'de> + HeapCost, const WIDTH: usize> Deserialize<'de>
    for Fields<T, WIDTH>
{
    fn deserialize<D: Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        struct FieldsVisitor<T, const WIDTH: usize>(PhantomData<T>);
        impl<'de, T: Deserialize<'de> + HeapCost, const WIDTH: usize> Visitor<'de>
            for FieldsVisitor<T, WIDTH>
        {
            type Value = Fields<T, WIDTH>;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a field map")
            }
            fn visit_map<A: MapAccess<'de>>(
                self,
                mut a: A,
            ) -> std::result::Result<Self::Value, A::Error> {
                let node = allocation(
                    add(
                        mul(11, add(size_of::<String>(), WIDTH).map_err(as_de)?).map_err(as_de)?,
                        mul(12, size_of::<usize>()).map_err(as_de)?,
                    )
                    .map_err(as_de)?,
                )
                .map_err(as_de)?;
                let mut heap = 0;
                while let Some(key) = a.next_key::<Text>()? {
                    let value = a.next_value::<T>()?;
                    heap = add(
                        heap,
                        add(
                            mul(node, 2).map_err(as_de)?,
                            add(key.heap().map_err(as_de)?, value.heap().map_err(as_de)?)
                                .map_err(as_de)?,
                        )
                        .map_err(as_de)?,
                    )
                    .map_err(as_de)?;
                }
                Ok(Fields {
                    heap,
                    marker: PhantomData,
                })
            }
        }
        d.deserialize_map(FieldsVisitor::<T, WIDTH>(PhantomData))
    }
}

pub(super) struct Value(usize);

impl HeapCost for Value {
    fn heap(&self) -> Result<usize> {
        Ok(self.0)
    }
}
// An array may hold only numbers or strings. A scalar marker identifies its
// alternative without keeping its content or trying an owned Vec decoder.
struct Scalar {
    number: bool,
}
impl<'de> Deserialize<'de> for Scalar {
    fn deserialize<D: Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        struct ScalarVisitor;
        impl Visitor<'_> for ScalarVisitor {
            type Value = Scalar;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a number or string")
            }
            fn visit_str<E: de::Error>(self, _s: &str) -> std::result::Result<Scalar, E> {
                Ok(Scalar { number: false })
            }
            fn visit_string<E: de::Error>(self, _s: String) -> std::result::Result<Scalar, E> {
                Ok(Scalar { number: false })
            }
            fn visit_f64<E: de::Error>(self, _: f64) -> std::result::Result<Scalar, E> {
                Ok(Scalar { number: true })
            }
            fn visit_i64<E: de::Error>(self, n: i64) -> std::result::Result<Scalar, E> {
                self.visit_f64(n as f64)
            }
            fn visit_u64<E: de::Error>(self, n: u64) -> std::result::Result<Scalar, E> {
                self.visit_f64(n as f64)
            }
        }
        d.deserialize_any(ScalarVisitor)
    }
}
impl<'de> Deserialize<'de> for Value {
    fn deserialize<D: Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        struct ValueVisitor;
        impl<'de> Visitor<'de> for ValueVisitor {
            type Value = Value;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a field value")
            }
            fn visit_str<E: de::Error>(self, s: &str) -> std::result::Result<Value, E> {
                let _ = s;
                Ok(Value(0))
            }
            fn visit_string<E: de::Error>(self, s: String) -> std::result::Result<Value, E> {
                drop(s);
                Ok(Value(0))
            }
            fn visit_f64<E: de::Error>(self, _: f64) -> std::result::Result<Value, E> {
                Ok(Value(0))
            }
            fn visit_i64<E: de::Error>(self, n: i64) -> std::result::Result<Value, E> {
                self.visit_f64(n as f64)
            }
            fn visit_u64<E: de::Error>(self, n: u64) -> std::result::Result<Value, E> {
                self.visit_f64(n as f64)
            }
            fn visit_seq<A: SeqAccess<'de>>(
                self,
                mut a: A,
            ) -> std::result::Result<Value, A::Error> {
                let (mut count, mut kind) = (0, None);
                while let Some(value) = a.next_element::<Scalar>()? {
                    if kind.is_some_and(|number| number != value.number) {
                        return Err(de::Error::custom("mixed field-value array"));
                    }
                    kind = Some(value.number);
                    count = add(count, 1).map_err(as_de)?;
                }
                // The private FieldValue visitor directly grows one typed
                // vector. There is no untagged Content tree or cloned strings.
                let width = if kind == Some(false) {
                    size_of::<String>()
                } else {
                    size_of::<f32>()
                };
                let heap = grow_array(count, width).map_err(as_de)?;
                Ok(Value(heap))
            }
        }
        d.deserialize_any(ValueVisitor)
    }
}
