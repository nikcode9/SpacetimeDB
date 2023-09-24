use crate::AlgebraicType;
use crate::AlgebraicValue;
use crate::BuiltinType;
use crate::BuiltinValue;
use crate::MapType;
use crate::ProductType;
use crate::ProductTypeElement;
use crate::ProductValue;
use crate::SumType;
use crate::SumTypeVariant;
use crate::SumValue;
use core::mem::size_of;

pub mod page;

/// Returns the first `N` elements of the slice, or `None` if it has fewer than `N` elements.
pub const fn first_chunk<T, const N: usize>(slice: &[T]) -> Option<&[T; N]> {
    // Implementation borrowed from standard library, see `<slice>::first_chunk`.
    if slice.len() < N {
        None
    } else {
        // SAFETY: We explicitly check for the correct number of elements,
        //   and do not let the reference outlive the slice.
        Some(unsafe { &*(slice.as_ptr() as *const [T; N]) })
    }
}

/// Returns the first `N` elements of the slice, or `None` if it has fewer than `N` elements.
pub fn first_chunk_unwrap<const N: usize>(slice: &[u8]) -> [u8; N] {
    *first_chunk(slice).unwrap()
}

pub trait FixedSizeOf {
    /// Returns the fixed size in terms of bytes required to store `Self`.
    fn fixed_size_of(&self) -> usize;
}

impl FixedSizeOf for AlgebraicType {
    fn fixed_size_of(&self) -> usize {
        match self {
            Self::Sum(ty) => ty.fixed_size_of(),
            Self::Product(ty) => ty.fixed_size_of(),
            Self::Ref(_) => size_of::<u32>(), // Needs typespace.
            &Self::Bool => size_of::<bool>(),
            &Self::I8 => size_of::<i8>(),
            &Self::U8 => size_of::<u8>(),
            &Self::I16 => size_of::<i16>(),
            &Self::U16 => size_of::<u16>(),
            &Self::I32 => size_of::<i32>(),
            &Self::U32 => size_of::<u32>(),
            &Self::I64 => size_of::<i64>(),
            &Self::U64 => size_of::<u64>(),
            &Self::I128 => size_of::<i128>(),
            &Self::U128 => size_of::<u128>(),
            &Self::F32 => size_of::<f32>(),
            &Self::F64 => size_of::<f64>(),
            // We store at most 32 bytes inline.
            // Longer strings are put in variable storage.
            &Self::String => 32 * size_of::<u8>(),
            // TODO: Content address?
            Self::Builtin(BuiltinType::Array(ty)) => ty.elem_ty.fixed_size_of() * 32,
            Self::Builtin(BuiltinType::Map(ty)) => ty.fixed_size_of(),
        }
    }
}

impl FixedSizeOf for SumTypeVariant {
    fn fixed_size_of(&self) -> usize {
        self.algebraic_type.fixed_size_of()
    }
}

impl FixedSizeOf for SumType {
    fn fixed_size_of(&self) -> usize {
        size_of::<u8>() + self.variants.iter().map(<_>::fixed_size_of).max().unwrap_or(0)
    }
}

impl FixedSizeOf for ProductTypeElement {
    fn fixed_size_of(&self) -> usize {
        self.algebraic_type.fixed_size_of()
    }
}

impl FixedSizeOf for ProductType {
    fn fixed_size_of(&self) -> usize {
        self.elements.iter().map(<_>::fixed_size_of).sum()
    }
}

impl FixedSizeOf for MapType {
    fn fixed_size_of(&self) -> usize {
        (self.key_ty.fixed_size_of() + self.ty.fixed_size_of()) * 32
    }
}

type Buffer = Vec<u8>;
type FlatBuffer<'a> = &'a [u8];

struct Variables {
    variables: Vec<Vec<u8>>,
}

struct MyVars<'a> {
    vars: &'a Vec<u8>,
}

pub trait SerializeFlat {
    type FlatValue<'a>
    where
        Self: 'a;

    fn serialize<'a>(&self, buffer: &'a mut Buffer) -> Self::FlatValue<'a>;
}

pub struct FlatAlgebraicValue<'a> {
    buffer: FlatBuffer<'a>,
}

impl FlatAlgebraicValue<'_> {
    pub fn nest(&self, ty: &AlgebraicType) -> (usize, AlgebraicValue) {
        use BuiltinType::*;

        match ty {
            AlgebraicType::Ref(_) => todo!(), // Needs typespace.
            AlgebraicType::Sum(ty) => {
                let flat_sum = FlatSumValue { buffer: self.buffer };
                let (len, sum) = flat_sum.nest(ty);
                (len, AlgebraicValue::Sum(sum))
            }
            AlgebraicType::Product(ty) => {
                let flat_prod = FlatProductValue { buffer: self.buffer };
                let (len, prod) = flat_prod.nest(ty);
                (len, AlgebraicValue::Product(prod))
            }
            &AlgebraicType::Bool => (1, self.as_bool_unchecked().into()),
            &AlgebraicType::I8 => (1, self.as_i8_unchecked().into()),
            &AlgebraicType::U8 => (1, self.as_u8_unchecked().into()),
            &AlgebraicType::I16 => (2, self.as_i16_unchecked().into()),
            &AlgebraicType::U16 => (2, self.as_u16_unchecked().into()),
            &AlgebraicType::I32 => (4, self.as_i32_unchecked().into()),
            &AlgebraicType::U32 => (4, self.as_u32_unchecked().into()),
            &AlgebraicType::I64 => (8, self.as_i64_unchecked().into()),
            &AlgebraicType::U64 => (8, self.as_u64_unchecked().into()),
            &AlgebraicType::I128 => (16, self.as_i128_unchecked().into()),
            &AlgebraicType::U128 => (16, self.as_u128_unchecked().into()),
            &AlgebraicType::F32 => (4, self.as_f32_unchecked().into()),
            &AlgebraicType::F64 => (8, self.as_f64_unchecked().into()),
            &AlgebraicType::String => todo!(),
            AlgebraicType::Builtin(Array(_)) => todo!(),
            AlgebraicType::Builtin(Map(_)) => todo!(),
        }
    }

    fn as_bool_unchecked(&self) -> bool {
        self.buffer[0] != 0
    }

    fn as_i8_unchecked(&self) -> i8 {
        self.buffer[0] as i8
    }

    fn as_u8_unchecked(&self) -> u8 {
        self.buffer[0]
    }

    fn as_i16_unchecked(&self) -> i16 {
        i16::from_le_bytes(first_chunk_unwrap(self.buffer))
    }

    fn as_u16_unchecked(&self) -> u16 {
        u16::from_le_bytes(first_chunk_unwrap(self.buffer))
    }

    fn as_i32_unchecked(&self) -> i32 {
        i32::from_le_bytes(first_chunk_unwrap(self.buffer))
    }

    fn as_u32_unchecked(&self) -> u32 {
        u32::from_le_bytes(first_chunk_unwrap(self.buffer))
    }

    fn as_i64_unchecked(&self) -> i64 {
        i64::from_le_bytes(first_chunk_unwrap(self.buffer))
    }

    fn as_u64_unchecked(&self) -> u64 {
        u64::from_le_bytes(first_chunk_unwrap(self.buffer))
    }

    fn as_i128_unchecked(&self) -> i128 {
        i128::from_le_bytes(first_chunk_unwrap(self.buffer))
    }

    fn as_u128_unchecked(&self) -> u128 {
        u128::from_le_bytes(first_chunk_unwrap(self.buffer))
    }

    fn as_f32_unchecked(&self) -> f32 {
        f32::from_le_bytes(first_chunk_unwrap(self.buffer))
    }

    fn as_f64_unchecked(&self) -> f64 {
        f64::from_le_bytes(first_chunk_unwrap(self.buffer))
    }
}

impl SerializeFlat for AlgebraicValue {
    type FlatValue<'a> = FlatAlgebraicValue<'a> where Self: 'a;

    fn serialize<'a>(&self, buffer: &'a mut Buffer) -> Self::FlatValue<'a> {
        let start = buffer.len();
        dbg!(start);

        use BuiltinValue::*;
        match self {
            Self::Sum(v) => {
                v.serialize(buffer);
            }
            Self::Product(v) => {
                v.serialize(buffer);
            }
            Self::Builtin(Bool(v)) => buffer.push(*v as u8),
            Self::Builtin(I8(v)) => buffer.extend(v.to_le_bytes()),
            Self::Builtin(U8(v)) => buffer.extend(v.to_le_bytes()),
            Self::Builtin(I16(v)) => buffer.extend(v.to_le_bytes()),
            Self::Builtin(U16(v)) => buffer.extend(v.to_le_bytes()),
            Self::Builtin(I32(v)) => buffer.extend(v.to_le_bytes()),
            Self::Builtin(U32(v)) => buffer.extend(v.to_le_bytes()),
            Self::Builtin(I64(v)) => buffer.extend(v.to_le_bytes()),
            Self::Builtin(U64(v)) => buffer.extend(v.to_le_bytes()),
            Self::Builtin(I128(v)) => buffer.extend(v.to_le_bytes()),
            Self::Builtin(U128(v)) => buffer.extend(v.to_le_bytes()),
            Self::Builtin(F32(v)) => buffer.extend(v.into_inner().to_le_bytes()),
            Self::Builtin(F64(v)) => buffer.extend(v.into_inner().to_le_bytes()),
            Self::Builtin(String(v)) => (),
            Self::Builtin(Array { val: v }) => (),
            Self::Builtin(Map { val: v }) => (),
        }

        dbg!(buffer.len());

        Self::FlatValue {
            buffer: &buffer[start..buffer.len()],
        }
    }
}

pub struct FlatSumValue<'a> {
    buffer: FlatBuffer<'a>,
}

impl FlatSumValue<'_> {
    /// Returns a traditional un-flattened sum value.
    pub fn nest(&self, ty: &SumType) -> (usize, SumValue) {
        // Deserialize the tag.
        let tag = self.tag();

        // Deserialize the value part.
        let buffer_ty = &ty.variants[tag as usize].algebraic_type;
        let (len, value) = self.value().nest(buffer_ty);
        let value = Box::new(value);

        // Stitch together.
        (1 + len, SumValue { tag, value })
    }

    /// Returns the tag of this flat sum value.
    pub fn tag(&self) -> u8 {
        self.buffer[0]
    }

    /// Returns the value / data part of this flat sum value.
    pub fn value(&self) -> FlatAlgebraicValue<'_> {
        let buffer = &self.buffer[1..];
        FlatAlgebraicValue { buffer }
    }
}

impl SerializeFlat for SumValue {
    type FlatValue<'a> = FlatAlgebraicValue<'a> where Self: 'a;

    fn serialize<'a>(&self, buffer: &'a mut Buffer) -> Self::FlatValue<'a> {
        let start = buffer.len();

        buffer.push(self.tag);
        self.value.serialize(buffer);

        Self::FlatValue {
            buffer: &buffer[start..buffer.len()],
        }
    }
}

pub struct FlatProductValue<'a> {
    buffer: FlatBuffer<'a>,
}

impl FlatProductValue<'_> {
    pub fn get_element(&self, ty: &ProductType, index: usize) -> FlatAlgebraicValue<'_> {
        let tys = &ty.elements;
        let elem_size = tys[index].fixed_size_of();
        let offset = tys[..index].iter().map(<_>::fixed_size_of).sum::<usize>();
        let buffer = &self.buffer[offset..offset + elem_size];
        FlatAlgebraicValue { buffer }
    }

    /// Returns a traditional un-flattened product value.
    pub fn nest(&self, ty: &ProductType) -> (usize, ProductValue) {
        let mut buffer = self.buffer;

        let elements = ty
            .elements
            .iter()
            .map(|elem| {
                let (len, value) = FlatAlgebraicValue { buffer }.nest(&elem.algebraic_type);
                buffer = &buffer[len..];
                value
            })
            .collect();
        let pv = ProductValue { elements };

        let len = self.buffer.len() - buffer.len();
        (len, pv)
    }
}

impl SerializeFlat for ProductValue {
    type FlatValue<'a> = FlatProductValue<'a> where Self: 'a;

    fn serialize<'a>(&self, buffer: &'a mut Buffer) -> Self::FlatValue<'a> {
        let start = buffer.len();
        dbg!(start);

        for elem in &self.elements {
            elem.serialize(buffer);
        }

        dbg!(buffer.len());

        Self::FlatValue {
            buffer: &buffer[start..buffer.len()],
        }
    }
}
