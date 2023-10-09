use crate::{AlgebraicType, AlgebraicValue, MapType, MapValue, ProductType, ProductValue, SatsVec};

impl crate::Value for AlgebraicValue {
    type Type = AlgebraicType;
}

impl From<AlgebraicValue> for ProductValue {
    fn from(x: AlgebraicValue) -> Self {
        Self { elements: [x].into() }
    }
}

impl From<&AlgebraicValue> for ProductValue {
    fn from(x: &AlgebraicValue) -> Self {
        x.clone().into()
    }
}

impl From<AlgebraicType> for ProductType {
    fn from(x: AlgebraicType) -> Self {
        Self::new([x.into()].into())
    }
}

impl From<MapType> for AlgebraicType {
    fn from(x: MapType) -> Self {
        Box::new(x).into()
    }
}

macro_rules! built_in_into {
    ($native:ty, $kind:ident) => {
        impl From<$native> for AlgebraicValue {
            fn from(x: $native) -> Self {
                Self::$kind(x.into())
            }
        }
    };
}
built_in_into!(u128, U128);
built_in_into!(i128, I128);
built_in_into!(f32, F32);
built_in_into!(f64, F64);
built_in_into!(MapValue, Map);
built_in_into!(SatsVec<u8>, Bytes);
