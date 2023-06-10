use std::sync::Arc;

use arrow::array::{
    ArrayRef, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, UInt16Array, UInt32Array,
    UInt64Array, UInt8Array,
};
use clam::number::Number;
use ndarray_npy::{ReadableElement, WritableElement};

pub(crate) trait IntoArrowArray: Number + WritableElement + ReadableElement {
    fn into_arrow_array(slice: &[Self]) -> ArrayRef;
}

impl IntoArrowArray for u8 {
    fn into_arrow_array(slice: &[Self]) -> ArrayRef {
        Arc::new(UInt8Array::from_iter(slice.iter().map(|&v| Some(v))))
    }
}

impl IntoArrowArray for i8 {
    fn into_arrow_array(slice: &[Self]) -> ArrayRef {
        Arc::new(Int8Array::from_iter(slice.iter().map(|&v| Some(v))))
    }
}

impl IntoArrowArray for u16 {
    fn into_arrow_array(slice: &[Self]) -> ArrayRef {
        Arc::new(UInt16Array::from_iter(slice.iter().map(|&v| Some(v))))
    }
}

impl IntoArrowArray for i16 {
    fn into_arrow_array(slice: &[Self]) -> ArrayRef {
        Arc::new(Int16Array::from_iter(slice.iter().map(|&v| Some(v))))
    }
}

impl IntoArrowArray for u32 {
    fn into_arrow_array(slice: &[Self]) -> ArrayRef {
        Arc::new(UInt32Array::from_iter(slice.iter().map(|&v| Some(v))))
    }
}

impl IntoArrowArray for i32 {
    fn into_arrow_array(slice: &[Self]) -> ArrayRef {
        Arc::new(Int32Array::from_iter(slice.iter().map(|&v| Some(v))))
    }
}

impl IntoArrowArray for u64 {
    fn into_arrow_array(slice: &[Self]) -> ArrayRef {
        Arc::new(UInt64Array::from_iter(slice.iter().map(|&v| Some(v))))
    }
}

impl IntoArrowArray for i64 {
    fn into_arrow_array(slice: &[Self]) -> ArrayRef {
        Arc::new(Int64Array::from_iter(slice.iter().map(|&v| Some(v))))
    }
}

impl IntoArrowArray for f32 {
    fn into_arrow_array(slice: &[Self]) -> ArrayRef {
        Arc::new(Float32Array::from_iter(slice.iter().map(|&v| Some(v))))
    }
}

impl IntoArrowArray for f64 {
    fn into_arrow_array(slice: &[Self]) -> ArrayRef {
        Arc::new(Float64Array::from_iter(slice.iter().map(|&v| Some(v))))
    }
}
