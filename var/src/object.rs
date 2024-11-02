use faststr::FastStr;
use indexmap::IndexMap;
use numtoa::NumToA;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Object {
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    Boolean(bool),
    String(FastStr),
    Map(IndexMap<FastStr, Object>),
    List(Vec<Object>),
    Null,
}

impl Object {
    pub fn is_null(&self) -> bool {
        match self {
            Self::Null => true,
            _ => false,
        }
    }

    pub fn cast_bool(&self) -> Option<bool> {
        match self {
            Self::Boolean(v) => Some(*v),
            _ => None,
        }
    }

    pub fn cast_string(&self) -> Option<&FastStr> {
        match self {
            Self::String(v) => Some(v),
            _ => None,
        }
    }
    pub fn cast_string_or_err(&self) -> anyhow::Result<&FastStr> {
        match self {
            Self::String(v) => Ok(&v),
            _ => anyhow::bail!("not a string {:?}", self),
        }
    }
    pub fn take_string(self) -> Option<FastStr> {
        match self {
            Self::String(v) => Some(v),
            _ => None,
        }
    }
    pub fn cast_i32(&self) -> Option<i32> {
        match self {
            Self::Int(v) => Some(*v),
            _ => None,
        }
    }
    pub fn cast_i32_or_err(&self) -> anyhow::Result<i32> {
        match self {
            Self::Int(v) => Ok(*v),
            _ => anyhow::bail!("not a i32 {:?}", self),
        }
    }

    pub fn is_map(&self) -> bool {
        match self {
            Object::Map(_) => true,
            _ => false,
        }
    }
    pub fn cast_map(&self) -> Option<&IndexMap<FastStr, Object>> {
        match self {
            Object::Map(v) => Some(v),
            _ => None,
        }
    }
    pub fn cast_map_or_err(&self) -> anyhow::Result<&IndexMap<FastStr, Object>> {
        match self {
            Object::Map(v) => Ok(v),
            _ => anyhow::bail!("not a map {:?}", self),
        }
    }
    pub fn take_map(self) -> Option<IndexMap<FastStr, Object>> {
        match self {
            Object::Map(v) => Some(v),
            _ => None,
        }
    }

    pub fn is_list(&self) -> bool {
        match self {
            Object::List(_) => true,
            _ => false,
        }
    }
    pub fn cast_list_or_err(&self) -> anyhow::Result<&Vec<Object>> {
        match self {
            Object::List(v) => Ok(v),
            _ => anyhow::bail!("not a list {:?}", self),
        }
    }
}

/// json <-> object
impl Object {
    pub fn from_json(json: &serde_json::Value) -> Object {
        match json {
            serde_json::Value::Bool(v) => (*v).into(),
            serde_json::Value::Number(v) => {
                if let Some(v) = v.as_i64() {
                    if v < i32::MAX as i64 && v > i32::MIN as i64 {
                        Object::Int(v as i32)
                    } else {
                        Object::Long(v)
                    }
                } else if let Some(v) = v.as_f64() {
                    if v < f32::MAX as f64 && v > f32::MIN as f64 {
                        Object::Float(v as f32)
                    } else {
                        Object::Double(v)
                    }
                } else {
                    unimplemented!()
                }
            }
            serde_json::Value::String(v) => v.into(),
            serde_json::Value::Object(v) => Self::convert_jsonmap_to_objectmap(v).into(),
            serde_json::Value::Array(v) => Self::convert_jsonlist_to_list(v).into(),
            serde_json::Value::Null => Object::Null,
        }
    }

    pub fn convert_map_to_objectmap(
        jsonmap: &serde_json::Map<String, serde_json::Value>,
    ) -> IndexMap<FastStr, Object> {
        let mut map = IndexMap::with_capacity(jsonmap.len());
        for (k, v) in jsonmap {
            map.insert(k.clone().into(), Self::from_json(v));
        }
        map
    }

    pub fn convert_jsonmap_to_objectmap(
        jsonmap: &serde_json::Map<String, serde_json::Value>,
    ) -> IndexMap<FastStr, Object> {
        let mut map = IndexMap::with_capacity(jsonmap.len());
        for (k, v) in jsonmap {
            map.insert(k.clone().into(), Self::from_json(v));
        }
        map
    }

    pub fn convert_jsonlist_to_list(json_list: &Vec<serde_json::Value>) -> Vec<Object> {
        let mut list = Vec::with_capacity(json_list.len());
        for v in json_list {
            list.push(Self::from_json(v));
        }
        list
    }

    pub fn to_json(&self) -> serde_json::Value {
        match self {
            Object::Int(v) => serde_json::Value::Number((*v).into()),
            Object::Long(v) => serde_json::Value::Number((*v).into()),
            Object::Float(v) => (*v).into(),
            Object::Double(v) => (*v).into(),
            Object::Boolean(v) => serde_json::Value::Bool(*v),
            Object::String(v) => serde_json::Value::String(v.to_string()),
            Object::Map(v) => Self::convert_objectmap_to_json(v),
            Object::List(v) => Self::convert_list_to_json(v),
            Object::Null => serde_json::Value::Null,
        }
    }

    pub fn convert_objectmap_to_json(hash_map: &IndexMap<FastStr, Object>) -> serde_json::Value {
        let mut map = serde_json::Map::with_capacity(hash_map.len());
        for (k, v) in hash_map {
            map.insert(k.to_string(), v.to_json());
        }
        serde_json::Value::Object(map)
    }

    fn convert_list_to_json(list: &Vec<Object>) -> serde_json::Value {
        let mut json_list = Vec::with_capacity(list.len());
        for v in list {
            json_list.push(v.to_json());
        }
        serde_json::Value::Array(json_list)
    }
}

impl Object {
    /// 在已知 Object 实际类型的情况下，不要乱用 to_string 方法，使用 cast_string 方法效率更高
    pub fn to_string(&self) -> FastStr {
        match self {
            Object::Int(v) => (*v).numtoa_str(10, &mut [0; 16]).to_owned().into(),
            Object::Long(v) => (*v).numtoa_str(10, &mut [0; 32]).to_owned().into(),
            Object::Float(v) => ryu::Buffer::new().format(*v).to_owned().into(),
            Object::Double(v) => ryu::Buffer::new().format(*v).to_owned().into(),
            Object::Boolean(v) => {
                if *v {
                    "True".into()
                } else {
                    "False".into()
                }
            }
            Object::String(v) => v.clone(),
            Object::Map(v) => Self::convert_objectmap_to_json(v).to_string().into(),
            Object::List(v) => Self::convert_list_to_json(v).to_string().into(),
            Object::Null => "".into(),
        }
    }

    pub fn estimate_map_memory_used(objectmap: &IndexMap<FastStr, Object>) -> i32 {
        let mut memory_used = 0;
        for (k, v) in objectmap {
            memory_used += k.as_bytes().len() as i32;
            memory_used += v.estimate_memory_used();
        }
        memory_used
    }

    pub fn estimate_list_memory_used(list: &Vec<Object>) -> i32 {
        let mut memory_used = 0;
        for v in list {
            memory_used += v.estimate_memory_used();
        }
        memory_used
    }

    pub fn estimate_memory_used(&self) -> i32 {
        match self {
            Object::Int(_) => 4,
            Object::Long(_) => 8,
            Object::Float(_) => 4,
            Object::Double(_) => 8,
            Object::Boolean(_) => 1,
            Object::String(v) => v.as_bytes().len() as i32,
            Object::Map(v) => Self::estimate_map_memory_used(v),
            Object::List(v) => Self::estimate_list_memory_used(v),
            Object::Null => 1,
        }
    }
}

impl From<i32> for Object {
    fn from(value: i32) -> Self {
        Object::Int(value)
    }
}
impl From<i64> for Object {
    fn from(value: i64) -> Self {
        Object::Long(value)
    }
}
impl From<f32> for Object {
    fn from(value: f32) -> Self {
        Object::Float(value)
    }
}
impl From<f64> for Object {
    fn from(value: f64) -> Self {
        Object::Double(value)
    }
}
impl From<bool> for Object {
    fn from(value: bool) -> Self {
        Object::Boolean(value)
    }
}
impl From<FastStr> for Object {
    fn from(value: FastStr) -> Self {
        Object::String(value)
    }
}
impl From<&FastStr> for Object {
    fn from(value: &FastStr) -> Self {
        Object::String(value.clone())
    }
}
impl From<&str> for Object {
    fn from(value: &str) -> Self {
        Object::String(value.to_owned().into())
    }
}
impl From<&String> for Object {
    fn from(value: &String) -> Self {
        Object::String(value.clone().into())
    }
}
impl From<String> for Object {
    fn from(value: String) -> Self {
        Object::String(value.into())
    }
}
impl From<Vec<Object>> for Object {
    fn from(value: Vec<Object>) -> Self {
        Object::List(value)
    }
}
impl From<IndexMap<FastStr, Object>> for Object {
    fn from(value: IndexMap<FastStr, Object>) -> Self {
        Object::Map(value)
    }
}
