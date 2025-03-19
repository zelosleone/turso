mod de;
mod error;
mod json_operations;
mod json_path;
mod jsonb;
mod ser;

use crate::bail_constraint_error;
pub use crate::json::de::from_str;
use crate::json::error::Error as JsonError;
pub use crate::json::json_operations::{
    json_patch, json_remove, json_replace, jsonb_remove, jsonb_replace,
};
use crate::json::json_path::{json_path, JsonPath, PathElement};
pub use crate::json::ser::to_string;
use crate::types::{OwnedValue, Text, TextSubtype};
use crate::{bail_parse_error, json::de::ordered_object};
use indexmap::IndexMap;
use jsonb::{ElementType, Jsonb, JsonbHeader};
use ser::to_string_pretty;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::rc::Rc;
use std::str::FromStr;

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
#[serde(untagged)]
pub enum Val {
    Null,
    Bool(bool),
    Integer(i64),
    Float(f64),
    String(String),
    Array(Vec<Val>),
    Removed,
    #[serde(with = "ordered_object")]
    Object(Vec<(String, Val)>),
}

pub fn get_json(json_value: &OwnedValue, indent: Option<&str>) -> crate::Result<OwnedValue> {
    match json_value {
        OwnedValue::Text(ref t) => {
            // optimization: once we know the subtype is a valid JSON, we do not have
            // to go through parsing JSON and serializing it back to string
            if t.subtype == TextSubtype::Json {
                return Ok(json_value.to_owned());
            }

            let json_val = get_json_value(json_value)?;
            let json = match indent {
                Some(indent) => to_string_pretty(&json_val, indent)?,
                None => to_string(&json_val)?,
            };

            Ok(OwnedValue::Text(Text::json(json)))
        }
        OwnedValue::Blob(b) => {
            let jsonbin = Jsonb::new(b.len(), Some(b));
            jsonbin.is_valid()?;
            Ok(OwnedValue::Text(Text {
                value: Rc::new(jsonbin.to_string()?.into_bytes()),
                subtype: TextSubtype::Json,
            }))
        }
        OwnedValue::Null => Ok(OwnedValue::Null),
        _ => {
            let json_val = get_json_value(json_value)?;
            let json = match indent {
                Some(indent) => to_string_pretty(&json_val, indent)?,
                None => to_string(&json_val)?,
            };

            Ok(OwnedValue::Text(Text::json(json)))
        }
    }
}

pub fn jsonb(json_value: &OwnedValue) -> crate::Result<OwnedValue> {
    let jsonbin = convert_dbtype_to_jsonb(json_value, true);
    match jsonbin {
        Ok(jsonbin) => Ok(OwnedValue::Blob(Rc::new(jsonbin.data()))),
        Err(_) => {
            bail_parse_error!("malformed JSON")
        }
    }
}

fn convert_dbtype_to_jsonb(val: &OwnedValue, strict: bool) -> crate::Result<Jsonb> {
    match val {
        OwnedValue::Text(text) => {
            let res = if text.subtype == TextSubtype::Json || strict {
                // Parse directly as JSON if it's already JSON subtype or strict mode is on
                Jsonb::from_str(text.as_str())
            } else {
                // Handle as a string literal otherwise
                let mut str = text.as_str().replace('"', "\\\"");
                if &str == "null" {
                    // Special case for "null"
                    Jsonb::from_str(&str)
                } else {
                    // Quote the string to make it a JSON string
                    str.insert(0, '"');
                    str.push('"');
                    Jsonb::from_str(&str)
                }
            };
            res
        }
        OwnedValue::Blob(blob) => {
            let json = Jsonb::from_raw_data(blob);
            json.is_valid()?;
            Ok(json)
        }
        OwnedValue::Record(_) | OwnedValue::Agg(_) => {
            bail_constraint_error!("Wrong number of arguments");
        }
        OwnedValue::Null => Ok(Jsonb::from_raw_data(
            JsonbHeader::make_null().into_bytes().as_bytes(),
        )),
        OwnedValue::Float(float) => {
            let mut buff = ryu::Buffer::new();
            Jsonb::from_str(buff.format(*float))
        }
        OwnedValue::Integer(int) => Jsonb::from_str(&int.to_string()),
    }
}

fn get_json_value(json_value: &OwnedValue) -> crate::Result<Val> {
    match json_value {
        OwnedValue::Text(ref t) => match from_str::<Val>(t.as_str()) {
            Ok(json) => Ok(json),
            Err(_) => {
                crate::bail_parse_error!("malformed JSON")
            }
        },
        OwnedValue::Blob(_) => {
            crate::bail_parse_error!("malformed JSON");
        }
        OwnedValue::Null => Ok(Val::Null),
        OwnedValue::Float(f) => Ok(Val::Float(*f)),
        OwnedValue::Integer(i) => Ok(Val::Integer(*i)),
        _ => Ok(Val::String(json_value.to_string())),
    }
}

pub fn json_array(values: &[OwnedValue]) -> crate::Result<OwnedValue> {
    let mut s = String::new();
    s.push('[');

    // TODO: use `convert_db_type_to_json` and map each value with that function,
    // so we can construct a `Val::Array` with each value and then serialize it directly.
    for (idx, value) in values.iter().enumerate() {
        match value {
            OwnedValue::Blob(_) => crate::bail_constraint_error!("JSON cannot hold BLOB values"),
            OwnedValue::Text(t) => {
                if t.subtype == TextSubtype::Json {
                    s.push_str(t.as_str());
                } else {
                    match to_string(&t.as_str().to_string()) {
                        Ok(json) => s.push_str(&json),
                        Err(_) => crate::bail_parse_error!("malformed JSON"),
                    }
                }
            }
            OwnedValue::Integer(i) => match to_string(&i) {
                Ok(json) => s.push_str(&json),
                Err(_) => crate::bail_parse_error!("malformed JSON"),
            },
            OwnedValue::Float(f) => match to_string(&f) {
                Ok(json) => s.push_str(&json),
                Err(_) => crate::bail_parse_error!("malformed JSON"),
            },
            OwnedValue::Null => s.push_str("null"),
            _ => unreachable!(),
        }

        if idx < values.len() - 1 {
            s.push(',');
        }
    }

    s.push(']');
    Ok(OwnedValue::Text(Text::json(s)))
}

pub fn json_array_length(
    json_value: &OwnedValue,
    json_path: Option<&OwnedValue>,
) -> crate::Result<OwnedValue> {
    let json = convert_dbtype_to_jsonb(json_value, true)?;

    if json_path.is_none() {
        let result = json.array_len()?;
        return Ok(OwnedValue::Integer(result as i64));
    }

    let path = json_path_from_owned_value(json_path.expect("We already checked none"), true)?;

    if let Some(path) = path {
        if let Ok(len) = json
            .get_by_path(&path)
            .and_then(|(json, _)| json.array_len())
        {
            return Ok(OwnedValue::Integer(len as i64));
        }
    }
    Ok(OwnedValue::Null)
}

pub fn json_set(json: &OwnedValue, values: &[OwnedValue]) -> crate::Result<OwnedValue> {
    let mut json_value = get_json_value(json)?;

    values
        .chunks(2)
        .map(|chunk| match chunk {
            [path, value] => {
                let path = json_path_from_owned_value(path, true)?;

                if let Some(path) = path {
                    let new_value = match value {
                        OwnedValue::Text(
                            t @ Text {
                                subtype: TextSubtype::Text,
                                ..
                            },
                        ) => Val::String(t.as_str().to_string()),
                        _ => get_json_value(value)?,
                    };

                    let mut new_json_value = json_value.clone();

                    match create_and_mutate_json_by_path(&mut new_json_value, path, |val| match val
                    {
                        Target::Array(arr, index) => arr[index] = new_value.clone(),
                        Target::Value(val) => *val = new_value.clone(),
                    }) {
                        Some(_) => json_value = new_json_value,
                        _ => {}
                    }
                }

                Ok(())
            }
            _ => crate::bail_constraint_error!("json_set needs an odd number of arguments"),
        })
        .collect::<crate::Result<()>>()?;

    convert_json_to_db_type(&json_value, true)
}

/// Implements the -> operator. Always returns a proper JSON value.
/// https://sqlite.org/json1.html#the_and_operators
pub fn json_arrow_extract(value: &OwnedValue, path: &OwnedValue) -> crate::Result<OwnedValue> {
    if let OwnedValue::Null = value {
        return Ok(OwnedValue::Null);
    }

    if let Some(path) = json_path_from_owned_value(path, false)? {
        let json = convert_dbtype_to_jsonb(value, true)?;
        let extracted = json.get_by_path(&path);
        if let Ok((json, _)) = extracted {
            Ok(OwnedValue::Text(Text::json(json.to_string()?)))
        } else {
            Ok(OwnedValue::Null)
        }
    } else {
        Ok(OwnedValue::Null)
    }
}

/// Implements the ->> operator. Always returns a SQL representation of the JSON subcomponent.
/// https://sqlite.org/json1.html#the_and_operators
pub fn json_arrow_shift_extract(
    value: &OwnedValue,
    path: &OwnedValue,
) -> crate::Result<OwnedValue> {
    if let OwnedValue::Null = value {
        return Ok(OwnedValue::Null);
    }
    if let Some(path) = json_path_from_owned_value(path, false)? {
        let json = convert_dbtype_to_jsonb(value, true)?;
        let extracted = json.get_by_path(&path);
        if let Ok((json, element_type)) = extracted {
            Ok(json_string_to_db_type(json, element_type, false, true)?)
        } else {
            Ok(OwnedValue::Null)
        }
    } else {
        Ok(OwnedValue::Null)
    }
}

/// Extracts a JSON value from a JSON object or array.
/// If there's only a single path, the return value might be either a TEXT or a database type.
/// https://sqlite.org/json1.html#the_json_extract_function
pub fn json_extract(value: &OwnedValue, paths: &[OwnedValue]) -> crate::Result<OwnedValue> {
    if let OwnedValue::Null = value {
        return Ok(OwnedValue::Null);
    }

    if paths.is_empty() {
        return Ok(OwnedValue::Null);
    }
    let (json, element_type) = jsonb_extract_internal(value, paths)?;

    let result = json_string_to_db_type(json, element_type, false, true)?;

    Ok(result)
}

pub fn jsonb_extract(value: &OwnedValue, paths: &[OwnedValue]) -> crate::Result<OwnedValue> {
    if let OwnedValue::Null = value {
        return Ok(OwnedValue::Null);
    }

    if paths.is_empty() {
        return Ok(OwnedValue::Null);
    }

    let (json, element_type) = jsonb_extract_internal(value, paths)?;
    let result = json_string_to_db_type(json, element_type, false, true)?;

    Ok(result)
}

fn jsonb_extract_internal(
    value: &OwnedValue,
    paths: &[OwnedValue],
) -> crate::Result<(Jsonb, ElementType)> {
    let null = Jsonb::from_raw_data(JsonbHeader::make_null().into_bytes().as_bytes());
    if paths.len() == 1 {
        if let Some(path) = json_path_from_owned_value(&paths[0], true)? {
            let json = convert_dbtype_to_jsonb(value, true)?;
            if let Ok((json, value_type)) = json.get_by_path(&path) {
                return Ok((json, value_type));
            } else {
                return Ok((null, ElementType::NULL));
            }
        } else {
            return Ok((null, ElementType::NULL));
        }
    }

    let json = convert_dbtype_to_jsonb(value, true)?;
    let mut result = Jsonb::make_empty_array(json.len());

    let paths = paths.iter().map(|p| json_path_from_owned_value(p, true));
    for path in paths {
        if let Some(path) = path? {
            let fragment = json.get_by_path_raw(&path);
            if let Ok(data) = fragment {
                result.append_to_array_unsafe(data);
            } else {
                result.append_to_array_unsafe(JsonbHeader::make_null().into_bytes().as_bytes());
            }
        } else {
            return Ok((null, ElementType::NULL));
        }
    }
    result.finalize_array_unsafe()?;
    Ok((result, ElementType::ARRAY))
}

fn json_string_to_db_type(
    json: Jsonb,
    element_type: ElementType,
    raw_flag: bool,
    raw_text: bool,
) -> crate::Result<OwnedValue> {
    let mut json_string = json.to_string()?;
    if raw_flag {
        return Ok(OwnedValue::Blob(Rc::new(json.data())));
    }
    match element_type {
        ElementType::ARRAY | ElementType::OBJECT => Ok(OwnedValue::Text(Text::json(json_string))),
        ElementType::TEXT | ElementType::TEXT5 | ElementType::TEXTJ | ElementType::TEXTRAW => {
            if raw_text {
                json_string.remove(json_string.len() - 1);
                json_string.remove(0);
                Ok(OwnedValue::Text(Text {
                    value: Rc::new(json_string.into_bytes()),
                    subtype: TextSubtype::Json,
                }))
            } else {
                Ok(OwnedValue::Text(Text {
                    value: Rc::new(json_string.into_bytes()),
                    subtype: TextSubtype::Text,
                }))
            }
        }
        ElementType::FLOAT5 | ElementType::FLOAT => Ok(OwnedValue::Float(
            json_string.parse().expect("Should be valid f64"),
        )),
        ElementType::INT | ElementType::INT5 => {
            let result = i64::from_str(&json_string);
            if let Ok(int) = result {
                Ok(OwnedValue::Integer(int))
            } else {
                let res = f64::from_str(&json_string);
                match res {
                    Ok(num) => Ok(OwnedValue::Float(num)),
                    Err(_) => Ok(OwnedValue::Null),
                }
            }
        }
        ElementType::TRUE => Ok(OwnedValue::Integer(1)),
        ElementType::FALSE => Ok(OwnedValue::Integer(0)),
        ElementType::NULL => Ok(OwnedValue::Null),
        _ => unreachable!(),
    }
}

/// Returns a value with type defined by SQLite documentation:
///   > the SQL datatype of the result is NULL for a JSON null,
///   > INTEGER or REAL for a JSON numeric value,
///   > an INTEGER zero for a JSON false value,
///   > an INTEGER one for a JSON true value,
///   > the dequoted text for a JSON string value,
///   > and a text representation for JSON object and array values.
///
/// https://sqlite.org/json1.html#the_json_extract_function
///
/// *all_as_db* - if true, objects and arrays will be returned as pure TEXT without the JSON subtype
fn convert_json_to_db_type(extracted: &Val, all_as_db: bool) -> crate::Result<OwnedValue> {
    match extracted {
        Val::Removed => Ok(OwnedValue::Null),
        Val::Null => Ok(OwnedValue::Null),
        Val::Float(f) => Ok(OwnedValue::Float(*f)),
        Val::Integer(i) => Ok(OwnedValue::Integer(*i)),
        Val::Bool(b) => {
            if *b {
                Ok(OwnedValue::Integer(1))
            } else {
                Ok(OwnedValue::Integer(0))
            }
        }
        Val::String(s) => Ok(OwnedValue::Text(Text::from_str(s))),
        _ => {
            let json = to_string(&extracted)?;
            if all_as_db {
                Ok(OwnedValue::build_text(&json))
            } else {
                Ok(OwnedValue::Text(Text::json(json)))
            }
        }
    }
}

/// Converts a DB value (`OwnedValue`) to a JSON representation (`Val`).
/// Note that when the internal text value is a json,
/// the returned `Val` will be an object. If the internal text value is a regular text,
/// then a string will be returned. This is useful to track if the value came from a json
/// function and therefore we must interpret it as json instead of raw text when working with it.
fn convert_db_type_to_json(value: &OwnedValue) -> crate::Result<Val> {
    let val = match value {
        OwnedValue::Null => Val::Null,
        OwnedValue::Float(f) => Val::Float(*f),
        OwnedValue::Integer(i) => Val::Integer(*i),
        OwnedValue::Text(t) => match t.subtype {
            // Convert only to json if the subtype is json (if we got it from another json function)
            TextSubtype::Json => get_json_value(value)?,
            TextSubtype::Text => Val::String(t.as_str().to_string()),
        },
        OwnedValue::Blob(_) => crate::bail_constraint_error!("JSON cannot hold BLOB values"),
        unsupported_value => crate::bail_constraint_error!(
            "JSON cannot hold this type of value: {unsupported_value:?}"
        ),
    };
    Ok(val)
}

pub fn json_type(value: &OwnedValue, path: Option<&OwnedValue>) -> crate::Result<OwnedValue> {
    if let OwnedValue::Null = value {
        return Ok(OwnedValue::Null);
    }
    if path.is_none() {
        let json = convert_dbtype_to_jsonb(value, true)?;
        let element_type = json.is_valid()?;

        return Ok(OwnedValue::Text(Text::json(element_type.into())));
    }
    if let Some(path) = json_path_from_owned_value(path.unwrap(), true)? {
        let json = convert_dbtype_to_jsonb(value, true)?;

        if let Ok((_, element_type)) = json.get_by_path(&path) {
            Ok(OwnedValue::Text(Text::json(element_type.into())))
        } else {
            Ok(OwnedValue::Null)
        }
    } else {
        Ok(OwnedValue::Null)
    }
}

fn json_path_from_owned_value(path: &OwnedValue, strict: bool) -> crate::Result<Option<JsonPath>> {
    let json_path = if strict {
        match path {
            OwnedValue::Text(t) => json_path(t.as_str())?,
            OwnedValue::Null => return Ok(None),
            _ => crate::bail_constraint_error!("JSON path error near: {:?}", path.to_string()),
        }
    } else {
        match path {
            OwnedValue::Text(t) => {
                if t.as_str().starts_with("$") {
                    json_path(t.as_str())?
                } else {
                    JsonPath {
                        elements: vec![
                            PathElement::Root(),
                            PathElement::Key(Cow::Borrowed(t.as_str()), false),
                        ],
                    }
                }
            }
            OwnedValue::Null => return Ok(None),
            OwnedValue::Integer(i) => JsonPath {
                elements: vec![
                    PathElement::Root(),
                    PathElement::ArrayLocator(Some(*i as i32)),
                ],
            },
            OwnedValue::Float(f) => JsonPath {
                elements: vec![
                    PathElement::Root(),
                    PathElement::Key(Cow::Owned(f.to_string()), false),
                ],
            },
            _ => crate::bail_constraint_error!("JSON path error near: {:?}", path.to_string()),
        }
    };

    Ok(Some(json_path))
}

enum Target<'a> {
    Array(&'a mut Vec<Val>, usize),
    Value(&'a mut Val),
}

fn create_and_mutate_json_by_path<F, R>(json: &mut Val, path: JsonPath, closure: F) -> Option<R>
where
    F: FnOnce(Target) -> R,
{
    find_or_create_target(json, &path).map(closure)
}

fn find_or_create_target<'a>(json: &'a mut Val, path: &JsonPath) -> Option<Target<'a>> {
    let mut current = json;
    for (i, key) in path.elements.iter().enumerate() {
        let is_last = i == path.elements.len() - 1;
        match key {
            PathElement::Root() => continue,
            PathElement::ArrayLocator(index) => match current {
                Val::Array(arr) => {
                    if let Some(index) = match index {
                        Some(i) if *i < 0 => arr.len().checked_sub(i.unsigned_abs() as usize),
                        Some(i) => Some(*i as usize),
                        None => Some(arr.len()),
                    } {
                        if is_last {
                            if index == arr.len() {
                                arr.push(Val::Null);
                            }

                            if index >= arr.len() {
                                return None;
                            }

                            return Some(Target::Array(arr, index));
                        } else {
                            if index == arr.len() {
                                arr.push(
                                    if matches!(path.elements[i + 1], PathElement::ArrayLocator(_))
                                    {
                                        Val::Array(vec![])
                                    } else {
                                        Val::Object(vec![])
                                    },
                                );
                            }

                            if index >= arr.len() {
                                return None;
                            }

                            current = &mut arr[index];
                        }
                    } else {
                        return None;
                    }
                }
                _ => {
                    *current = Val::Array(vec![]);
                }
            },
            PathElement::Key(key, _) => match current {
                Val::Object(obj) => {
                    if let Some(pos) = &obj
                        .iter()
                        .position(|(k, v)| k == key && !matches!(v, Val::Removed))
                    {
                        let val = &mut obj[*pos].1;
                        current = val;
                    } else {
                        let element = if !is_last
                            && matches!(path.elements[i + 1], PathElement::ArrayLocator(_))
                        {
                            Val::Array(vec![])
                        } else {
                            Val::Object(vec![])
                        };

                        obj.push((key.to_string(), element));
                        let index = obj.len() - 1;
                        current = &mut obj[index].1;
                    }
                }
                _ => {
                    return None;
                }
            },
        }
    }
    Some(Target::Value(current))
}

pub fn json_error_position(json: &OwnedValue) -> crate::Result<OwnedValue> {
    match json {
        OwnedValue::Text(t) => match from_str::<Val>(t.as_str()) {
            Ok(_) => Ok(OwnedValue::Integer(0)),
            Err(JsonError::Message { location, .. }) => {
                if let Some(loc) = location {
                    Ok(OwnedValue::Integer(loc.column as i64))
                } else {
                    Err(crate::error::LimboError::InternalError(
                        "failed to determine json error position".into(),
                    ))
                }
            }
        },
        OwnedValue::Blob(_) => {
            bail_parse_error!("Unsupported")
        }
        OwnedValue::Null => Ok(OwnedValue::Null),
        _ => Ok(OwnedValue::Integer(0)),
    }
}

/// Constructs a JSON object from a list of values that represent key-value pairs.
/// The number of values must be even, and the first value of each pair (which represents the map key)
/// must be a TEXT value. The second value of each pair can be any JSON value (which represents the map value)
pub fn json_object(values: &[OwnedValue]) -> crate::Result<OwnedValue> {
    let value_map = values
        .chunks(2)
        .map(|chunk| match chunk {
            [key, value] => {
                let key = match key {
                    OwnedValue::Text(t) => t.as_str().to_string(),
                    _ => crate::bail_constraint_error!("labels must be TEXT"),
                };
                let json_val = convert_db_type_to_json(value)?;

                Ok((key, json_val))
            }
            _ => crate::bail_constraint_error!("json_object requires an even number of values"),
        })
        .collect::<Result<IndexMap<String, Val>, _>>()?;

    let result = crate::json::to_string(&value_map)?;
    Ok(OwnedValue::Text(Text::json(result)))
}

pub fn is_json_valid(json_value: &OwnedValue) -> OwnedValue {
    if matches!(json_value, OwnedValue::Null) {
        return OwnedValue::Null;
    }
    convert_dbtype_to_jsonb(json_value, true)
        .map(|_| OwnedValue::Integer(1))
        .unwrap_or(OwnedValue::Integer(0))
}

pub fn json_quote(value: &OwnedValue) -> crate::Result<OwnedValue> {
    match value {
        OwnedValue::Text(ref t) => {
            // If X is a JSON value returned by another JSON function,
            // then this function is a no-op
            if t.subtype == TextSubtype::Json {
                // Should just return the json value with no quotes
                return Ok(value.to_owned());
            }

            let mut escaped_value = String::with_capacity(t.value.len() + 4);
            escaped_value.push('"');

            for c in t.as_str().chars() {
                match c {
                    '"' | '\\' | '\n' | '\r' | '\t' | '\u{0008}' | '\u{000c}' => {
                        escaped_value.push('\\');
                        escaped_value.push(c);
                    }
                    c => escaped_value.push(c),
                }
            }
            escaped_value.push('"');

            Ok(OwnedValue::build_text(&escaped_value))
        }
        // Numbers are unquoted in json
        OwnedValue::Integer(ref int) => Ok(OwnedValue::Integer(int.to_owned())),
        OwnedValue::Float(ref float) => Ok(OwnedValue::Float(float.to_owned())),
        OwnedValue::Blob(_) => crate::bail_constraint_error!("JSON cannot hold BLOB values"),
        OwnedValue::Null => Ok(OwnedValue::build_text("null")),
        _ => {
            unreachable!()
        }
    }
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use super::*;
    use crate::types::OwnedValue;

    #[test]
    fn test_get_json_valid_json5() {
        let input = OwnedValue::build_text("{ key: 'value' }");
        let result = get_json(&input, None).unwrap();
        if let OwnedValue::Text(result_str) = result {
            assert!(result_str.as_str().contains("\"key\":\"value\""));
            assert_eq!(result_str.subtype, TextSubtype::Json);
        } else {
            panic!("Expected OwnedValue::Text");
        }
    }

    #[test]
    fn test_get_json_valid_json5_double_single_quotes() {
        let input = OwnedValue::build_text("{ key: ''value'' }");
        let result = get_json(&input, None).unwrap();
        if let OwnedValue::Text(result_str) = result {
            assert!(result_str.as_str().contains("\"key\":\"value\""));
            assert_eq!(result_str.subtype, TextSubtype::Json);
        } else {
            panic!("Expected OwnedValue::Text");
        }
    }

    #[test]
    fn test_get_json_valid_json5_infinity() {
        let input = OwnedValue::build_text("{ \"key\": Infinity }");
        let result = get_json(&input, None).unwrap();
        if let OwnedValue::Text(result_str) = result {
            assert!(result_str.as_str().contains("{\"key\":9e999}"));
            assert_eq!(result_str.subtype, TextSubtype::Json);
        } else {
            panic!("Expected OwnedValue::Text");
        }
    }

    #[test]
    fn test_get_json_valid_json5_negative_infinity() {
        let input = OwnedValue::build_text("{ \"key\": -Infinity }");
        let result = get_json(&input, None).unwrap();
        if let OwnedValue::Text(result_str) = result {
            assert!(result_str.as_str().contains("{\"key\":-9e999}"));
            assert_eq!(result_str.subtype, TextSubtype::Json);
        } else {
            panic!("Expected OwnedValue::Text");
        }
    }

    #[test]
    fn test_get_json_valid_json5_nan() {
        let input = OwnedValue::build_text("{ \"key\": NaN }");
        let result = get_json(&input, None).unwrap();
        if let OwnedValue::Text(result_str) = result {
            assert!(result_str.as_str().contains("{\"key\":null}"));
            assert_eq!(result_str.subtype, TextSubtype::Json);
        } else {
            panic!("Expected OwnedValue::Text");
        }
    }

    #[test]
    fn test_get_json_invalid_json5() {
        let input = OwnedValue::build_text("{ key: value }");
        let result = get_json(&input, None);
        match result {
            Ok(_) => panic!("Expected error for malformed JSON"),
            Err(e) => assert!(e.to_string().contains("malformed JSON")),
        }
    }

    #[test]
    fn test_get_json_valid_jsonb() {
        let input = OwnedValue::build_text("{\"key\":\"value\"}");
        let result = get_json(&input, None).unwrap();
        if let OwnedValue::Text(result_str) = result {
            assert!(result_str.as_str().contains("\"key\":\"value\""));
            assert_eq!(result_str.subtype, TextSubtype::Json);
        } else {
            panic!("Expected OwnedValue::Text");
        }
    }

    #[test]
    fn test_get_json_invalid_jsonb() {
        let input = OwnedValue::build_text("{key:\"value\"");
        let result = get_json(&input, None);
        match result {
            Ok(_) => panic!("Expected error for malformed JSON"),
            Err(e) => assert!(e.to_string().contains("malformed JSON")),
        }
    }

    #[test]
    fn test_get_json_blob_valid_jsonb() {
        let binary_json = vec![124, 55, 104, 101, 121, 39, 121, 111];
        let input = OwnedValue::Blob(Rc::new(binary_json));
        let result = get_json(&input, None).unwrap();
        if let OwnedValue::Text(result_str) = result {
            assert!(result_str.as_str().contains(r#"{"hey":"yo"}"#));
            assert_eq!(result_str.subtype, TextSubtype::Json);
        } else {
            panic!("Expected OwnedValue::Text");
        }
    }

    #[test]
    fn test_get_json_blob_invalid_jsonb() {
        let binary_json: Vec<u8> = vec![0xA2, 0x62, 0x6B, 0x31, 0x62, 0x76]; // Incomplete binary JSON
        let input = OwnedValue::Blob(Rc::new(binary_json));
        let result = get_json(&input, None);
        println!("{:?}", result);
        match result {
            Ok(_) => panic!("Expected error for malformed JSON"),
            Err(e) => assert!(e.to_string().contains("malformed JSON")),
        }
    }

    #[test]
    fn test_get_json_non_text() {
        let input = OwnedValue::Null;
        let result = get_json(&input, None).unwrap();
        if let OwnedValue::Null = result {
            // Test passed
        } else {
            panic!("Expected OwnedValue::Null");
        }
    }

    #[test]
    fn test_json_array_simple() {
        let text = OwnedValue::build_text("value1");
        let json = OwnedValue::Text(Text::json("\"value2\"".to_string()));
        let input = vec![text, json, OwnedValue::Integer(1), OwnedValue::Float(1.1)];

        let result = json_array(&input).unwrap();
        if let OwnedValue::Text(res) = result {
            assert_eq!(res.as_str(), "[\"value1\",\"value2\",1,1.1]");
            assert_eq!(res.subtype, TextSubtype::Json);
        } else {
            panic!("Expected OwnedValue::Text");
        }
    }

    #[test]
    fn test_json_array_empty() {
        let input = vec![];

        let result = json_array(&input).unwrap();
        if let OwnedValue::Text(res) = result {
            assert_eq!(res.as_str(), "[]");
            assert_eq!(res.subtype, TextSubtype::Json);
        } else {
            panic!("Expected OwnedValue::Text");
        }
    }

    #[test]
    fn test_json_array_blob_invalid() {
        let blob = OwnedValue::Blob(Rc::new("1".as_bytes().to_vec()));

        let input = vec![blob];

        let result = json_array(&input);

        match result {
            Ok(_) => panic!("Expected error for blob input"),
            Err(e) => assert!(e.to_string().contains("JSON cannot hold BLOB values")),
        }
    }

    #[test]
    fn test_json_array_length() {
        let input = OwnedValue::build_text("[1,2,3,4]");
        let result = json_array_length(&input, None).unwrap();
        if let OwnedValue::Integer(res) = result {
            assert_eq!(res, 4);
        } else {
            panic!("Expected OwnedValue::Integer");
        }
    }

    #[test]
    fn test_json_array_length_empty() {
        let input = OwnedValue::build_text("[]");
        let result = json_array_length(&input, None).unwrap();
        if let OwnedValue::Integer(res) = result {
            assert_eq!(res, 0);
        } else {
            panic!("Expected OwnedValue::Integer");
        }
    }

    #[test]
    fn test_json_array_length_root() {
        let input = OwnedValue::build_text("[1,2,3,4]");
        let result = json_array_length(&input, Some(&OwnedValue::build_text("$"))).unwrap();
        if let OwnedValue::Integer(res) = result {
            assert_eq!(res, 4);
        } else {
            panic!("Expected OwnedValue::Integer");
        }
    }

    #[test]
    fn test_json_array_length_not_array() {
        let input = OwnedValue::build_text("{one: [1,2,3,4]}");
        let result = json_array_length(&input, None).unwrap();
        if let OwnedValue::Integer(res) = result {
            assert_eq!(res, 0);
        } else {
            panic!("Expected OwnedValue::Integer");
        }
    }

    #[test]
    fn test_json_array_length_via_prop() {
        let input = OwnedValue::build_text("{one: [1,2,3,4]}");
        let result = json_array_length(&input, Some(&OwnedValue::build_text("$.one"))).unwrap();
        if let OwnedValue::Integer(res) = result {
            assert_eq!(res, 4);
        } else {
            panic!("Expected OwnedValue::Integer");
        }
    }

    #[test]
    fn test_json_array_length_via_index() {
        let input = OwnedValue::build_text("[[1,2,3,4]]");
        let result = json_array_length(&input, Some(&OwnedValue::build_text("$[0]"))).unwrap();
        if let OwnedValue::Integer(res) = result {
            assert_eq!(res, 4);
        } else {
            panic!("Expected OwnedValue::Integer");
        }
    }

    #[test]
    fn test_json_array_length_via_index_not_array() {
        let input = OwnedValue::build_text("[1,2,3,4]");
        let result = json_array_length(&input, Some(&OwnedValue::build_text("$[2]"))).unwrap();
        if let OwnedValue::Integer(res) = result {
            assert_eq!(res, 0);
        } else {
            panic!("Expected OwnedValue::Integer");
        }
    }

    #[test]
    fn test_json_array_length_via_index_bad_prop() {
        let input = OwnedValue::build_text("{one: [1,2,3,4]}");
        let result = json_array_length(&input, Some(&OwnedValue::build_text("$.two"))).unwrap();
        assert_eq!(OwnedValue::Null, result);
    }

    #[test]
    fn test_json_array_length_simple_json_subtype() {
        let input = OwnedValue::build_text("[1,2,3]");
        let wrapped = get_json(&input, None).unwrap();
        let result = json_array_length(&wrapped, None).unwrap();

        if let OwnedValue::Integer(res) = result {
            assert_eq!(res, 3);
        } else {
            panic!("Expected OwnedValue::Integer");
        }
    }

    #[test]
    fn test_json_extract_missing_path() {
        let result = json_extract(
            &OwnedValue::build_text("{\"a\":2}"),
            &[OwnedValue::build_text("$.x")],
        );

        match result {
            Ok(OwnedValue::Null) => (),
            _ => panic!("Expected null result, got: {:?}", result),
        }
    }
    #[test]
    fn test_json_extract_null_path() {
        let result = json_extract(&OwnedValue::build_text("{\"a\":2}"), &[OwnedValue::Null]);

        match result {
            Ok(OwnedValue::Null) => (),
            _ => panic!("Expected null result, got: {:?}", result),
        }
    }

    #[test]
    fn test_json_path_invalid() {
        let result = json_extract(
            &OwnedValue::build_text("{\"a\":2}"),
            &[OwnedValue::Float(1.1)],
        );

        match result {
            Ok(_) => panic!("expected error"),
            Err(e) => assert!(e.to_string().contains("JSON path error")),
        }
    }

    #[test]
    fn test_json_error_position_no_error() {
        let input = OwnedValue::build_text("[1,2,3]");
        let result = json_error_position(&input).unwrap();
        assert_eq!(result, OwnedValue::Integer(0));
    }

    #[test]
    fn test_json_error_position_no_error_more() {
        let input = OwnedValue::build_text(r#"{"a":55,"b":72 , }"#);
        let result = json_error_position(&input).unwrap();
        assert_eq!(result, OwnedValue::Integer(0));
    }

    #[test]
    fn test_json_error_position_object() {
        let input = OwnedValue::build_text(r#"{"a":55,"b":72,,}"#);
        let result = json_error_position(&input).unwrap();
        assert_eq!(result, OwnedValue::Integer(16));
    }

    #[test]
    fn test_json_error_position_array() {
        let input = OwnedValue::build_text(r#"["a",55,"b",72,,]"#);
        let result = json_error_position(&input).unwrap();
        assert_eq!(result, OwnedValue::Integer(16));
    }

    #[test]
    fn test_json_error_position_null() {
        let input = OwnedValue::Null;
        let result = json_error_position(&input).unwrap();
        assert_eq!(result, OwnedValue::Null);
    }

    #[test]
    fn test_json_error_position_integer() {
        let input = OwnedValue::Integer(5);
        let result = json_error_position(&input).unwrap();
        assert_eq!(result, OwnedValue::Integer(0));
    }

    #[test]
    fn test_json_error_position_float() {
        let input = OwnedValue::Float(-5.5);
        let result = json_error_position(&input).unwrap();
        assert_eq!(result, OwnedValue::Integer(0));
    }

    #[test]
    fn test_json_object_simple() {
        let key = OwnedValue::build_text("key");
        let value = OwnedValue::build_text("value");
        let input = vec![key, value];

        let result = json_object(&input).unwrap();
        let OwnedValue::Text(json_text) = result else {
            panic!("Expected OwnedValue::Text");
        };
        assert_eq!(json_text.as_str(), r#"{"key":"value"}"#);
    }

    #[test]
    fn test_json_object_multiple_values() {
        let text_key = OwnedValue::build_text("text_key");
        let text_value = OwnedValue::build_text("text_value");
        let json_key = OwnedValue::build_text("json_key");
        let json_value = OwnedValue::Text(Text::json(r#"{"json":"value","number":1}"#.to_string()));
        let integer_key = OwnedValue::build_text("integer_key");
        let integer_value = OwnedValue::Integer(1);
        let float_key = OwnedValue::build_text("float_key");
        let float_value = OwnedValue::Float(1.1);
        let null_key = OwnedValue::build_text("null_key");
        let null_value = OwnedValue::Null;

        let input = vec![
            text_key,
            text_value,
            json_key,
            json_value,
            integer_key,
            integer_value,
            float_key,
            float_value,
            null_key,
            null_value,
        ];

        let result = json_object(&input).unwrap();
        let OwnedValue::Text(json_text) = result else {
            panic!("Expected OwnedValue::Text");
        };
        assert_eq!(
            json_text.as_str(),
            r#"{"text_key":"text_value","json_key":{"json":"value","number":1},"integer_key":1,"float_key":1.1,"null_key":null}"#
        );
    }

    #[test]
    fn test_json_object_json_value_is_rendered_as_json() {
        let key = OwnedValue::build_text("key");
        let value = OwnedValue::Text(Text::json(r#"{"json":"value"}"#.to_string()));
        let input = vec![key, value];

        let result = json_object(&input).unwrap();
        let OwnedValue::Text(json_text) = result else {
            panic!("Expected OwnedValue::Text");
        };
        assert_eq!(json_text.as_str(), r#"{"key":{"json":"value"}}"#);
    }

    #[test]
    fn test_json_object_json_text_value_is_rendered_as_regular_text() {
        let key = OwnedValue::build_text("key");
        let value = OwnedValue::Text(Text::new(r#"{"json":"value"}"#));
        let input = vec![key, value];

        let result = json_object(&input).unwrap();
        let OwnedValue::Text(json_text) = result else {
            panic!("Expected OwnedValue::Text");
        };
        assert_eq!(json_text.as_str(), r#"{"key":"{\"json\":\"value\"}"}"#);
    }

    #[test]
    fn test_json_object_nested() {
        let key = OwnedValue::build_text("key");
        let value = OwnedValue::build_text("value");
        let input = vec![key, value];

        let parent_key = OwnedValue::build_text("parent_key");
        let parent_value = json_object(&input).unwrap();
        let parent_input = vec![parent_key, parent_value];

        let result = json_object(&parent_input).unwrap();

        let OwnedValue::Text(json_text) = result else {
            panic!("Expected OwnedValue::Text");
        };
        assert_eq!(json_text.as_str(), r#"{"parent_key":{"key":"value"}}"#);
    }

    #[test]
    fn test_json_object_duplicated_keys() {
        let key = OwnedValue::build_text("key");
        let value = OwnedValue::build_text("value");
        let input = vec![key.clone(), value.clone(), key, value];

        let result = json_object(&input).unwrap();
        let OwnedValue::Text(json_text) = result else {
            panic!("Expected OwnedValue::Text");
        };
        assert_eq!(json_text.as_str(), r#"{"key":"value"}"#);
    }

    #[test]
    fn test_json_object_empty() {
        let input = vec![];

        let result = json_object(&input).unwrap();
        let OwnedValue::Text(json_text) = result else {
            panic!("Expected OwnedValue::Text");
        };
        assert_eq!(json_text.as_str(), r#"{}"#);
    }

    #[test]
    fn test_json_object_non_text_key() {
        let key = OwnedValue::Integer(1);
        let value = OwnedValue::build_text("value");
        let input = vec![key, value];

        match json_object(&input) {
            Ok(_) => panic!("Expected error for non-TEXT key"),
            Err(e) => assert!(e.to_string().contains("labels must be TEXT")),
        }
    }

    #[test]
    fn test_json_odd_number_of_values() {
        let key = OwnedValue::build_text("key");
        let value = OwnedValue::build_text("value");
        let input = vec![key.clone(), value, key];

        match json_object(&input) {
            Ok(_) => panic!("Expected error for odd number of values"),
            Err(e) => assert!(e
                .to_string()
                .contains("json_object requires an even number of values")),
        }
    }

    #[test]
    fn test_json_path_from_owned_value_root_strict() {
        let path = OwnedValue::Text(Text::new("$"));

        let result = json_path_from_owned_value(&path, true);
        assert!(result.is_ok());

        let result = result.unwrap();
        assert!(result.is_some());

        let result = result.unwrap();
        match result.elements[..] {
            [PathElement::Root()] => {}
            _ => panic!("Expected root"),
        }
    }

    #[test]
    fn test_json_path_from_owned_value_root_non_strict() {
        let path = OwnedValue::Text(Text::new("$"));

        let result = json_path_from_owned_value(&path, false);
        assert!(result.is_ok());

        let result = result.unwrap();
        assert!(result.is_some());

        let result = result.unwrap();
        match result.elements[..] {
            [PathElement::Root()] => {}
            _ => panic!("Expected root"),
        }
    }

    #[test]
    fn test_json_path_from_owned_value_named_strict() {
        let path = OwnedValue::Text(Text::new("field"));

        assert!(json_path_from_owned_value(&path, true).is_err());
    }

    #[test]
    fn test_json_path_from_owned_value_named_non_strict() {
        let path = OwnedValue::Text(Text::new("field"));

        let result = json_path_from_owned_value(&path, false);
        assert!(result.is_ok());

        let result = result.unwrap();
        assert!(result.is_some());

        let result = result.unwrap();
        match &result.elements[..] {
            [PathElement::Root(), PathElement::Key(field, false)] if *field == "field" => {}
            _ => panic!("Expected root and field"),
        }
    }

    #[test]
    fn test_json_path_from_owned_value_integer_strict() {
        let path = OwnedValue::Integer(3);
        assert!(json_path_from_owned_value(&path, true).is_err());
    }

    #[test]
    fn test_json_path_from_owned_value_integer_non_strict() {
        let path = OwnedValue::Integer(3);

        let result = json_path_from_owned_value(&path, false);
        assert!(result.is_ok());

        let result = result.unwrap();
        assert!(result.is_some());

        let result = result.unwrap();
        match &result.elements[..] {
            [PathElement::Root(), PathElement::ArrayLocator(index)] if *index == Some(3) => {}
            _ => panic!("Expected root and array locator"),
        }
    }

    #[test]
    fn test_json_path_from_owned_value_null_strict() {
        let path = OwnedValue::Null;

        let result = json_path_from_owned_value(&path, true);
        assert!(result.is_ok());

        let result = result.unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_json_path_from_owned_value_null_non_strict() {
        let path = OwnedValue::Null;

        let result = json_path_from_owned_value(&path, false);
        assert!(result.is_ok());

        let result = result.unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_json_path_from_owned_value_float_strict() {
        let path = OwnedValue::Float(1.23);

        assert!(json_path_from_owned_value(&path, true).is_err());
    }

    #[test]
    fn test_json_path_from_owned_value_float_non_strict() {
        let path = OwnedValue::Float(1.23);

        let result = json_path_from_owned_value(&path, false);
        assert!(result.is_ok());

        let result = result.unwrap();
        assert!(result.is_some());

        let result = result.unwrap();
        match &result.elements[..] {
            [PathElement::Root(), PathElement::Key(field, false)] if *field == "1.23" => {}
            _ => panic!("Expected root and field"),
        }
    }

    #[test]
    fn test_json_set_field_empty_object() {
        let result = json_set(
            &OwnedValue::build_text("{}"),
            &[
                OwnedValue::build_text("$.field"),
                OwnedValue::build_text("value"),
            ],
        );

        assert!(result.is_ok());

        assert_eq!(
            result.unwrap(),
            OwnedValue::build_text(r#"{"field":"value"}"#)
        );
    }

    #[test]
    fn test_json_set_replace_field() {
        let result = json_set(
            &OwnedValue::build_text(r#"{"field":"old_value"}"#),
            &[
                OwnedValue::build_text("$.field"),
                OwnedValue::build_text("new_value"),
            ],
        );

        assert!(result.is_ok());

        assert_eq!(
            result.unwrap(),
            OwnedValue::build_text(r#"{"field":"new_value"}"#)
        );
    }

    #[test]
    fn test_json_set_set_deeply_nested_key() {
        let result = json_set(
            &OwnedValue::build_text("{}"),
            &[
                OwnedValue::build_text("$.object.doesnt.exist"),
                OwnedValue::build_text("value"),
            ],
        );

        assert!(result.is_ok());

        assert_eq!(
            result.unwrap(),
            OwnedValue::build_text(r#"{"object":{"doesnt":{"exist":"value"}}}"#)
        );
    }

    #[test]
    fn test_json_set_add_value_to_empty_array() {
        let result = json_set(
            &OwnedValue::build_text("[]"),
            &[
                OwnedValue::build_text("$[0]"),
                OwnedValue::build_text("value"),
            ],
        );

        assert!(result.is_ok());

        assert_eq!(result.unwrap(), OwnedValue::build_text(r#"["value"]"#));
    }

    #[test]
    fn test_json_set_add_value_to_nonexistent_array() {
        let result = json_set(
            &OwnedValue::build_text("{}"),
            &[
                OwnedValue::build_text("$.some_array[0]"),
                OwnedValue::Integer(123),
            ],
        );

        assert!(result.is_ok());

        assert_eq!(
            result.unwrap(),
            OwnedValue::build_text(r#"{"some_array":[123]}"#)
        );
    }

    #[test]
    fn test_json_set_add_value_to_array() {
        let result = json_set(
            &OwnedValue::build_text("[123]"),
            &[OwnedValue::build_text("$[1]"), OwnedValue::Integer(456)],
        );

        assert!(result.is_ok());

        assert_eq!(result.unwrap(), OwnedValue::build_text("[123,456]"));
    }

    #[test]
    fn test_json_set_add_value_to_array_out_of_bounds() {
        let result = json_set(
            &OwnedValue::build_text("[123]"),
            &[OwnedValue::build_text("$[200]"), OwnedValue::Integer(456)],
        );

        assert!(result.is_ok());

        assert_eq!(result.unwrap(), OwnedValue::build_text("[123]"));
    }

    #[test]
    fn test_json_set_replace_value_in_array() {
        let result = json_set(
            &OwnedValue::build_text("[123]"),
            &[OwnedValue::build_text("$[0]"), OwnedValue::Integer(456)],
        );

        assert!(result.is_ok());

        assert_eq!(result.unwrap(), OwnedValue::build_text("[456]"));
    }

    #[test]
    fn test_json_set_null_path() {
        let result = json_set(
            &OwnedValue::build_text("{}"),
            &[OwnedValue::Null, OwnedValue::Integer(456)],
        );

        assert!(result.is_ok());

        assert_eq!(result.unwrap(), OwnedValue::build_text("{}"));
    }

    #[test]
    fn test_json_set_multiple_keys() {
        let result = json_set(
            &OwnedValue::build_text("[123]"),
            &[
                OwnedValue::build_text("$[0]"),
                OwnedValue::Integer(456),
                OwnedValue::build_text("$[1]"),
                OwnedValue::Integer(789),
            ],
        );

        assert!(result.is_ok());

        assert_eq!(result.unwrap(), OwnedValue::build_text("[456,789]"));
    }

    #[test]
    fn test_json_set_missing_value() {
        let result = json_set(
            &OwnedValue::build_text("[123]"),
            &[OwnedValue::build_text("$[0]")],
        );

        assert!(result.is_err());
    }

    #[test]
    fn test_json_set_add_array_in_nested_object() {
        let result = json_set(
            &OwnedValue::build_text("{}"),
            &[
                OwnedValue::build_text("$.object[0].field"),
                OwnedValue::Integer(123),
            ],
        );

        assert!(result.is_ok());

        assert_eq!(
            result.unwrap(),
            OwnedValue::build_text(r#"{"object":[{"field":123}]}"#)
        );
    }

    #[test]
    fn test_json_set_add_array_in_array_in_nested_object() {
        let result = json_set(
            &OwnedValue::build_text("{}"),
            &[
                OwnedValue::build_text("$.object[0][0]"),
                OwnedValue::Integer(123),
            ],
        );

        assert!(result.is_ok());

        assert_eq!(
            result.unwrap(),
            OwnedValue::build_text(r#"{"object":[[123]]}"#)
        );
    }

    #[test]
    fn test_json_set_add_array_in_array_in_nested_object_out_of_bounds() {
        let result = json_set(
            &OwnedValue::build_text("{}"),
            &[
                OwnedValue::build_text("$.object[123].another"),
                OwnedValue::build_text("value"),
                OwnedValue::build_text("$.field"),
                OwnedValue::build_text("value"),
            ],
        );

        assert!(result.is_ok());

        assert_eq!(
            result.unwrap(),
            OwnedValue::build_text(r#"{"field":"value"}"#)
        );
    }
}
