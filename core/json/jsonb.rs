use crate::{bail_parse_error, LimboError, Result};
use std::{fmt::Write, iter::Peekable, str::from_utf8};

const PAYLOAD_SIZE8: u8 = 12;
const PAYLOAD_SIZE16: u8 = 13;
const PAYLOAD_SIZE32: u8 = 14;
const MAX_JSON_DEPTH: usize = 1000;
const INFINITY_CHAR_COUNT: u8 = 5;

#[derive(Debug, Clone)]
pub struct Jsonb {
    data: Vec<u8>,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum ElementType {
    NULL = 0,
    TRUE = 1,
    FALSE = 2,
    INT = 3,
    INT5 = 4,
    FLOAT = 5,
    FLOAT5 = 6,
    TEXT = 7,
    TEXTJ = 8,
    TEXT5 = 9,
    TEXTRAW = 10,
    ARRAY = 11,
    OBJECT = 12,
    RESERVED1 = 13,
    RESERVED2 = 14,
    RESERVED3 = 15,
}

impl TryFrom<u8> for ElementType {
    type Error = LimboError;

    fn try_from(value: u8) -> std::result::Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::NULL),
            1 => Ok(Self::TRUE),
            2 => Ok(Self::FALSE),
            3 => Ok(Self::INT),
            4 => Ok(Self::INT5),
            5 => Ok(Self::FLOAT),
            6 => Ok(Self::FLOAT5),
            7 => Ok(Self::TEXT),
            8 => Ok(Self::TEXTJ),
            9 => Ok(Self::TEXT5),
            10 => Ok(Self::TEXTRAW),
            11 => Ok(Self::ARRAY),
            12 => Ok(Self::OBJECT),
            13 => Ok(Self::RESERVED1),
            14 => Ok(Self::RESERVED2),
            15 => Ok(Self::RESERVED3),
            _ => bail_parse_error!("Failed to recognize jsonvalue type"),
        }
    }
}

type PayloadSize = usize;

#[derive(Debug, Clone)]
pub struct JsonbHeader(ElementType, PayloadSize);

impl JsonbHeader {
    fn new(element_type: ElementType, payload_size: PayloadSize) -> Self {
        Self(element_type, payload_size)
    }

    fn from_slice(cursor: usize, slice: &[u8]) -> Result<(Self, usize)> {
        match slice.get(cursor) {
            Some(header_byte) => {
                // Extract first 4 bits (values 0-15)
                let element_type = header_byte & 15;
                if element_type > 12 {
                    bail_parse_error!("Invalid element type: {}", element_type);
                }
                // Get the last 4 bits for header_size
                let header_size = header_byte >> 4;
                let offset: usize;
                let total_size = match header_size {
                    size if size <= 11 => {
                        offset = 1;
                        size as usize
                    }

                    12 => match slice.get(cursor + 1) {
                        Some(value) => {
                            offset = 2;
                            *value as usize
                        }
                        None => bail_parse_error!("Failed to read 1-byte size"),
                    },

                    13 => match Self::get_size_bytes(slice, cursor + 1, 2) {
                        Ok(bytes) => {
                            offset = 3;
                            u16::from_be_bytes([bytes[0], bytes[1]]) as usize
                        }
                        Err(e) => return Err(e),
                    },

                    14 => match Self::get_size_bytes(slice, cursor + 1, 4) {
                        Ok(bytes) => {
                            offset = 5;
                            u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize
                        }
                        Err(e) => return Err(e),
                    },

                    _ => unreachable!(),
                };

                Ok((Self(element_type.try_into()?, total_size), offset))
            }
            None => bail_parse_error!("Failed to read header byte"),
        }
    }

    fn into_bytes(self) -> [u8; 5] {
        let mut bytes = [0; 5];
        let element_type = self.0;
        let payload_size = self.1;
        if payload_size <= 11 {
            bytes[0] = (element_type as u8) | ((payload_size as u8) << 4);
        } else if payload_size <= 0xFF {
            bytes[0] = (element_type as u8) | (PAYLOAD_SIZE8 << 4);
            bytes[1] = payload_size as u8;
        } else if payload_size <= 0xFFFF {
            bytes[0] = (element_type as u8) | (PAYLOAD_SIZE16 << 4);

            let size_bytes = (payload_size as u16).to_be_bytes();
            bytes[1] = size_bytes[0];
            bytes[2] = size_bytes[1];
        } else if payload_size <= 0xFFFFFFFF {
            bytes[0] = (element_type as u8) | (PAYLOAD_SIZE32 << 4);

            let size_bytes = (payload_size as u32).to_be_bytes();

            bytes[1] = size_bytes[0];
            bytes[2] = size_bytes[1];
            bytes[3] = size_bytes[2];
            bytes[4] = size_bytes[3];
        } else {
            panic!("Payload size too large for encoding");
        }

        bytes
    }

    fn get_size_bytes(slice: &[u8], start: usize, count: usize) -> Result<&[u8]> {
        match slice.get(start..start + count) {
            Some(bytes) => Ok(bytes),
            None => bail_parse_error!("Failed to read header size"),
        }
    }
}

impl Jsonb {
    pub fn new(capacity: usize, data: Option<&[u8]>) -> Self {
        if let Some(data) = data {
            return Self {
                data: data.to_vec(),
            };
        }
        Self {
            data: Vec::with_capacity(capacity),
        }
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    fn read_header(&self, cursor: usize) -> Result<(JsonbHeader, usize)> {
        let (header, offset) = JsonbHeader::from_slice(cursor, &self.data)?;

        Ok((header, offset))
    }

    pub fn is_valid(&self) -> Result<()> {
        match self.read_header(0) {
            Ok((header, offset)) => {
                if let Some(_) = self.data.get(offset..offset + header.1) {
                    Ok(())
                } else {
                    bail_parse_error!("malformed JSON")
                }
            }
            Err(_) => bail_parse_error!("malformed JSON"),
        }
    }

    #[allow(dead_code)]
    // Needed for debug. I am open to remove it
    pub fn debug_read(&self) {
        let mut cursor = 0usize;
        while cursor < self.len() {
            let (header, offset) = self.read_header(cursor).unwrap();
            println!("{}, {}", cursor, offset);
            cursor += offset;
            println!("{:?}: HEADER", header);
            if header.0 != ElementType::OBJECT || header.0 != ElementType::ARRAY {
                let value = from_utf8(&self.data[cursor..cursor + header.1]).unwrap();
                println!("{:?}: VALUE", value);
                cursor += header.1
            }
        }
    }

    pub fn to_string(&self) -> Result<String> {
        let mut result = String::with_capacity(self.data.len() * 2);
        self.write_to_string(&mut result)?;

        Ok(result)
    }

    fn write_to_string(&self, string: &mut String) -> Result<()> {
        let cursor = 0;
        let _ = self.serialize_value(string, cursor);
        Ok(())
    }

    fn serialize_value(&self, string: &mut String, cursor: usize) -> Result<usize> {
        let (header, skip_header) = self.read_header(cursor)?;
        let cursor = cursor + skip_header;

        let current_cursor = match header {
            JsonbHeader(ElementType::OBJECT, len) => self.serialize_object(string, cursor, len)?,
            JsonbHeader(ElementType::ARRAY, len) => self.serialize_array(string, cursor, len)?,
            JsonbHeader(ElementType::TEXT, len)
            | JsonbHeader(ElementType::TEXTRAW, len)
            | JsonbHeader(ElementType::TEXTJ, len)
            | JsonbHeader(ElementType::TEXT5, len) => {
                self.serialize_string(string, cursor, len, &header.0)?
            }
            JsonbHeader(ElementType::INT, len)
            | JsonbHeader(ElementType::INT5, len)
            | JsonbHeader(ElementType::FLOAT, len)
            | JsonbHeader(ElementType::FLOAT5, len) => {
                self.serialize_number(string, cursor, len, &header.0)?
            }

            JsonbHeader(ElementType::TRUE, _) => self.serialize_boolean(string, cursor, true),
            JsonbHeader(ElementType::FALSE, _) => self.serialize_boolean(string, cursor, false),
            JsonbHeader(ElementType::NULL, _) => self.serialize_null(string, cursor),
            JsonbHeader(_, _) => {
                unreachable!();
            }
        };
        Ok(current_cursor)
    }

    fn serialize_object(&self, string: &mut String, cursor: usize, len: usize) -> Result<usize> {
        let end_cursor = cursor + len;
        let mut current_cursor = cursor;
        string.push('{');
        while current_cursor < end_cursor {
            let (key_header, key_header_offset) = self.read_header(current_cursor)?;
            current_cursor += key_header_offset;
            let JsonbHeader(element_type, len) = key_header;

            match element_type {
                ElementType::TEXT
                | ElementType::TEXTRAW
                | ElementType::TEXTJ
                | ElementType::TEXT5 => {
                    current_cursor =
                        self.serialize_string(string, current_cursor, len, &element_type)?;
                }
                _ => bail_parse_error!("malformed JSON"),
            }

            string.push(':');
            current_cursor = self.serialize_value(string, current_cursor)?;
            if current_cursor < end_cursor {
                string.push(',');
            }
        }
        string.push('}');
        Ok(current_cursor)
    }

    fn serialize_array(&self, string: &mut String, cursor: usize, len: usize) -> Result<usize> {
        let end_cursor = cursor + len;
        let mut current_cursor = cursor;

        string.push('[');

        while current_cursor < end_cursor {
            current_cursor = self.serialize_value(string, current_cursor)?;
            if current_cursor < end_cursor {
                string.push(',');
            }
        }

        string.push(']');
        Ok(current_cursor)
    }

    fn serialize_string(
        &self,
        string: &mut String,
        cursor: usize,
        len: usize,
        kind: &ElementType,
    ) -> Result<usize> {
        let word_slice = &self.data[cursor..cursor + len];
        string.push('"');
        match kind {
            // Can be serialized as is. Do not need escaping
            ElementType::TEXT => {
                let word = from_utf8(word_slice).map_err(|_| {
                    LimboError::ParseError("Failed to serialize string!".to_string())
                })?;
                string.push_str(word);
            }

            // Contain standard json escapes
            ElementType::TEXTJ => {
                let word = from_utf8(word_slice).map_err(|_| {
                    LimboError::ParseError("Failed to serialize string!".to_string())
                })?;
                string.push_str(word);
            }

            // We have to escape some JSON5 escape sequences
            ElementType::TEXT5 => {
                let mut i = 0;
                while i < word_slice.len() {
                    let ch = word_slice[i];

                    // Handle normal characters that don't need escaping
                    if self.is_json_ok(ch) || ch == b'\'' {
                        string.push(ch as char);
                        i += 1;
                        continue;
                    }

                    // Handle special cases
                    match ch {
                        // Double quotes need escaping
                        b'"' => {
                            string.push_str("\\\"");
                            i += 1;
                        }

                        // Control characters (0x00-0x1F)
                        ch if ch <= 0x1F => {
                            match ch {
                                // \b
                                0x08 => string.push_str("\\b"),
                                b'\t' => string.push_str("\\t"),
                                b'\n' => string.push_str("\\n"),
                                // \f
                                0x0C => string.push_str("\\f"),
                                b'\r' => string.push_str("\\r"),
                                _ => {
                                    // Format as \u00XX
                                    let hex = format!("\\u{:04x}", ch);
                                    string.push_str(&hex);
                                }
                            }
                            i += 1;
                        }

                        // Handle escape sequences
                        b'\\' if i + 1 < word_slice.len() => {
                            let next_ch = word_slice[i + 1];
                            match next_ch {
                                // Single quote
                                b'\'' => {
                                    string.push('\'');
                                    i += 2;
                                }

                                // Vertical tab
                                b'v' => {
                                    string.push_str("\\u0009");
                                    i += 2;
                                }

                                // Hex escapes like \x27
                                b'x' if i + 3 < word_slice.len() => {
                                    string.push_str("\\u00");
                                    string.push(word_slice[i + 2] as char);
                                    string.push(word_slice[i + 3] as char);
                                    i += 4;
                                }

                                // Null character
                                b'0' => {
                                    string.push_str("\\u0000");
                                    i += 2;
                                }

                                // CR line continuation
                                b'\r' => {
                                    if i + 2 < word_slice.len() && word_slice[i + 2] == b'\n' {
                                        i += 3; // Skip CRLF
                                    } else {
                                        i += 2; // Skip CR
                                    }
                                }

                                // LF line continuation
                                b'\n' => {
                                    i += 2;
                                }

                                // Unicode line separators (U+2028 and U+2029)
                                0xe2 if i + 3 < word_slice.len()
                                    && word_slice[i + 2] == 0x80
                                    && (word_slice[i + 3] == 0xa8 || word_slice[i + 3] == 0xa9) =>
                                {
                                    i += 4;
                                }

                                // All other escapes pass through
                                _ => {
                                    string.push('\\');
                                    string.push(next_ch as char);
                                    i += 2;
                                }
                            }
                        }

                        // Default case - just push the character
                        _ => {
                            string.push(ch as char);
                            i += 1;
                        }
                    }
                }
            }

            ElementType::TEXTRAW => {
                let word = from_utf8(word_slice).map_err(|_| {
                    LimboError::ParseError("Failed to serialize string!".to_string())
                })?;

                for ch in word.chars() {
                    match ch {
                        '"' => string.push_str("\\\""),
                        '\\' => string.push_str("\\\\"),
                        '\x08' => string.push_str("\\b"),
                        '\x0C' => string.push_str("\\f"),
                        '\n' => string.push_str("\\n"),
                        '\r' => string.push_str("\\r"),
                        '\t' => string.push_str("\\t"),
                        c if c <= '\u{001F}' => {
                            string.push_str(&format!("\\u{:04x}", c as u32));
                        }
                        _ => string.push(ch),
                    }
                }
            }

            _ => {
                unreachable!()
            }
        }
        string.push('"');
        Ok(cursor + len)
    }

    fn is_json_ok(&self, ch: u8) -> bool {
        (0x20..=0x7E).contains(&ch) && ch != b'"' && ch != b'\\'
    }

    fn serialize_number(
        &self,
        string: &mut String,
        cursor: usize,
        len: usize,
        kind: &ElementType,
    ) -> Result<usize> {
        let current_cursor = cursor + len;
        let num_slice = from_utf8(&self.data[cursor..current_cursor])
            .map_err(|_| LimboError::ParseError("Failed to parse integer".to_string()))?;

        match kind {
            ElementType::INT | ElementType::FLOAT => {
                string.push_str(num_slice);
            }
            ElementType::INT5 => {
                self.serialize_int5(string, num_slice)?;
            }
            ElementType::FLOAT5 => {
                self.serialize_float5(string, num_slice)?;
            }
            _ => unreachable!(),
        }
        Ok(current_cursor)
    }

    fn serialize_int5(&self, string: &mut String, hex_str: &str) -> Result<()> {
        // Check if number is hex
        if hex_str.len() > 2
            && (hex_str[..2].eq_ignore_ascii_case("0x")
                || (hex_str.starts_with("-") || hex_str.starts_with("+"))
                    && hex_str[1..3].eq_ignore_ascii_case("0x"))
        {
            let (sign, hex_part) = if hex_str.starts_with("-0x") || hex_str.starts_with("-0X") {
                ("-", &hex_str[3..])
            } else if hex_str.starts_with("+0x") || hex_str.starts_with("+0X") {
                ("", &hex_str[3..])
            } else {
                ("", &hex_str[2..])
            };

            // Add sign
            string.push_str(sign);

            let mut value = 0u64;

            for ch in hex_part.chars() {
                if !ch.is_ascii_hexdigit() {
                    bail_parse_error!("Failed to parse hex digit: {}", hex_part);
                }

                if (value >> 60) != 0 {
                    string.push_str("9.0e999");
                    return Ok(());
                }

                value = value * 16 + ch.to_digit(16).unwrap_or(0) as u64;
            }
            write!(string, "{}", value)
                .map_err(|_| LimboError::ParseError("Error writing string to json!".to_string()))?;
        } else {
            string.push_str(hex_str);
        }

        Ok(())
    }

    fn serialize_float5(&self, string: &mut String, float_str: &str) -> Result<()> {
        if float_str.len() < 2 {
            bail_parse_error!("Integer is less then 2 chars: {}", float_str);
        }
        match float_str {
            "9e999" | "-9e999" => {
                string.push_str(float_str);
            }
            val if val.starts_with("-.") => {
                string.push_str("-0.");
                string.push_str(&val[2..]);
            }
            val if val.starts_with("+.") => {
                string.push_str("0.");
                string.push_str(&val[2..]);
            }
            val if val.starts_with(".") => {
                string.push_str("0.");
                string.push_str(&val[1..]);
            }
            val if val
                .chars()
                .next()
                .map_or(false, |c| c.is_ascii_alphanumeric() || c == '+' || c == '-') =>
            {
                string.push_str(val);
                string.push('0');
            }
            _ => bail_parse_error!("Unable to serialize float5: {}", float_str),
        }

        Ok(())
    }

    fn serialize_boolean(&self, string: &mut String, cursor: usize, val: bool) -> usize {
        if val {
            string.push_str("true");
        } else {
            string.push_str("false");
        }

        cursor
    }

    fn serialize_null(&self, string: &mut String, cursor: usize) -> usize {
        string.push_str("null");
        cursor
    }

    fn deserialize_value<'a, I>(&mut self, input: &mut Peekable<I>, depth: usize) -> Result<usize>
    where
        I: Iterator<Item = &'a u8>,
    {
        if depth > MAX_JSON_DEPTH {
            bail_parse_error!("Too deep")
        };
        let current_depth = depth + 1;
        skip_whitespace(input);
        match input.peek() {
            Some(b'{') => {
                input.next(); // consume '{'
                self.deserialize_obj(input, current_depth)
            }
            Some(b'[') => {
                input.next(); // consume '['
                self.deserialize_array(input, current_depth)
            }
            Some(b't') => self.deserialize_true(input),
            Some(b'f') => self.deserialize_false(input),
            Some(b'n') => self.deserialize_null_or_nan(input),
            Some(b'"') => self.deserialize_string(input),
            Some(b'\'') => self.deserialize_string(input),
            Some(&&c)
                if c.is_ascii_digit()
                    || c == b'-'
                    || c == b'+'
                    || c == b'.'
                    || c.to_ascii_lowercase() == b'i' =>
            {
                self.deserialize_number(input)
            }
            Some(ch) => bail_parse_error!("Unexpected character: {}", ch),
            None => Ok(0),
        }
    }

    pub fn deserialize_obj<'a, I>(&mut self, input: &mut Peekable<I>, depth: usize) -> Result<usize>
    where
        I: Iterator<Item = &'a u8>,
    {
        if depth > MAX_JSON_DEPTH {
            bail_parse_error!("Too deep!")
        }
        let header_pos = self.len();
        self.write_element_header(header_pos, ElementType::OBJECT, 0)?;
        let obj_start = self.len();
        let mut first = true;
        let current_depth = depth + 1;
        loop {
            skip_whitespace(input);

            match input.peek() {
                Some(&&b'}') => {
                    input.next(); // consume '}'
                    if first {
                        return Ok(1); // empty header
                    } else {
                        let obj_size = self.len() - obj_start;
                        self.write_element_header(header_pos, ElementType::OBJECT, obj_size)?;
                        return Ok(obj_size + 2);
                    }
                }
                Some(&&b',') if !first => {
                    input.next(); // consume ','
                    skip_whitespace(input);
                }
                Some(_) => {
                    // Parse key (must be string)
                    self.deserialize_string(input)?;

                    skip_whitespace(input);

                    // Expect and consume ':'
                    if input.next() != Some(&b':') {
                        bail_parse_error!("Expected ':' after object key");
                    }

                    skip_whitespace(input);

                    // Parse value - can be any JSON value including another object
                    self.deserialize_value(input, current_depth)?;

                    first = false;
                }
                None => {
                    bail_parse_error!("Unexpected end of input!")
                }
            }
        }
    }

    pub fn deserialize_array<'a, I>(
        &mut self,
        input: &mut Peekable<I>,
        depth: usize,
    ) -> Result<usize>
    where
        I: Iterator<Item = &'a u8>,
    {
        if depth > MAX_JSON_DEPTH {
            bail_parse_error!("Too deep");
        }
        let header_pos = self.len();
        self.write_element_header(header_pos, ElementType::ARRAY, 0)?;
        let arr_start = self.len();
        let mut first = true;
        let current_depth = depth + 1;
        loop {
            skip_whitespace(input);

            match input.peek() {
                Some(&&b']') => {
                    input.next();
                    if first {
                        return Ok(1);
                    } else {
                        let arr_len = self.len() - arr_start;
                        let header_size =
                            self.write_element_header(header_pos, ElementType::ARRAY, arr_len)?;
                        return Ok(arr_len + header_size);
                    }
                }
                Some(&&b',') if !first => {
                    input.next(); // consume ','
                    skip_whitespace(input);
                }
                Some(_) => {
                    skip_whitespace(input);
                    self.deserialize_value(input, current_depth)?;

                    first = false;
                }
                None => {
                    bail_parse_error!("Unexpected end of input!")
                }
            }
        }
    }

    fn deserialize_string<'a, I>(&mut self, input: &mut Peekable<I>) -> Result<usize>
    where
        I: Iterator<Item = &'a u8>,
    {
        let string_start = self.len();
        let quote = input.next().unwrap(); // "
        let quoted = quote == &b'"' || quote == &b'\'';
        let mut len = 0;
        self.write_element_header(string_start, ElementType::TEXT, 0)?;
        let payload_start = self.len();

        if input.peek().is_none() {
            bail_parse_error!("Unexpected end of input in string handling");
        };

        let mut element_type = ElementType::TEXT;
        // This needed to support 1 char unquoted JSON5 keys
        if !quoted {
            self.data.push(*quote);
            len += 1;
            if let Some(&&c) = input.peek() {
                if c == b':' {
                    self.write_element_header(string_start, element_type, len)?;

                    return Ok(self.len() - payload_start);
                }
            }
        };

        while let Some(c) = input.next() {
            if c == quote && quoted {
                break;
            } else if c == &b'\\' {
                // Handle escapes
                if let Some(&esc) = input.next() {
                    match esc {
                        b'b' => {
                            self.data.push(b'\\');
                            self.data.push(b'b');
                            len += 2;
                            element_type = ElementType::TEXTJ;
                        }
                        b'f' => {
                            self.data.push(b'\\');
                            self.data.push(b'f');
                            len += 2;
                            element_type = ElementType::TEXTJ;
                        }
                        b'n' => {
                            self.data.push(b'\\');
                            self.data.push(b'n');
                            len += 2;
                            element_type = ElementType::TEXTJ;
                        }
                        b'r' => {
                            self.data.push(b'\\');
                            self.data.push(b'r');
                            len += 2;
                            element_type = ElementType::TEXTJ;
                        }
                        b't' => {
                            self.data.push(b'\\');
                            self.data.push(b't');
                            len += 2;
                            element_type = ElementType::TEXTJ;
                        }
                        b'\\' | b'"' | b'/' => {
                            self.data.push(b'\\');
                            self.data.push(esc);
                            len += 2;
                            element_type = ElementType::TEXTJ;
                        }
                        b'u' => {
                            // Unicode escape
                            element_type = ElementType::TEXTJ;
                            self.data.push(b'\\');
                            self.data.push(b'u');
                            len += 2;
                            for _ in 0..4 {
                                if let Some(&h) = input.next() {
                                    if is_hex_digit(h) {
                                        self.data.push(h);
                                        len += 1;
                                    } else {
                                        bail_parse_error!("Incomplete Unicode escape");
                                    }
                                } else {
                                    bail_parse_error!("Incomplete Unicode escape");
                                }
                            }
                        }
                        // JSON5 extensions
                        b'\n' => {
                            element_type = ElementType::TEXT5;
                            self.data.push(b'\\');
                            self.data.push(b'\n');
                            len += 2;
                        }
                        b'\'' => {
                            element_type = ElementType::TEXT5;
                            self.data.push(b'\\');
                            self.data.push(b'\'');
                            len += 2;
                        }
                        b'0' => {
                            element_type = ElementType::TEXT5;
                            self.data.push(b'\\');
                            self.data.push(b'0');
                            len += 2;
                        }
                        b'v' => {
                            element_type = ElementType::TEXT5;
                            self.data.push(b'\\');
                            self.data.push(b'v');
                            len += 2;
                        }
                        b'x' => {
                            element_type = ElementType::TEXT5;
                            self.data.push(b'\\');
                            self.data.push(b'x');
                            len += 2;
                            for _ in 0..2 {
                                if let Some(&h) = input.next() {
                                    if is_hex_digit(h) {
                                        self.data.push(h);
                                        len += 1;
                                    } else {
                                        bail_parse_error!("Invalid hex escape sequence");
                                    }
                                } else {
                                    bail_parse_error!("Incomplete hex escape sequence");
                                }
                            }
                        }
                        _ => {
                            bail_parse_error!("Invalid escape sequence")
                        }
                    }
                } else {
                    bail_parse_error!("Unexpected end of input in escape sequence");
                }
            } else if c <= &0x1F {
                element_type = ElementType::TEXT5;
                self.data.push(*c);
                len += 1;
            } else {
                self.data.push(*c);
                len += 1;
            }
            if let Some(&&c) = input.peek() {
                if (c == b':' || c.is_ascii_whitespace()) && !quoted {
                    break;
                }
            }
        }

        // Write header and payload
        self.write_element_header(string_start, element_type, len)?;

        Ok(self.len() - payload_start)
    }

    pub fn deserialize_number<'a, I>(&mut self, input: &mut Peekable<I>) -> Result<usize>
    where
        I: Iterator<Item = &'a u8>,
    {
        let num_start = self.len();
        let mut len = 0;
        let mut is_float = false;
        let mut is_json5 = false;

        // Dummy header
        self.write_element_header(num_start, ElementType::INT, 0)?;

        // Handle sign
        if input.peek() == Some(&&b'-') || input.peek() == Some(&&b'+') {
            if input.peek() == Some(&&b'+') {
                is_json5 = true;
                input.next();
            } else {
                self.data.push(*input.next().unwrap());
                len += 1;
            }
        }

        // Handle json5 float number
        if input.peek() == Some(&&b'.') {
            is_json5 = true;
        };

        // Check for hex (JSON5)
        if input.peek() == Some(&&b'0') {
            self.data.push(*input.next().unwrap());
            len += 1;
            let next_ch = input.peek();
            if let Some(&&ch) = next_ch {
                if ch == b'x' || ch == b'X' {
                    self.data.push(*input.next().unwrap());
                    len += 1;
                    while let Some(&&byte) = input.peek() {
                        if is_hex_digit(byte) {
                            self.data.push(*input.next().unwrap());
                            len += 1;
                        } else {
                            break;
                        }
                    }

                    self.write_element_header(num_start, ElementType::INT5, len)?;

                    return Ok(self.len() - num_start);
                } else if ch.is_ascii_alphanumeric() {
                    bail_parse_error!("Leading zero is not allowed")
                }
            }
        }

        // Check for Infinity
        if input.peek().map(|x| x.to_ascii_lowercase()) == Some(b'i') {
            for expected in b"infinity" {
                if input.next().map(|x| x.to_ascii_lowercase()) != Some(*expected) {
                    bail_parse_error!("Failed to parse number");
                }
            }
            self.write_element_header(
                num_start,
                ElementType::FLOAT5,
                len + INFINITY_CHAR_COUNT as usize,
            )?;

            self.data.extend_from_slice(b"9e999");

            return Ok(self.len() - num_start);
        };

        // Regular number parsing
        while let Some(&&ch) = input.peek() {
            match ch {
                b'0'..=b'9' => {
                    self.data.push(*input.next().unwrap());
                    len += 1;
                }
                b'.' => {
                    is_float = true;
                    self.data.push(*input.next().unwrap());
                    let next_ch = input.peek();
                    match next_ch {
                        Some(ch) => {
                            println!("{}", **ch as char);
                            if !ch.is_ascii_alphanumeric() {
                                is_json5 = true;
                            }
                        }
                        None => {
                            is_json5 = true;
                        }
                    };
                    len += 1;
                }
                b'e' | b'E' => {
                    is_float = true;
                    self.data.push(*input.next().unwrap());
                    len += 1;
                    if input.peek() == Some(&&b'+') || input.peek() == Some(&&b'-') {
                        self.data.push(*input.next().unwrap());
                        len += 1;
                    }
                }
                _ => break,
            }
        }

        // Write appropriate header and payload
        let element_type = if is_float {
            if is_json5 {
                ElementType::FLOAT5
            } else {
                ElementType::FLOAT
            }
        } else {
            if is_json5 {
                ElementType::INT5
            } else {
                ElementType::INT
            }
        };

        self.write_element_header(num_start, element_type, len)?;

        Ok(self.len() - num_start)
    }

    pub fn deserialize_null_or_nan<'a, I>(&mut self, input: &mut Peekable<I>) -> Result<usize>
    where
        I: Iterator<Item = &'a u8>,
    {
        let start = self.len();
        let nul = b"null";
        let nan = b"nan";
        let mut nan_score = 0;
        let mut nul_score = 0;
        for i in 0..4 {
            if nan_score == 3 {
                self.data.push(ElementType::NULL as u8);
                return Ok(self.len() - start);
            };
            let nul_ch = nul.get(i);
            let nan_ch = nan.get(i);
            let ch = input.next();
            if nan_ch != ch && nul_ch != ch {
                bail_parse_error!("expected null or nan");
            }
            if nan_ch == ch {
                nan_score += 1;
            }
            if nul_ch == ch {
                nul_score += 1;
            }
        }
        if nul_score == 4 {
            self.data.push(ElementType::NULL as u8);
            Ok(self.len() - start)
        } else {
            bail_parse_error!("expected null or nan");
        }
    }

    pub fn deserialize_true<'a, I>(&mut self, input: &mut Peekable<I>) -> Result<usize>
    where
        I: Iterator<Item = &'a u8>,
    {
        let start = self.len();
        for expected in b"true" {
            if input.next() != Some(expected) {
                bail_parse_error!("Expected 'true'");
            }
        }
        self.data.push(ElementType::TRUE as u8);
        Ok(self.len() - start)
    }

    fn deserialize_false<'a, I>(&mut self, input: &mut Peekable<I>) -> Result<usize>
    where
        I: Iterator<Item = &'a u8>,
    {
        let start = self.len();
        for expected in b"false" {
            if input.next() != Some(expected) {
                bail_parse_error!("Expected 'false'");
            }
        }
        self.data.push(ElementType::FALSE as u8);
        Ok(self.len() - start)
    }

    fn write_element_header(
        &mut self,
        cursor: usize,
        element_type: ElementType,
        payload_size: usize,
    ) -> Result<usize> {
        let header = JsonbHeader::new(element_type, payload_size).into_bytes();
        if cursor == self.len() {
            for byte in header {
                if byte != 0 {
                    self.data.push(byte);
                }
            }
        } else {
            self.data[cursor] = header[0];
            self.data.splice(
                cursor + 1..cursor + 1,
                header[1..].iter().filter(|&&x| x != 0).cloned(),
            );
        }
        Ok(header.iter().filter(|&&x| x != 0).count())
    }

    fn from_str(input: &str) -> Result<Self> {
        let mut result = Self::new(input.len(), None);
        let mut input_iter = input.as_bytes().iter().peekable();
        while input_iter.peek().is_some() {
            result.deserialize_value(&mut input_iter, 0)?;
        }

        Ok(result)
    }

    pub fn data(self) -> Vec<u8> {
        self.data
    }
}

impl std::str::FromStr for Jsonb {
    type Err = LimboError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        Self::from_str(s)
    }
}

pub fn skip_whitespace<'a, I>(input: &mut Peekable<I>)
where
    I: Iterator<Item = &'a u8>,
{
    while let Some(&ch) = input.peek() {
        match ch {
            b' ' | b'\t' | b'\n' | b'\r' => {
                input.next();
            }
            b'/' => {
                // Handle JSON5 comments
                input.next();
                if let Some(&&next_ch) = input.peek() {
                    if next_ch == b'/' {
                        // Line comment - skip until newline
                        input.next();
                        while let Some(&c) = input.next() {
                            if c == b'\n' {
                                break;
                            }
                        }
                    } else if next_ch == b'*' {
                        // Block comment - skip until "*/"
                        input.next();
                        let mut prev = b'\0';
                        while let Some(&c) = input.next() {
                            if prev == b'*' && c == b'/' {
                                break;
                            }
                            prev = c;
                        }
                    } else {
                        // Not a comment, put the '/' back
                        break;
                    }
                } else {
                    break;
                }
            }
            _ => break,
        }
    }
}

fn is_hex_digit(b: u8) -> bool {
    matches!(b, b'0'..=b'9' | b'a'..=b'f' | b'A'..=b'F')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_null_serialization() {
        // Create JSONB with null value
        let mut jsonb = Jsonb::new(10, None);
        jsonb.data.push(ElementType::NULL as u8);

        // Test serialization
        let json_str = jsonb.to_string().unwrap();
        assert_eq!(json_str, "null");

        // Test round-trip
        let reparsed = Jsonb::from_str("null").unwrap();
        assert_eq!(reparsed.data[0] as u8, ElementType::NULL as u8);
    }

    #[test]
    fn test_boolean_serialization() {
        // True
        let mut jsonb_true = Jsonb::new(10, None);
        jsonb_true.data.push(ElementType::TRUE as u8);
        assert_eq!(jsonb_true.to_string().unwrap(), "true");

        // False
        let mut jsonb_false = Jsonb::new(10, None);
        jsonb_false.data.push(ElementType::FALSE as u8);
        assert_eq!(jsonb_false.to_string().unwrap(), "false");

        // Round-trip
        let true_parsed = Jsonb::from_str("true").unwrap();
        assert_eq!(true_parsed.data[0] as u8, ElementType::TRUE as u8);

        let false_parsed = Jsonb::from_str("false").unwrap();
        assert_eq!(false_parsed.data[0] as u8, ElementType::FALSE as u8);
    }

    #[test]
    fn test_integer_serialization() {
        // Standard integer
        let parsed = Jsonb::from_str("42").unwrap();
        assert_eq!(parsed.to_string().unwrap(), "42");

        // Negative integer
        let parsed = Jsonb::from_str("-123").unwrap();
        assert_eq!(parsed.to_string().unwrap(), "-123");

        // Zero
        let parsed = Jsonb::from_str("0").unwrap();
        assert_eq!(parsed.to_string().unwrap(), "0");

        // Verify correct type
        let header = JsonbHeader::from_slice(0, &parsed.data).unwrap().0;
        assert!(matches!(header.0, ElementType::INT));
    }

    #[test]
    fn test_json5_integer_serialization() {
        // Hexadecimal notation
        let parsed = Jsonb::from_str("0x1A").unwrap();
        assert_eq!(parsed.to_string().unwrap(), "26"); // Should convert to decimal

        // Positive sign (JSON5)
        let parsed = Jsonb::from_str("+42").unwrap();
        assert_eq!(parsed.to_string().unwrap(), "42");

        // Negative hexadecimal
        let parsed = Jsonb::from_str("-0xFF").unwrap();
        assert_eq!(parsed.to_string().unwrap(), "-255");

        // Verify correct type
        let header = JsonbHeader::from_slice(0, &parsed.data).unwrap().0;
        assert!(matches!(header.0, ElementType::INT5));
    }

    #[test]
    fn test_float_serialization() {
        // Standard float
        let parsed = Jsonb::from_str("3.14159").unwrap();
        assert_eq!(parsed.to_string().unwrap(), "3.14159");

        // Negative float
        let parsed = Jsonb::from_str("-2.718").unwrap();
        assert_eq!(parsed.to_string().unwrap(), "-2.718");

        // Scientific notation
        let parsed = Jsonb::from_str("6.022e23").unwrap();
        assert_eq!(parsed.to_string().unwrap(), "6.022e23");

        // Verify correct type
        let header = JsonbHeader::from_slice(0, &parsed.data).unwrap().0;
        assert!(matches!(header.0, ElementType::FLOAT));
    }

    #[test]
    fn test_json5_float_serialization() {
        // Leading decimal point
        let parsed = Jsonb::from_str(".123").unwrap();
        assert_eq!(parsed.to_string().unwrap(), "0.123");

        // Trailing decimal point
        let parsed = Jsonb::from_str("42.").unwrap();
        assert_eq!(parsed.to_string().unwrap(), "42.0");

        // Plus sign in exponent
        let parsed = Jsonb::from_str("1.5e+10").unwrap();
        assert_eq!(parsed.to_string().unwrap(), "1.5e+10");

        // Infinity
        let parsed = Jsonb::from_str("Infinity").unwrap();
        assert_eq!(parsed.to_string().unwrap(), "9e999");

        // Negative Infinity
        let parsed = Jsonb::from_str("-Infinity").unwrap();
        assert_eq!(parsed.to_string().unwrap(), "-9e999");

        // Verify correct type
        let header = JsonbHeader::from_slice(0, &parsed.data).unwrap().0;
        assert!(matches!(header.0, ElementType::FLOAT5));
    }

    #[test]
    fn test_string_serialization() {
        // Simple string
        let parsed = Jsonb::from_str(r#""hello world""#).unwrap();
        assert_eq!(parsed.to_string().unwrap(), r#""hello world""#);

        // String with escaped characters
        let parsed = Jsonb::from_str(r#""hello\nworld""#).unwrap();
        assert_eq!(parsed.to_string().unwrap(), r#""hello\nworld""#);

        // Unicode escape
        let parsed = Jsonb::from_str(r#""hello\u0020world""#).unwrap();
        assert_eq!(parsed.to_string().unwrap(), r#""hello\u0020world""#);

        // Verify correct type
        let header = JsonbHeader::from_slice(0, &parsed.data).unwrap().0;
        assert!(matches!(header.0, ElementType::TEXTJ));
    }

    #[test]
    fn test_json5_string_serialization() {
        // Single quotes
        let parsed = Jsonb::from_str("'hello world'").unwrap();
        assert_eq!(parsed.to_string().unwrap(), r#""hello world""#);

        // Hex escape
        let parsed = Jsonb::from_str(r#"'\x41\x42\x43'"#).unwrap();
        assert_eq!(parsed.to_string().unwrap(), r#""\u0041\u0042\u0043""#);

        // Multiline string with line continuation
        let parsed = Jsonb::from_str(
            r#""hello \
world""#,
        )
        .unwrap();
        assert_eq!(parsed.to_string().unwrap(), r#""hello world""#);

        // Escaped single quote
        let parsed = Jsonb::from_str(r#"'Don\'t worry'"#).unwrap();
        assert_eq!(parsed.to_string().unwrap(), r#""Don't worry""#);

        // Verify correct type
        let header = JsonbHeader::from_slice(0, &parsed.data).unwrap().0;
        assert!(matches!(header.0, ElementType::TEXT5));
    }

    #[test]
    fn test_array_serialization() {
        // Empty array
        let parsed = Jsonb::from_str("[]").unwrap();
        assert_eq!(parsed.to_string().unwrap(), "[]");

        // Simple array
        let parsed = Jsonb::from_str("[1,2,3]").unwrap();
        assert_eq!(parsed.to_string().unwrap(), "[1,2,3]");

        // Nested array
        let parsed = Jsonb::from_str("[[1,2],[3,4]]").unwrap();
        assert_eq!(parsed.to_string().unwrap(), "[[1,2],[3,4]]");

        // Mixed types array
        let parsed = Jsonb::from_str(r#"[1,"text",true,null,{"key":"value"}]"#).unwrap();
        assert_eq!(
            parsed.to_string().unwrap(),
            r#"[1,"text",true,null,{"key":"value"}]"#
        );

        // Verify correct type
        let header = JsonbHeader::from_slice(0, &parsed.data).unwrap().0;
        assert!(matches!(header.0, ElementType::ARRAY));
    }

    #[test]
    fn test_json5_array_serialization() {
        // Trailing comma
        let parsed = Jsonb::from_str("[1,2,3,]").unwrap();
        assert_eq!(parsed.to_string().unwrap(), "[1,2,3]");

        // Comments in array
        let parsed = Jsonb::from_str("[1,/* comment */2,3]").unwrap();
        assert_eq!(parsed.to_string().unwrap(), "[1,2,3]");

        // Line comment in array
        let parsed = Jsonb::from_str("[1,// line comment\n2,3]").unwrap();
        assert_eq!(parsed.to_string().unwrap(), "[1,2,3]");
    }

    #[test]
    fn test_object_serialization() {
        // Empty object
        let parsed = Jsonb::from_str("{}").unwrap();
        assert_eq!(parsed.to_string().unwrap(), "{}");

        // Simple object
        let parsed = Jsonb::from_str(r#"{"key":"value"}"#).unwrap();
        assert_eq!(parsed.to_string().unwrap(), r#"{"key":"value"}"#);

        // Multiple properties
        let parsed = Jsonb::from_str(r#"{"a":1,"b":2,"c":3}"#).unwrap();
        assert_eq!(parsed.to_string().unwrap(), r#"{"a":1,"b":2,"c":3}"#);

        // Nested object
        let parsed = Jsonb::from_str(r#"{"outer":{"inner":"value"}}"#).unwrap();
        assert_eq!(
            parsed.to_string().unwrap(),
            r#"{"outer":{"inner":"value"}}"#
        );

        // Mixed values
        let parsed =
            Jsonb::from_str(r#"{"str":"text","num":42,"bool":true,"null":null,"arr":[1,2]}"#)
                .unwrap();
        assert_eq!(
            parsed.to_string().unwrap(),
            r#"{"str":"text","num":42,"bool":true,"null":null,"arr":[1,2]}"#
        );

        // Verify correct type
        let header = JsonbHeader::from_slice(0, &parsed.data).unwrap().0;
        assert!(matches!(header.0, ElementType::OBJECT));
    }

    #[test]
    fn test_json5_object_serialization() {
        // Unquoted keys
        let parsed = Jsonb::from_str("{key:\"value\"}").unwrap();
        assert_eq!(parsed.to_string().unwrap(), r#"{"key":"value"}"#);

        // Trailing comma
        let parsed = Jsonb::from_str(r#"{"a":1,"b":2,}"#).unwrap();
        assert_eq!(parsed.to_string().unwrap(), r#"{"a":1,"b":2}"#);

        // Comments in object
        let parsed = Jsonb::from_str(r#"{"a":1,/*comment*/"b":2}"#).unwrap();
        assert_eq!(parsed.to_string().unwrap(), r#"{"a":1,"b":2}"#);

        // Single quotes for keys and values
        let parsed = Jsonb::from_str("{'a':'value'}").unwrap();
        assert_eq!(parsed.to_string().unwrap(), r#"{"a":"value"}"#);
    }

    #[test]
    fn test_complex_json() {
        let complex_json = r#"{
            "string": "Hello, world!",
            "number": 42,
            "float": 3.14159,
            "boolean": true,
            "null": null,
            "array": [1, 2, 3, "text", {"nested": "object"}],
            "object": {
                "key1": "value1",
                "key2": [4, 5, 6],
                "key3": {
                    "nested": true
                }
            }
        }"#;

        let parsed = Jsonb::from_str(complex_json).unwrap();
        // Round-trip test
        let reparsed = Jsonb::from_str(&parsed.to_string().unwrap()).unwrap();
        assert_eq!(parsed.to_string().unwrap(), reparsed.to_string().unwrap());
    }

    #[test]
    fn test_error_handling() {
        // Invalid JSON syntax
        assert!(Jsonb::from_str("{").is_err());
        assert!(Jsonb::from_str("[").is_err());
        assert!(Jsonb::from_str("}").is_err());
        assert!(Jsonb::from_str("]").is_err());

        // Unclosed string
        assert!(Jsonb::from_str(r#"{"key":"value"#).is_err());

        // Invalid number format
        assert!(Jsonb::from_str("01234").is_err()); // Leading zero not allowed in JSON

        // Invalid escape sequence
        assert!(Jsonb::from_str(r#""\z""#).is_err());

        // Missing colon in object
        assert!(Jsonb::from_str(r#"{"key" "value"}"#).is_err());

        // Trailing characters
        assert!(Jsonb::from_str(r#"{"key":"value"} extra"#).is_err());
    }

    #[test]
    fn test_depth_limit() {
        // Create a JSON string that exceeds MAX_JSON_DEPTH
        let mut deep_json = String::from("[");
        for _ in 0..MAX_JSON_DEPTH + 1 {
            deep_json.push_str("[");
        }
        for _ in 0..MAX_JSON_DEPTH + 1 {
            deep_json.push_str("]");
        }
        deep_json.push_str("]");

        // Should fail due to exceeding depth limit
        assert!(Jsonb::from_str(&deep_json).is_err());
    }

    #[test]
    fn test_header_encoding() {
        // Small payload (fits in 4 bits)
        let header = JsonbHeader::new(ElementType::TEXT, 5);
        let bytes = header.into_bytes();
        assert_eq!(bytes[0], (5 << 4) | (ElementType::TEXT as u8));

        // Medium payload (8-bit)
        let header = JsonbHeader::new(ElementType::TEXT, 200);
        let bytes = header.into_bytes();
        assert_eq!(bytes[0], (PAYLOAD_SIZE8 << 4) | (ElementType::TEXT as u8));
        assert_eq!(bytes[1], 200);

        // Large payload (16-bit)
        let header = JsonbHeader::new(ElementType::TEXT, 40000);
        let bytes = header.into_bytes();
        assert_eq!(bytes[0], (PAYLOAD_SIZE16 << 4) | (ElementType::TEXT as u8));
        assert_eq!(bytes[1], (40000 >> 8) as u8);
        assert_eq!(bytes[2], (40000 & 0xFF) as u8);

        // Extra large payload (32-bit)
        let header = JsonbHeader::new(ElementType::TEXT, 70000);
        let bytes = header.into_bytes();
        assert_eq!(bytes[0], (PAYLOAD_SIZE32 << 4) | (ElementType::TEXT as u8));
        assert_eq!(bytes[1], (70000 >> 24) as u8);
        assert_eq!(bytes[2], ((70000 >> 16) & 0xFF) as u8);
        assert_eq!(bytes[3], ((70000 >> 8) & 0xFF) as u8);
        assert_eq!(bytes[4], (70000 & 0xFF) as u8);
    }

    #[test]
    fn test_header_decoding() {
        // Create sample data with various headers
        let mut data = Vec::new();

        // Small payload
        data.push((5 << 4) | (ElementType::TEXT as u8));

        // Medium payload (8-bit)
        data.push((PAYLOAD_SIZE8 << 4) | (ElementType::ARRAY as u8));
        data.push(150); // Payload size

        // Large payload (16-bit)
        data.push((PAYLOAD_SIZE16 << 4) | (ElementType::OBJECT as u8));
        data.push(0x98); // High byte of 39000
        data.push(0x68); // Low byte of 39000

        // Parse and verify each header
        let (header1, offset1) = JsonbHeader::from_slice(0, &data).unwrap();
        assert_eq!(offset1, 1);
        assert_eq!(header1.0, ElementType::TEXT);
        assert_eq!(header1.1, 5);

        let (header2, offset2) = JsonbHeader::from_slice(1, &data).unwrap();
        assert_eq!(offset2, 2);
        assert_eq!(header2.0, ElementType::ARRAY);
        assert_eq!(header2.1, 150);

        let (header3, offset3) = JsonbHeader::from_slice(3, &data).unwrap();
        assert_eq!(offset3, 3);
        assert_eq!(header3.0, ElementType::OBJECT);
        assert_eq!(header3.1, 0x9868); // 39000
    }

    #[test]
    fn test_unicode_escapes() {
        // Basic unicode escape
        let parsed = Jsonb::from_str(r#""\u00A9""#).unwrap(); // Copyright symbol
        assert_eq!(parsed.to_string().unwrap(), r#""\u00A9""#);

        // Non-BMP character (surrogate pair)
        let parsed = Jsonb::from_str(r#""\uD83D\uDE00""#).unwrap(); // Smiley emoji
        assert_eq!(parsed.to_string().unwrap(), r#""\uD83D\uDE00""#);
    }

    #[test]
    fn test_json5_comments() {
        // Line comments
        let parsed = Jsonb::from_str(
            r#"{
            // This is a line comment
            "key": "value"
        }"#,
        )
        .unwrap();
        assert_eq!(parsed.to_string().unwrap(), r#"{"key":"value"}"#);

        // Block comments
        let parsed = Jsonb::from_str(
            r#"{
            /* This is a
               block comment */
            "key": "value"
        }"#,
        )
        .unwrap();
        assert_eq!(parsed.to_string().unwrap(), r#"{"key":"value"}"#);

        // Comments inside array
        let parsed = Jsonb::from_str(
            r#"[1, // Comment
                                       2, /* Another comment */ 3]"#,
        )
        .unwrap();
        assert_eq!(parsed.to_string().unwrap(), "[1,2,3]");
    }

    #[test]
    fn test_whitespace_handling() {
        // Various whitespace patterns
        let json_with_whitespace = r#"
        {
            "key1"    :    "value1"   ,
             "key2": [   1,    2,    3   ]  ,
            "key3":   {
                "nested"   :   true
            }
        }
        "#;

        let parsed = Jsonb::from_str(json_with_whitespace).unwrap();
        assert_eq!(
            parsed.to_string().unwrap(),
            r#"{"key1":"value1","key2":[1,2,3],"key3":{"nested":true}}"#
        );
    }

    #[test]
    fn test_binary_roundtrip() {
        // Test that binary data can be round-tripped through the JSONB format
        let original = r#"{"test":"value","array":[1,2,3]}"#;
        let parsed = Jsonb::from_str(original).unwrap();
        let binary_data = parsed.data.clone();

        // Create a new Jsonb from the binary data
        let from_binary = Jsonb::new(0, Some(&binary_data));
        assert_eq!(from_binary.to_string().unwrap(), original);
    }

    #[test]
    fn test_large_json() {
        // Generate a large JSON with many elements
        let mut large_array = String::from("[");
        for i in 0..1000 {
            large_array.push_str(&format!("{}", i));
            if i < 999 {
                large_array.push_str(",");
            }
        }
        large_array.push_str("]");

        let parsed = Jsonb::from_str(&large_array).unwrap();
        assert!(parsed.to_string().unwrap().starts_with("[0,1,2,"));
        assert!(parsed.to_string().unwrap().ends_with("998,999]"));
    }

    #[test]
    fn test_jsonb_is_valid() {
        // Valid JSONB
        let jsonb = Jsonb::from_str(r#"{"test":"value"}"#).unwrap();
        assert!(jsonb.is_valid().is_ok());

        // Invalid JSONB (manually corrupted)
        let mut invalid = jsonb.data.clone();
        if !invalid.is_empty() {
            invalid[0] = 0xFF; // Invalid element type
            let jsonb = Jsonb::new(0, Some(&invalid));
            assert!(jsonb.is_valid().is_err());
        }
    }

    #[test]
    fn test_special_characters_in_strings() {
        // Test handling of various special characters
        let json = r#"{
            "escaped_quotes": "He said \"Hello\"",
            "backslashes": "C:\\Windows\\System32",
            "control_chars": "\b\f\n\r\t",
            "unicode": "\u00A9 2023"
        }"#;

        let parsed = Jsonb::from_str(json).unwrap();
        let result = parsed.to_string().unwrap();

        assert!(result.contains(r#""escaped_quotes":"He said \"Hello\"""#));
        assert!(result.contains(r#""backslashes":"C:\\Windows\\System32""#));
        assert!(result.contains(r#""control_chars":"\b\f\n\r\t""#));
        assert!(result.contains(r#""unicode":"\u00A9 2023""#));
    }
}
