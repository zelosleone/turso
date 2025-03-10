use crate::{bail_parse_error, LimboError, Result};
use std::{
    iter::Peekable,
    slice::Iter,
    str::{from_utf8, Chars},
};

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
                // Get the last 4 bits for header_size
                let header_size = header_byte >> 4;
                let mut offset = 0;
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

    fn into_bytes(&self) -> [u8; 5] {
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
    pub fn new(capacity: usize) -> Self {
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

    pub fn debug_read(&self) {
        let mut cursor = 0usize;
        while cursor < self.len() {
            let (header, offset) = self.read_header(cursor).unwrap();
            cursor = cursor + offset;
            println!("{:?}: HEADER", header);
            if header.0 == ElementType::OBJECT || header.0 == ElementType::ARRAY {
                cursor = cursor;
            } else {
                let value = from_utf8(&self.data[cursor..cursor + header.1]).unwrap();
                println!("{:?}: VALUE", value);
                cursor = cursor + header.1
            }
        }
    }

    pub fn to_string(&self) -> String {
        let mut result = String::with_capacity(self.data.len() * 2);
        self.write_to_string(&mut result);

        result
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

            JsonbHeader(ElementType::TRUE, _) | JsonbHeader(ElementType::FALSE, _) => {
                self.serialize_boolean(string, cursor)?
            }
            JsonbHeader(ElementType::NULL, _) => self.serialize_null(string, cursor)?,
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
            string.push('"');
            match element_type {
                ElementType::TEXT
                | ElementType::TEXTRAW
                | ElementType::TEXTJ
                | ElementType::TEXT5 => {
                    current_cursor =
                        self.serialize_string(string, current_cursor, len, &element_type)?;
                }
                _ => bail_parse_error!("Malformed json!"),
            }
            string.push('"');
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

        while end_cursor > current_cursor {
            current_cursor = self.serialize_value(string, cursor)?;
            if end_cursor > current_cursor {
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
        todo!()
    }

    fn serialize_number(
        &self,
        string: &mut String,
        cursor: usize,
        len: usize,
        kind: &ElementType,
    ) -> Result<usize> {
        todo!()
    }

    fn serialize_boolean(&self, string: &mut String, cursor: usize) -> Result<usize> {
        todo!()
    }

    fn serialize_null(&self, string: &mut String, cursor: usize) -> Result<usize> {
        todo!()
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
            Some(b'n') => self.deserialize_null(input),
            Some(b'"') => self.deserialize_string(input),
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
            None => bail_parse_error!("Unexpected end of input"),
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
                    if input.peek() != Some(&&b'"') {
                        bail_parse_error!("Object key must be a string");
                    }
                    self.deserialize_string(input)?;

                    skip_whitespace(input);

                    // Expect and consume ':'
                    if input.next() != Some(&&b':') {
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
        let mut len = 0;
        self.write_element_header(string_start, ElementType::TEXT, 0)?;
        let payload_start = self.len();

        if input.peek().is_none() {
            bail_parse_error!("Unexpected end of input");
        };
        // Determine if this will be TEXT, TEXTJ, or TEXT5
        let mut element_type = ElementType::TEXT;

        while let Some(c) = input.next() {
            if c == quote {
                break;
            } else if c == &b'\\' {
                // Handle escapes
                if let Some(&esc) = input.next() {
                    match esc {
                        b'b' => {
                            self.data.push('\u{0008}' as u8);
                            len += 1;
                            element_type = ElementType::TEXTJ;
                        }
                        b'f' => {
                            self.data.push('\u{000C}' as u8);
                            len += 1;
                            element_type = ElementType::TEXTJ;
                        }
                        b'n' => {
                            self.data.push('\n' as u8);
                            len += 1;
                            element_type = ElementType::TEXTJ;
                        }
                        b'r' => {
                            self.data.push('\r' as u8);
                            len += 1;
                            element_type = ElementType::TEXTJ;
                        }
                        b't' => {
                            self.data.push('\t' as u8);
                            len += 1;
                            element_type = ElementType::TEXTJ;
                        }
                        b'\\' | b'"' | b'/' => {
                            self.data.push(esc);
                            len += 1;
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
                            self.data.push(b'\n');
                            len += 1;
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
                        }
                        _ => {
                            bail_parse_error!("Invalid escape sequence")
                        }
                    }
                } else {
                    bail_parse_error!("Unexpected end of input in escape sequence");
                }
            } else if c <= &('\u{001F}' as u8) {
                // Control characters need escaping in standard JSON
                element_type = ElementType::TEXT5;
                self.data.push(*c);
                len += 1;
            } else {
                self.data.push(*c);
                len += 1;
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
        self.write_element_header(num_start, ElementType::INT, 0)?;

        // Handle sign
        if input.peek() == Some(&&b'-') || input.peek() == Some(&&b'+') {
            if input.peek() == Some(&&b'+') {
                is_json5 = true; // JSON5 extension
            }
            self.data.push(*input.next().unwrap());
            len += 1;
        }

        // Handle json5 float number
        if input.peek() == Some(&&b'.') {
            is_json5 = true;
        };

        // Check for hex (JSON5)
        if input.peek() == Some(&&b'0') {
            self.data.push(*input.next().unwrap());
            len += 1;
            if input.peek() == Some(&&b'x') || input.peek() == Some(&&b'X') {
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

                // Write INT5 header and payload
                self.write_element_header(num_start, ElementType::INT5, len)?;

                return Ok(self.len() - num_start);
            }
        }

        // Check for Infinity
        if input.peek().map(|x| x.to_ascii_lowercase()) == Some(b'i') {
            for expected in &[b'i', b'n', b'f', b'i', b'n', b'i', b't', b'y'] {
                if input.next().map(|x| x.to_ascii_lowercase()) != Some(*expected) {
                    bail_parse_error!("Failed to parse number");
                }
            }
            self.write_element_header(
                num_start,
                ElementType::INT5,
                len + INFINITY_CHAR_COUNT as usize,
            )?;
            for byte in [b'9', b'e', b'9', b'9', b'9'].into_iter() {
                self.data.push(byte)
            }

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

    pub fn deserialize_null<'a, I>(&mut self, input: &mut Peekable<I>) -> Result<usize>
    where
        I: Iterator<Item = &'a u8>,
    {
        let start = self.len();
        // Expect "null"
        for expected in &[b'n', b'u', b'l', b'l'] {
            if input.next() != Some(expected) {
                bail_parse_error!("Expected 'null'");
            }
        }
        self.data.push(ElementType::NULL as u8);
        Ok(self.len() - start)
    }

    pub fn deserialize_true<'a, I>(&mut self, input: &mut Peekable<I>) -> Result<usize>
    where
        I: Iterator<Item = &'a u8>,
    {
        let start = self.len();
        // Expect "true"
        for expected in &[b't', b'r', b'u', b'e'] {
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
        // Expect "false"
        for expected in &[b'f', b'a', b'l', b's', b'e'] {
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

    pub fn from_str(input: &str) -> Result<Self> {
        let mut result = Self::new(input.len());
        let mut input_iter = input.as_bytes().iter().peekable();

        result.deserialize_value(&mut input_iter, 0)?;

        Ok(result)
    }

    pub fn from_bytes(input: &[u8]) -> Result<Self> {
        let mut result = Self::new(input.len());
        let mut input_iter = input.iter().peekable();
        result.deserialize_value(&mut input_iter, 0)?;

        Ok(result)
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
    match b {
        b'0'..=b'9' | b'a'..=b'f' | b'A'..=b'F' => true,
        _ => false,
    }
}
