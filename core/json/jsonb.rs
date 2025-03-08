use crate::{bail_parse_error, LimboError, Result};
use std::{
    iter::Peekable,
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
        from_utf8(&self.data).unwrap().to_owned()
    }

    fn deserialize_value(
        &mut self,
        input: &mut Peekable<Chars<'_>>,
        depth: usize,
    ) -> Result<usize> {
        if depth > MAX_JSON_DEPTH {
            bail_parse_error!("Too deep")
        };
        let current_depth = depth + 1;
        skip_whitespace(input);
        match input.peek() {
            Some('{') => {
                input.next(); // consume '{'
                self.deserialize_obj(input, current_depth)
            }
            Some('[') => {
                input.next(); // consume '['
                self.deserialize_array(input, current_depth)
            }
            Some('t') => self.deserialize_true(input),
            Some('f') => self.deserialize_false(input),
            Some('n') => self.deserialize_null(input),
            Some('"') => self.deserialize_string(input),
            Some(c)
                if c.is_ascii_digit()
                    || *c == '-'
                    || *c == '+'
                    || *c == '.'
                    || c.to_ascii_lowercase() == 'i' =>
            {
                self.deserialize_number(input)
            }
            Some(ch) => bail_parse_error!("Unexpected character: {}", ch),
            None => bail_parse_error!("Unexpected end of input"),
        }
    }

    pub fn deserialize_obj(
        &mut self,
        input: &mut Peekable<Chars<'_>>,
        depth: usize,
    ) -> Result<usize> {
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
                Some('}') => {
                    input.next(); // consume '}'
                    if first {
                        return Ok(1); // empty header
                    } else {
                        let obj_size = self.len() - obj_start;
                        self.write_element_header(header_pos, ElementType::OBJECT, obj_size)?;
                        return Ok(obj_size + 2);
                    }
                }
                Some(',') if !first => {
                    input.next(); // consume ','
                    skip_whitespace(input);
                }
                Some(_) => {
                    // Parse key (must be string)
                    if input.peek() != Some(&'"') {
                        bail_parse_error!("Object key must be a string");
                    }
                    self.deserialize_string(input)?;

                    skip_whitespace(input);

                    // Expect and consume ':'
                    if input.next() != Some(':') {
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

    pub fn deserialize_array(
        &mut self,
        input: &mut Peekable<Chars<'_>>,
        depth: usize,
    ) -> Result<usize> {
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
                Some(']') => {
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
                Some(',') if !first => {
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

    pub fn deserialize_string(&mut self, input: &mut Peekable<Chars<'_>>) -> Result<usize> {
        let string_start = self.len();
        let quote = input.next().unwrap(); // "

        if input.peek().is_none() {
            bail_parse_error!("Unexpected end of input");
        };
        // Determine if this will be TEXT, TEXTJ, or TEXT5
        let mut element_type = ElementType::TEXT;
        let mut content = String::new();

        while let Some(c) = input.next() {
            if c == quote {
                break;
            } else if c == '\\' {
                // Handle escapes
                if let Some(esc) = input.next() {
                    match esc {
                        'b' => {
                            content.push('\u{0008}');
                            element_type = ElementType::TEXTJ;
                        }
                        'f' => {
                            content.push('\u{000C}');
                            element_type = ElementType::TEXTJ;
                        }
                        'n' => {
                            content.push('\n');
                            element_type = ElementType::TEXTJ;
                        }
                        'r' => {
                            content.push('\r');
                            element_type = ElementType::TEXTJ;
                        }
                        't' => {
                            content.push('\t');
                            element_type = ElementType::TEXTJ;
                        }
                        '\\' | '"' | '/' => {
                            content.push(esc);
                            element_type = ElementType::TEXTJ;
                        }
                        'u' => {
                            // Unicode escape
                            element_type = ElementType::TEXTJ;
                            let mut code = 0u32;
                            for _ in 0..4 {
                                if let Some(h) = input.next() {
                                    let h = h.to_digit(16);
                                    match h {
                                        Some(digit) => {
                                            code = code * 16 + digit;
                                        }
                                        None => bail_parse_error!("Failed to parse u16"),
                                    }
                                } else {
                                    bail_parse_error!("Incomplete Unicode escape");
                                }
                            }
                            match char::from_u32(code) {
                                Some(ch) => content.push(ch),
                                None => bail_parse_error!("Invalid unicode escape!"),
                            };
                        }
                        // JSON5 extensions
                        '\n' => {
                            element_type = ElementType::TEXT5;
                            content.push('\n');
                        }
                        '\'' | '0' | 'v' | 'x' => {
                            element_type = ElementType::TEXT5;
                            // Appropriate handling for each case
                        }
                        _ => bail_parse_error!("Invalid escape sequence: \\{}", esc),
                    }
                } else {
                    bail_parse_error!("Unexpected end of input in escape sequence");
                }
            } else if c <= '\u{001F}' {
                // Control characters need escaping in standard JSON
                element_type = ElementType::TEXT5;
                content.push(c);
            } else {
                content.push(c);
            }
        }

        // Write header and payload
        self.write_element_header(self.len(), element_type, content.len())?;
        for byte in content.bytes() {
            self.data.push(byte);
        }

        Ok(self.len() - string_start)
    }

    pub fn deserialize_number(&mut self, input: &mut Peekable<Chars<'_>>) -> Result<usize> {
        let num_start = self.len();
        let mut num_str = String::new();
        let mut is_float = false;
        let mut is_json5 = false;

        // Handle sign
        if input.peek() == Some(&'-') || input.peek() == Some(&'+') {
            if input.peek() == Some(&'+') {
                is_json5 = true; // JSON5 extension
            }
            num_str.push(input.next().unwrap());
        }

        // Handle json5 float number
        if input.peek() == Some(&'.') {
            is_json5 = true;
        };

        // Check for hex (JSON5)
        if input.peek() == Some(&'0') {
            num_str.push(input.next().unwrap());
            if input.peek() == Some(&'x') || input.peek() == Some(&'X') {
                num_str.push(input.next().unwrap());
                while let Some(&ch) = input.peek() {
                    if ch.is_digit(16) {
                        num_str.push(input.next().unwrap());
                    } else {
                        break;
                    }
                }

                // Write INT5 header and payload
                self.write_element_header(self.len(), ElementType::INT5, num_str.len())?;
                for byte in num_str.bytes() {
                    self.data.push(byte);
                }
                return Ok(self.len() - num_start);
            }
        }

        // Check for Infinity
        if input.peek().map(|x| x.to_ascii_lowercase()) == Some('i') {
            for expected in &['i', 'n', 'f', 'i', 'n', 'i', 't', 'y'] {
                if input.next().map(|x| x.to_ascii_lowercase()) != Some(*expected) {
                    bail_parse_error!("Failed to parse number");
                }
            }
            self.write_element_header(
                self.len(),
                ElementType::INT5,
                num_str.len() + INFINITY_CHAR_COUNT as usize,
            )?;
            for byte in num_str
                .bytes()
                .chain([b'9', b'e', b'9', b'9', b'9'].into_iter())
            {
                self.data.push(byte)
            }

            return Ok(self.len() - num_start);
        };

        // Regular number parsing
        while let Some(&ch) = input.peek() {
            match ch {
                '0'..='9' => {
                    num_str.push(input.next().unwrap());
                }
                '.' => {
                    is_float = true;
                    num_str.push(input.next().unwrap());
                }
                'e' | 'E' => {
                    is_float = true;
                    num_str.push(input.next().unwrap());
                    if input.peek() == Some(&'+') || input.peek() == Some(&'-') {
                        num_str.push(input.next().unwrap());
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

        self.write_element_header(self.len(), element_type, num_str.len())?;
        for byte in num_str.bytes() {
            self.data.push(byte);
        }

        Ok(self.len() - num_start)
    }

    pub fn deserialize_null(&mut self, input: &mut Peekable<Chars<'_>>) -> Result<usize> {
        let start = self.len();
        // Expect "null"
        for expected in &['n', 'u', 'l', 'l'] {
            if input.next() != Some(*expected) {
                bail_parse_error!("Expected 'null'");
            }
        }
        self.data.push(ElementType::NULL as u8);
        Ok(self.len() - start)
    }

    pub fn deserialize_true(&mut self, input: &mut Peekable<Chars<'_>>) -> Result<usize> {
        let start = self.len();
        // Expect "true"
        for expected in &['t', 'r', 'u', 'e'] {
            if input.next() != Some(*expected) {
                bail_parse_error!("Expected 'true'");
            }
        }
        self.data.push(ElementType::TRUE as u8);
        Ok(self.len() - start)
    }

    fn deserialize_false(&mut self, input: &mut Peekable<Chars<'_>>) -> Result<usize> {
        let start = self.len();
        // Expect "false"
        for expected in &['f', 'a', 'l', 's', 'e'] {
            if input.next() != Some(*expected) {
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
        let mut input_iter = input.chars().peekable();

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

pub fn skip_whitespace(input: &mut Peekable<Chars<'_>>) {
    while let Some(&ch) = input.peek() {
        match ch {
            ' ' | '\t' | '\n' | '\r' => {
                input.next();
            }
            '/' => {
                // Handle JSON5 comments
                input.next();
                if let Some(next_ch) = input.peek() {
                    if *next_ch == '/' {
                        // Line comment - skip until newline
                        input.next();
                        while let Some(c) = input.next() {
                            if c == '\n' {
                                break;
                            }
                        }
                    } else if *next_ch == '*' {
                        // Block comment - skip until "*/"
                        input.next();
                        let mut prev = '\0';
                        while let Some(c) = input.next() {
                            if prev == '*' && c == '/' {
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
