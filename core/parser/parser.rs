use crate::parser::ast::{Cmd, Name, Stmt, TransactionType};
use crate::parser::error::Error;
use crate::parser::lexer::{Lexer, Token};
use crate::parser::token::TokenType;

fn from_bytes(bytes: &[u8]) -> String {
    unsafe { str::from_utf8_unchecked(bytes).to_owned() }
}

pub struct Parser<'a> {
    lexer: Lexer<'a>,
    /// The current token being processed
    peek_mark: Option<Token<'a>>,
}

impl<'a> Iterator for Parser<'a> {
    type Item = Result<Cmd, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.peek() {
            None => None, // EOF
            Some(Ok(token)) => match token.token_type {
                Some(TokenType::TK_EXPLAIN) => {
                    self.eat_assert(TokenType::TK_EXPLAIN);

                    let mut is_query_plan = false;
                    match self.peek_no_eof() {
                        Ok(tok) if tok.token_type == Some(TokenType::TK_QUERY) => {
                            self.eat_assert(TokenType::TK_QUERY);

                            if let Err(err) = self.eat_expect(&[TokenType::TK_PLAN]) {
                                return Some(Err(err));
                            }

                            is_query_plan = true;
                        }
                        Err(err) => return Some(Err(err)),
                        _ => {}
                    }

                    let stmt = self.parse_stmt();
                    if let Err(err) = stmt {
                        return Some(Err(err));
                    }

                    if is_query_plan {
                        return Some(Ok(Cmd::ExplainQueryPlan(stmt.unwrap())));
                    }

                    Some(Ok(Cmd::Explain(stmt.unwrap())))
                }
                _ => {
                    let stmt = self.parse_stmt();
                    if let Err(err) = stmt {
                        return Some(Err(err));
                    }

                    Some(Ok(Cmd::Stmt(stmt.unwrap())))
                }
            },
            Some(Err(err)) => Some(Err(err)),
        }
    }
}

impl<'a> Parser<'a> {
    #[inline(always)]
    pub fn new(input: &'a [u8]) -> Self {
        Self {
            lexer: Lexer::new(input),
            peek_mark: None,
        }
    }

    /// Get the next token from the lexer
    #[inline(always)]
    fn eat(&mut self) -> Option<Result<Token<'a>, Error>> {
        if let Some(token) = self.peek_mark.take() {
            return Some(Ok(token));
        }

        loop {
            let tok = self.lexer.next();
            if let Some(Ok(ref token)) = tok {
                if token.token_type.is_none() {
                    continue; // white space or comment
                }
            }

            return tok;
        }
    }

    #[inline(always)]
    fn eat_no_eof(&mut self) -> Result<Token<'a>, Error> {
        match self.eat() {
            None => Err(Error::ParseUnexpectedEOF),
            Some(Ok(token)) => Ok(token),
            Some(Err(err)) => Err(err),
        }
    }

    #[inline(always)]
    fn eat_expect(&mut self, expected: &'static [TokenType]) -> Result<Token<'a>, Error> {
        let token = self.eat_no_eof()?;
        for expected in expected {
            if token.token_type == Some(*expected) {
                return Ok(token);
            }
        }

        Err(Error::ParseUnexpectedToken {
            expected: expected,
            got: token.token_type.unwrap(), // no whitespace or comment tokens here
        })
    }

    #[inline(always)]
    fn eat_assert(&mut self, expected: TokenType) {
        let token = self.eat_no_eof().unwrap();
        debug_assert_eq!(
            token.token_type,
            Some(expected),
            "Expected token {:?}, got {:?}",
            expected,
            token.token_type
        );
    }

    /// Peek at the next token without consuming it
    #[inline(always)]
    fn peek(&mut self) -> Option<Result<Token<'a>, Error>> {
        if let Some(ref token) = self.peek_mark {
            return Some(Ok(token.clone()));
        }

        match self.eat() {
            None => None, // EOF
            Some(Ok(token)) => {
                self.peek_mark = Some(token.clone());
                Some(Ok(token))
            }
            Some(Err(err)) => Some(Err(err)),
        }
    }

    #[inline(always)]
    fn peek_no_eof(&mut self) -> Result<Token<'a>, Error> {
        match self.peek() {
            None => Err(Error::ParseUnexpectedEOF),
            Some(Ok(token)) => Ok(token),
            Some(Err(err)) => Err(err),
        }
    }

    #[inline(always)]
    fn peek_ignore_eof(&mut self) -> Result<Option<Token<'a>>, Error> {
        match self.peek() {
            None => Ok(None),
            Some(Ok(token)) => Ok(Some(token)),
            Some(Err(err)) => Err(err),
        }
    }

    #[inline(always)]
    fn peek_expect(&mut self, expected: &'static [TokenType]) -> Result<Token<'a>, Error> {
        let token = self.peek_no_eof()?;
        for expected in expected {
            if token.token_type == Some(*expected) {
                return Ok(token);
            }
        }

        Err(Error::ParseUnexpectedToken {
            expected: expected,
            got: token.token_type.unwrap(), // no whitespace or comment tokens here
        })
    }

    #[inline(always)]
    fn parse_stmt(&mut self) -> Result<Stmt, Error> {
        // consumes prefix SEMI
        while let Ok(token) = self.peek_no_eof() {
            if token.token_type == Some(TokenType::TK_SEMI) {
                self.eat_assert(TokenType::TK_SEMI);
            } else {
                break;
            }
        }

        let tok = self.peek_expect(&[
            TokenType::TK_BEGIN,
            // add more
        ])?;

        let stmt = match tok.token_type.unwrap() {
            TokenType::TK_BEGIN => self.parse_begin()?,
            _ => unreachable!(),
        };

        // consumes suffix SEMI
        let mut found_semi = false;
        loop {
            match self.peek_ignore_eof()? {
                None => break,
                Some(token) if token.token_type == Some(TokenType::TK_SEMI) => {
                    found_semi = true;
                    self.eat_expect(&[TokenType::TK_SEMI]).unwrap();
                }
                Some(token) => {
                    if !found_semi {
                        return Err(Error::ParseUnexpectedToken {
                            expected: &[TokenType::TK_SEMI],
                            got: token.token_type.unwrap(),
                        });
                    }

                    break;
                }
            }
        }

        return Ok(stmt);
    }

    #[inline(always)]
    fn peek_nm(&mut self) -> Result<Token<'a>, Error> {
        self.peek_expect(&[
            TokenType::TK_ID,
            TokenType::TK_STRING,
            TokenType::TK_INDEXED,
            TokenType::TK_JOIN_KW,
        ])
    }

    #[inline(always)]
    fn parse_nm(&mut self) -> Result<Name, Error> {
        let tok = self.eat_expect(&[
            TokenType::TK_ID,
            TokenType::TK_STRING,
            TokenType::TK_INDEXED,
            TokenType::TK_JOIN_KW,
        ])?;

        let first_char = tok.value[0];
        match first_char {
            b'[' | b'\'' | b'`' | b'"' => {
                let value = &tok.value[1..tok.value.len() - 1];
                Ok(Name::Quoted(from_bytes(value)))
            }
            _ => Ok(Name::Ident(from_bytes(tok.value))),
        }
    }

    #[inline(always)]
    fn parse_begin(&mut self) -> Result<Stmt, Error> {
        self.eat_assert(TokenType::TK_BEGIN);

        let transtype = match self.peek_ignore_eof()? {
            None => None,
            Some(tok) => match tok.token_type.unwrap() {
                TokenType::TK_DEFERRED => {
                    self.eat_assert(TokenType::TK_DEFERRED);
                    Some(TransactionType::Deferred)
                }
                TokenType::TK_IMMEDIATE => {
                    self.eat_assert(TokenType::TK_IMMEDIATE);
                    Some(TransactionType::Immediate)
                }
                TokenType::TK_EXCLUSIVE => {
                    self.eat_assert(TokenType::TK_EXCLUSIVE);
                    Some(TransactionType::Exclusive)
                }
                _ => None,
            },
        };

        let trans_opt = match self.peek_ignore_eof()? {
            None => None,
            Some(tok) => match tok.token_type.unwrap() {
                TokenType::TK_TRANSACTION => {
                    self.eat_assert(TokenType::TK_TRANSACTION);
                    if self.peek_nm().ok().is_some() {
                        self.parse_nm().ok()
                    } else {
                        None
                    }
                }
                _ => None,
            },
        };

        Ok(Stmt::Begin {
            typ: transtype,
            name: trans_opt,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parser() {
        let test_cases = vec![
            // begin
            (
                b"BEGIN".as_slice(),
                vec![Cmd::Stmt(Stmt::Begin {
                    typ: None,
                    name: None,
                })],
            ),
            (
                b"EXPLAIN BEGIN".as_slice(),
                vec![Cmd::Explain(Stmt::Begin {
                    typ: None,
                    name: None,
                })],
            ),
            (
                b"EXPLAIN QUERY PLAN BEGIN".as_slice(),
                vec![Cmd::ExplainQueryPlan(Stmt::Begin {
                    typ: None,
                    name: None,
                })],
            ),
            (
                b"BEGIN TRANSACTION".as_slice(),
                vec![Cmd::Stmt(Stmt::Begin {
                    typ: None,
                    name: None,
                })],
            ),
            (
                b"BEGIN DEFERRED TRANSACTION".as_slice(),
                vec![Cmd::Stmt(Stmt::Begin {
                    typ: Some(TransactionType::Deferred),
                    name: None,
                })],
            ),
            (
                b"BEGIN IMMEDIATE TRANSACTION".as_slice(),
                vec![Cmd::Stmt(Stmt::Begin {
                    typ: Some(TransactionType::Immediate),
                    name: None,
                })],
            ),
            (
                b"BEGIN EXCLUSIVE TRANSACTION".as_slice(),
                vec![Cmd::Stmt(Stmt::Begin {
                    typ: Some(TransactionType::Exclusive),
                    name: None,
                })],
            ),
            (
                b"BEGIN DEFERRED TRANSACTION my_transaction".as_slice(),
                vec![Cmd::Stmt(Stmt::Begin {
                    typ: Some(TransactionType::Deferred),
                    name: Some(Name::Ident("my_transaction".to_string())),
                })],
            ),
            (
                b"BEGIN IMMEDIATE TRANSACTION my_transaction".as_slice(),
                vec![Cmd::Stmt(Stmt::Begin {
                    typ: Some(TransactionType::Immediate),
                    name: Some(Name::Ident("my_transaction".to_string())),
                })],
            ),
            (
                b"BEGIN EXCLUSIVE TRANSACTION my_transaction".as_slice(),
                vec![Cmd::Stmt(Stmt::Begin {
                    typ: Some(TransactionType::Exclusive),
                    name: Some(Name::Ident("my_transaction".to_string())),
                })],
            ),
            (
                b"BEGIN EXCLUSIVE TRANSACTION 'my_transaction'".as_slice(),
                vec![Cmd::Stmt(Stmt::Begin {
                    typ: Some(TransactionType::Exclusive),
                    name: Some(Name::Quoted("my_transaction".to_string())),
                })],
            ),
            (
                ";;;BEGIN;BEGIN;;;;;;BEGIN".as_bytes(),
                vec![
                    Cmd::Stmt(Stmt::Begin {
                        typ: None,
                        name: None,
                    }),
                    Cmd::Stmt(Stmt::Begin {
                        typ: None,
                        name: None,
                    }),
                    Cmd::Stmt(Stmt::Begin {
                        typ: None,
                        name: None,
                    }),
                ],
            ),
            // commit
        ];

        for (input, expected) in test_cases {
            let mut parser = Parser::new(input);
            let mut results = Vec::new();
            while let Some(cmd) = parser.next() {
                match cmd {
                    Ok(cmd) => results.push(cmd),
                    Err(err) => panic!("Parse error: {}", err),
                }
            }

            assert_eq!(results, expected, "Input: {:?}", input);
        }
    }
}
