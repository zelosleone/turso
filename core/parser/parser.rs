use crate::parser::ast::{
    Cmd, CommonTableExpr, CreateTableBody, Expr, IndexedColumn, LikeOperator, Limit, Literal,
    Materialized, Name, NullsOrder, Operator, QualifiedName, Select, SelectBody, SortOrder,
    SortedColumn, Stmt, TransactionType, Type, TypeSize, UnaryOperator, With,
};
use crate::parser::error::Error;
use crate::parser::lexer::{Lexer, Token};
use crate::parser::token::TokenType;

fn from_bytes_as_str(bytes: &[u8]) -> &str {
    unsafe { str::from_utf8_unchecked(bytes) }
}

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
        // consumes prefix SEMI
        while let Some(Ok(token)) = self.peek() {
            if token.token_type == Some(TokenType::TK_SEMI) {
                self.eat_assert(&[TokenType::TK_SEMI]);
            } else {
                break;
            }
        }

        let result = match self.peek() {
            None => None, // EOF
            Some(Ok(token)) => match token.token_type {
                Some(TokenType::TK_EXPLAIN) => {
                    self.eat_assert(&[TokenType::TK_EXPLAIN]);

                    let mut is_query_plan = false;
                    match self.peek_no_eof() {
                        Ok(tok) if tok.token_type == Some(TokenType::TK_QUERY) => {
                            self.eat_assert(&[TokenType::TK_QUERY]);

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
                        Some(Ok(Cmd::ExplainQueryPlan(stmt.unwrap())))
                    } else {
                        Some(Ok(Cmd::Explain(stmt.unwrap())))
                    }
                }
                _ => {
                    let stmt = self.parse_stmt();
                    if let Err(err) = stmt {
                        return Some(Err(err));
                    }

                    Some(Ok(Cmd::Stmt(stmt.unwrap())))
                }
            },
            Some(Err(err)) => return Some(Err(err)),
        };

        // consumes suffix SEMI
        let mut found_semi = false;
        loop {
            match self.peek_ignore_eof() {
                Ok(None) => break,
                Ok(Some(token)) if token.token_type == Some(TokenType::TK_SEMI) => {
                    found_semi = true;
                    self.eat_assert(&[TokenType::TK_SEMI]);
                }
                Ok(Some(token)) => {
                    if !found_semi {
                        return Some(Err(Error::ParseUnexpectedToken {
                            expected: &[TokenType::TK_SEMI],
                            got: token.token_type.unwrap(),
                        }));
                    }

                    break;
                }
                Err(err) => return Some(Err(err)),
            }
        }

        result
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
        self.peek_expect(expected)?;
        Ok(self.eat_assert(expected))
    }

    #[inline(always)]
    fn eat_assert(&mut self, expected: &'static [TokenType]) -> Token<'a> {
        let token = self.eat_no_eof().unwrap();

        #[cfg(debug_assertions)]
        {
            for expected in expected {
                if token.token_type == Some(*expected) {
                    return token;
                }
            }

            panic!(
                "Expected token {:?}, got {:?}",
                expected,
                token.token_type.unwrap()
            );
        }

        #[cfg(not(debug_assertions))]
        token // in release mode, we assume the caller has checked the token type
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
        let tok = self.peek_expect(&[
            TokenType::TK_BEGIN,
            TokenType::TK_COMMIT,
            TokenType::TK_END,
            TokenType::TK_ROLLBACK,
            TokenType::TK_SAVEPOINT,
            TokenType::TK_RELEASE,
            TokenType::TK_CREATE,
            // add more
        ])?;

        match tok.token_type.unwrap() {
            TokenType::TK_BEGIN => self.parse_begin(),
            TokenType::TK_COMMIT | TokenType::TK_END => self.parse_commit(),
            TokenType::TK_ROLLBACK => self.parse_rollback(),
            TokenType::TK_SAVEPOINT => self.parse_savepoint(),
            TokenType::TK_RELEASE => self.parse_release(),
            TokenType::TK_CREATE => self.parse_create_stmt(),
            _ => unreachable!(),
        }
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
    fn parse_nm(&mut self) -> Name {
        let tok = self.eat_assert(&[
            TokenType::TK_ID,
            TokenType::TK_STRING,
            TokenType::TK_INDEXED,
            TokenType::TK_JOIN_KW,
        ]);

        let first_char = tok.value[0]; // no need to check empty
        match first_char {
            b'[' | b'\'' | b'`' | b'"' => Name::Quoted(from_bytes(tok.value)),
            _ => Name::Ident(from_bytes(tok.value)),
        }
    }

    #[inline(always)]
    fn parse_transopt(&mut self) -> Result<Option<Name>, Error> {
        match self.peek_ignore_eof()? {
            None => Ok(None),
            Some(tok) => match tok.token_type.unwrap() {
                TokenType::TK_TRANSACTION => {
                    self.eat_assert(&[TokenType::TK_TRANSACTION]);
                    if self.peek_nm().ok().is_some() {
                        Ok(Some(self.parse_nm()))
                    } else {
                        Ok(None)
                    }
                }
                _ => Ok(None),
            },
        }
    }

    #[inline(always)]
    fn parse_begin(&mut self) -> Result<Stmt, Error> {
        self.eat_assert(&[TokenType::TK_BEGIN]);

        let transtype = match self.peek_ignore_eof()? {
            None => None,
            Some(tok) => match tok.token_type.unwrap() {
                TokenType::TK_DEFERRED => {
                    self.eat_assert(&[TokenType::TK_DEFERRED]);
                    Some(TransactionType::Deferred)
                }
                TokenType::TK_IMMEDIATE => {
                    self.eat_assert(&[TokenType::TK_IMMEDIATE]);
                    Some(TransactionType::Immediate)
                }
                TokenType::TK_EXCLUSIVE => {
                    self.eat_assert(&[TokenType::TK_EXCLUSIVE]);
                    Some(TransactionType::Exclusive)
                }
                _ => None,
            },
        };

        Ok(Stmt::Begin {
            typ: transtype,
            name: self.parse_transopt()?,
        })
    }

    #[inline(always)]
    fn parse_commit(&mut self) -> Result<Stmt, Error> {
        self.eat_assert(&[TokenType::TK_COMMIT, TokenType::TK_END]);
        Ok(Stmt::Commit {
            name: self.parse_transopt()?,
        })
    }

    #[inline(always)]
    fn parse_rollback(&mut self) -> Result<Stmt, Error> {
        self.eat_assert(&[TokenType::TK_ROLLBACK]);

        let tx_name = self.parse_transopt()?;

        let savepoint_name = match self.peek_ignore_eof()? {
            None => None,
            Some(tok) => {
                if tok.token_type == Some(TokenType::TK_TO) {
                    self.eat_assert(&[TokenType::TK_TO]);

                    if self.peek_no_eof()?.token_type == Some(TokenType::TK_SAVEPOINT) {
                        self.eat_assert(&[TokenType::TK_SAVEPOINT]);
                    }

                    self.peek_nm()?;
                    Some(self.parse_nm())
                } else {
                    None
                }
            }
        };

        Ok(Stmt::Rollback {
            tx_name,
            savepoint_name,
        })
    }

    #[inline(always)]
    fn parse_savepoint(&mut self) -> Result<Stmt, Error> {
        self.eat_assert(&[TokenType::TK_SAVEPOINT]);
        self.peek_nm()?;
        Ok(Stmt::Savepoint {
            name: self.parse_nm(),
        })
    }

    #[inline(always)]
    fn parse_release(&mut self) -> Result<Stmt, Error> {
        self.eat_assert(&[TokenType::TK_RELEASE]);

        if self.peek_no_eof()?.token_type == Some(TokenType::TK_SAVEPOINT) {
            self.eat_assert(&[TokenType::TK_SAVEPOINT]);
        }

        self.peek_nm()?;
        Ok(Stmt::Release {
            name: self.parse_nm(),
        })
    }

    #[inline(always)]
    fn parse_create_stmt(&mut self) -> Result<Stmt, Error> {
        self.eat_assert(&[TokenType::TK_CREATE]);
        let first_tok = self.peek_expect(&[
            TokenType::TK_TEMP,
            TokenType::TK_TABLE,
            TokenType::TK_VIRTUAL,
            TokenType::TK_VIEW,
            TokenType::TK_INDEX,
            TokenType::TK_UNIQUE,
            TokenType::TK_TRIGGER,
        ])?;

        match first_tok.token_type.unwrap() {
            TokenType::TK_TABLE => self.parse_create_table(false),
            TokenType::TK_TEMP => {
                self.eat_assert(&[TokenType::TK_TEMP]);
                let first_tok = self.peek_expect(&[
                    TokenType::TK_TABLE,
                    TokenType::TK_VIEW,
                    TokenType::TK_TRIGGER,
                ])?;

                match first_tok.token_type.unwrap() {
                    TokenType::TK_TABLE => self.parse_create_table(true),
                    _ => unreachable!(),
                }
            }
            _ => unreachable!(),
        }
    }

    #[inline(always)]
    fn parse_if_not_exists(&mut self) -> Result<bool, Error> {
        if let Some(tok) = self.peek_ignore_eof()? {
            if tok.token_type == Some(TokenType::TK_IF) {
                self.eat_assert(&[TokenType::TK_IF]);
            } else {
                return Ok(false);
            }
        } else {
            return Ok(false);
        }

        self.eat_expect(&[TokenType::TK_NOT])?;
        self.eat_expect(&[TokenType::TK_EXISTS])?;
        Ok(true)
    }

    #[inline(always)]
    fn parse_fullname(&mut self, allow_alias: bool) -> Result<QualifiedName, Error> {
        self.peek_nm()?;
        let first_name = self.parse_nm();

        let secone_name = if let Some(tok) = self.peek_ignore_eof()? {
            if tok.token_type == Some(TokenType::TK_DOT) {
                self.eat_assert(&[TokenType::TK_DOT]);
                self.peek_nm()?;
                Some(self.parse_nm())
            } else {
                None
            }
        } else {
            None
        };

        let alias_name = if allow_alias {
            if let Some(tok) = self.peek_ignore_eof()? {
                if tok.token_type == Some(TokenType::TK_AS) {
                    self.eat_assert(&[TokenType::TK_AS]);
                    self.peek_nm()?;
                    Some(self.parse_nm())
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };

        if secone_name.is_some() {
            Ok(QualifiedName {
                db_name: Some(first_name),
                name: secone_name.unwrap(),
                alias: alias_name,
            })
        } else {
            Ok(QualifiedName {
                db_name: None,
                name: first_name,
                alias: alias_name,
            })
        }
    }

    #[inline(always)]
    fn parse_signed(&mut self) -> Result<Box<Expr>, Error> {
        self.peek_expect(&[
            TokenType::TK_FLOAT,
            TokenType::TK_INTEGER,
            TokenType::TK_PLUS,
            TokenType::TK_MINUS,
        ])?;

        let expr = self.parse_expr_operand()?;
        match expr.as_ref() {
            Expr::Unary(_, inner) => match inner.as_ref() {
                Expr::Literal(Literal::Numeric(_)) => Ok(expr),
                _ => Err(Error::Custom(
                    "Expected a numeric literal after unary operator".to_string(),
                )),
            },
            _ => Ok(expr),
        }
    }

    #[inline(always)]
    fn parse_type(&mut self) -> Result<Option<Type>, Error> {
        let mut type_name = if let Some(tok) = self.peek_ignore_eof()? {
            match tok.token_type.unwrap() {
                TokenType::TK_ID | TokenType::TK_STRING => {
                    self.eat_assert(&[TokenType::TK_ID, TokenType::TK_STRING]);
                    from_bytes(tok.value)
                }
                _ => return Ok(None),
            }
        } else {
            return Ok(None);
        };

        loop {
            if let Some(tok) = self.peek_ignore_eof()? {
                match tok.token_type.unwrap() {
                    TokenType::TK_ID | TokenType::TK_STRING => {
                        self.eat_assert(&[TokenType::TK_ID, TokenType::TK_STRING]);
                        type_name.push_str(" ");
                        type_name.push_str(from_bytes_as_str(tok.value));
                    }
                    _ => break,
                }
            } else {
                break;
            }
        }

        let size = if let Some(tok) = self.peek_ignore_eof()? {
            if tok.token_type == Some(TokenType::TK_LP) {
                self.eat_assert(&[TokenType::TK_LP]);
                let first_size = self.parse_signed()?;
                let tok = self.eat_expect(&[TokenType::TK_RP, TokenType::TK_COMMA])?;
                match tok.token_type.unwrap() {
                    TokenType::TK_RP => Some(TypeSize::MaxSize(first_size)),
                    TokenType::TK_COMMA => {
                        let second_size = self.parse_signed()?;
                        self.eat_expect(&[TokenType::TK_RP])?;
                        Some(TypeSize::TypeSize(first_size, second_size))
                    }
                    _ => unreachable!(),
                }
            } else {
                None
            }
        } else {
            None
        };

        Ok(Some(Type {
            name: type_name,
            size,
        }))
    }

    /// SQLite understands these operators, listed in precedence1 order
    /// (top to bottom / highest to lowest):
    ///
    /// Operators 2
    /// 11 -> ~ [expr]    + [expr]    - [expr]
    /// 10 -> [expr] COLLATE (collation-name) 3
    /// 9 -> ||   ->   ->>
    /// 8 -> *   /   %
    /// 7 -> +   -
    /// 6 -> &  |   <<  >>
    /// 5 -> [expr] ESCAPE [escape-character-expr] 4
    /// 4 -> <  >  <=  >=
    /// 3 -> =  ==  <>  !=  IS   IS NOT
    ///     IS DISTINCT FROM   IS NOT DISTINCT FROM
    ///     [expr] BETWEEN5 [expr] AND [expr]
    ///     IN5  MATCH5  LIKE5  REGEXP5  GLOB5
    ///     [expr] ISNULL  [expr] NOTNULL   [expr] NOT NULL
    /// 2   NOT [expr]
    /// 1 -> AND
    /// 0 -> OR
    ///
    /// this function detect precedence by peeking first token of operator
    /// after parsing a operand (binary operator)
    #[inline(always)]
    fn current_token_precedence(&mut self) -> Result<Option<u8>, Error> {
        let tok = self.peek_ignore_eof()?;
        if tok.is_none() {
            return Ok(None);
        }

        match tok.unwrap().token_type.unwrap() {
            TokenType::TK_OR => Ok(Some(0)),
            TokenType::TK_AND => Ok(Some(1)),
            TokenType::TK_NOT => Ok(Some(3)), // NOT is 3 because of binary operator
            TokenType::TK_EQ
            | TokenType::TK_NE
            | TokenType::TK_IS
            | TokenType::TK_BETWEEN
            | TokenType::TK_IN
            | TokenType::TK_MATCH
            | TokenType::TK_LIKE_KW
            | TokenType::TK_ISNULL
            | TokenType::TK_NOTNULL => Ok(Some(3)),
            TokenType::TK_LT | TokenType::TK_GT | TokenType::TK_LE | TokenType::TK_GE => {
                Ok(Some(4))
            }
            TokenType::TK_ESCAPE => Ok(Some(5)),
            TokenType::TK_BITAND
            | TokenType::TK_BITOR
            | TokenType::TK_LSHIFT
            | TokenType::TK_RSHIFT => Ok(Some(6)),
            TokenType::TK_PLUS | TokenType::TK_MINUS => Ok(Some(7)),
            TokenType::TK_STAR | TokenType::TK_SLASH | TokenType::TK_REM => Ok(Some(8)),
            TokenType::TK_CONCAT | TokenType::TK_PTR => Ok(Some(9)),
            TokenType::TK_COLLATE => Ok(Some(10)),
            // no need 11 because its for unary operators
            _ => Ok(None),
        }
    }

    #[inline(always)]
    fn parse_expr_operand(&mut self) -> Result<Box<Expr>, Error> {
        let tok = self.peek_expect(&[
            TokenType::TK_LP,
            TokenType::TK_ID,
            TokenType::TK_STRING,
            TokenType::TK_INDEXED,
            TokenType::TK_JOIN_KW,
            TokenType::TK_NULL,
            TokenType::TK_BLOB,
            TokenType::TK_FLOAT,
            TokenType::TK_INTEGER,
            TokenType::TK_VARIABLE,
            TokenType::TK_CAST,
            TokenType::TK_CTIME_KW,
            TokenType::TK_NOT,
            TokenType::TK_BITNOT,
            TokenType::TK_PLUS,
            TokenType::TK_MINUS,
            TokenType::TK_EXISTS,
            TokenType::TK_CASE,
        ])?;

        match tok.token_type.unwrap() {
            TokenType::TK_LP => {
                self.eat_assert(&[TokenType::TK_LP]);
                let exprs = self.parse_expr_list()?;
                self.eat_expect(&[TokenType::TK_RP])?;
                Ok(Box::new(Expr::Parenthesized(exprs)))
            }
            TokenType::TK_ID
            | TokenType::TK_STRING
            | TokenType::TK_INDEXED
            | TokenType::TK_JOIN_KW => {
                let can_be_lit_str = tok.token_type == Some(TokenType::TK_STRING);
                debug_assert!(self.peek_nm().is_ok(), "Expected a name token");
                let name = self.parse_nm();

                let second_name = if let Some(tok) = self.peek_ignore_eof()? {
                    if tok.token_type == Some(TokenType::TK_DOT) {
                        self.eat_assert(&[TokenType::TK_DOT]);
                        self.peek_nm()?;
                        Some(self.parse_nm())
                    } else {
                        None
                    }
                } else {
                    None
                };

                let third_name = if let Some(tok) = self.peek_ignore_eof()? {
                    if tok.token_type == Some(TokenType::TK_DOT) {
                        self.eat_assert(&[TokenType::TK_DOT]);
                        self.peek_nm()?;
                        Some(self.parse_nm())
                    } else {
                        None
                    }
                } else {
                    None
                };

                if second_name.is_some() && third_name.is_some() {
                    Ok(Box::new(Expr::DoublyQualified(
                        name,
                        second_name.unwrap(),
                        third_name.unwrap(),
                    )))
                } else if second_name.is_some() {
                    Ok(Box::new(Expr::Qualified(name, second_name.unwrap())))
                } else if can_be_lit_str {
                    Ok(Box::new(Expr::Literal(match name {
                        Name::Quoted(s) => Literal::String(s),
                        Name::Ident(s) => Literal::String(s),
                    })))
                } else {
                    Ok(Box::new(Expr::Id(name)))
                }
            }
            TokenType::TK_NULL => {
                self.eat_assert(&[TokenType::TK_NULL]);
                Ok(Box::new(Expr::Literal(Literal::Null)))
            }
            TokenType::TK_BLOB => {
                self.eat_assert(&[TokenType::TK_BLOB]);
                Ok(Box::new(Expr::Literal(Literal::Blob(from_bytes(
                    tok.value,
                )))))
            }
            TokenType::TK_FLOAT => {
                self.eat_assert(&[TokenType::TK_FLOAT]);
                Ok(Box::new(Expr::Literal(Literal::Numeric(from_bytes(
                    tok.value,
                )))))
            }
            TokenType::TK_INTEGER => {
                self.eat_assert(&[TokenType::TK_INTEGER]);
                Ok(Box::new(Expr::Literal(Literal::Numeric(from_bytes(
                    tok.value,
                )))))
            }
            TokenType::TK_VARIABLE => {
                self.eat_assert(&[TokenType::TK_VARIABLE]);
                Ok(Box::new(Expr::Variable(from_bytes(tok.value))))
            }
            TokenType::TK_CAST => {
                self.eat_assert(&[TokenType::TK_CAST]);
                self.eat_expect(&[TokenType::TK_LP])?;
                let expr = self.parse_expr(0)?;
                self.eat_expect(&[TokenType::TK_AS])?;
                let typ = self.parse_type()?;
                self.eat_expect(&[TokenType::TK_RP])?;
                Ok(Box::new(Expr::Cast {
                    expr,
                    type_name: typ,
                }))
            }
            TokenType::TK_CTIME_KW => {
                let tok = self.eat_assert(&[TokenType::TK_CTIME_KW]);
                if b"CURRENT_DATE".eq_ignore_ascii_case(&tok.value) {
                    Ok(Box::new(Expr::Literal(Literal::CurrentDate)))
                } else if b"CURRENT_TIME".eq_ignore_ascii_case(&tok.value) {
                    Ok(Box::new(Expr::Literal(Literal::CurrentTime)))
                } else if b"CURRENT_TIMESTAMP".eq_ignore_ascii_case(&tok.value) {
                    Ok(Box::new(Expr::Literal(Literal::CurrentTimestamp)))
                } else {
                    unreachable!()
                }
            }
            TokenType::TK_NOT => {
                self.eat_assert(&[TokenType::TK_NOT]);
                let expr = self.parse_expr(2)?; // NOT precedence is 2
                Ok(Box::new(Expr::Unary(UnaryOperator::Not, expr)))
            }
            TokenType::TK_BITNOT => {
                self.eat_assert(&[TokenType::TK_BITNOT]);
                let expr = self.parse_expr(11)?; // BITNOT precedence is 11
                Ok(Box::new(Expr::Unary(UnaryOperator::BitwiseNot, expr)))
            }
            TokenType::TK_PLUS => {
                self.eat_assert(&[TokenType::TK_PLUS]);
                let expr = self.parse_expr(11)?; // PLUS precedence is 11
                Ok(Box::new(Expr::Unary(UnaryOperator::Positive, expr)))
            }
            TokenType::TK_MINUS => {
                self.eat_assert(&[TokenType::TK_MINUS]);
                let expr = self.parse_expr(11)?; // MINUS precedence is 11
                Ok(Box::new(Expr::Unary(UnaryOperator::Negative, expr)))
            }
            TokenType::TK_EXISTS => {
                self.eat_assert(&[TokenType::TK_EXISTS]);
                self.eat_expect(&[TokenType::TK_LP])?;
                let select = self.parse_select()?;
                self.eat_expect(&[TokenType::TK_RP])?;
                Ok(Box::new(Expr::Exists(select)))
            }
            TokenType::TK_CASE => {
                self.eat_assert(&[TokenType::TK_CASE]);
                let base = if self.peek_no_eof()?.token_type.unwrap() != TokenType::TK_WHEN {
                    Some(self.parse_expr(0)?)
                } else {
                    None
                };

                let mut when_then_pairs = vec![];
                loop {
                    if let Some(tok) = self.peek_ignore_eof()? {
                        if tok.token_type.unwrap() != TokenType::TK_WHEN {
                            break;
                        }
                    } else {
                        break;
                    }

                    self.eat_assert(&[TokenType::TK_WHEN]);
                    let when = self.parse_expr(0)?;
                    self.eat_expect(&[TokenType::TK_THEN])?;
                    let then = self.parse_expr(0)?;
                    when_then_pairs.push((when, then));
                }

                let else_expr = if let Some(ok) = self.peek_ignore_eof()? {
                    if ok.token_type == Some(TokenType::TK_ELSE) {
                        self.eat_assert(&[TokenType::TK_ELSE]);
                        Some(self.parse_expr(0)?)
                    } else {
                        None
                    }
                } else {
                    None
                };

                Ok(Box::new(Expr::Case {
                    base,
                    when_then_pairs,
                    else_expr,
                }))
            }
            _ => unreachable!(),
        }
    }

    fn parse_expr_list(&mut self) -> Result<Vec<Box<Expr>>, Error> {
        let mut exprs = vec![];
        loop {
            match self.peek_ignore_eof()? {
                Some(tok) => match tok.token_type.unwrap() {
                    TokenType::TK_LP
                    | TokenType::TK_ID
                    | TokenType::TK_STRING
                    | TokenType::TK_INDEXED
                    | TokenType::TK_JOIN_KW
                    | TokenType::TK_NULL
                    | TokenType::TK_BLOB
                    | TokenType::TK_FLOAT
                    | TokenType::TK_INTEGER
                    | TokenType::TK_VARIABLE
                    | TokenType::TK_CAST
                    | TokenType::TK_CTIME_KW
                    | TokenType::TK_NOT
                    | TokenType::TK_BITNOT
                    | TokenType::TK_PLUS
                    | TokenType::TK_MINUS
                    | TokenType::TK_EXISTS
                    | TokenType::TK_CASE => {}
                    _ => break,
                },
                None => break,
            }

            exprs.push(self.parse_expr(0)?);
            match self.peek_no_eof()?.token_type.unwrap() {
                TokenType::TK_COMMA => {
                    self.eat_assert(&[TokenType::TK_COMMA]);
                }
                _ => break,
            }
        }

        Ok(exprs)
    }

    #[inline(always)]
    fn parse_expr(&mut self, precedence: u8) -> Result<Box<Expr>, Error> {
        let mut result = self.parse_expr_operand()?;

        loop {
            let pre = match self.current_token_precedence()? {
                Some(pre) if pre < precedence => break,
                None => break, // no more ops
                Some(pre) => pre,
            };

            let mut tok = self.peek_no_eof()?;
            let mut not = false;
            if tok.token_type.unwrap() == TokenType::TK_NOT {
                self.eat_assert(&[TokenType::TK_NOT]);
                tok = self.peek_expect(&[
                    TokenType::TK_BETWEEN,
                    TokenType::TK_IN,
                    TokenType::TK_MATCH,
                    TokenType::TK_LIKE_KW,
                ])?;
                not = true;
            }

            result = match tok.token_type.unwrap() {
                TokenType::TK_OR => {
                    self.eat_assert(&[TokenType::TK_OR]);
                    Box::new(Expr::Binary(result, Operator::Or, self.parse_expr(pre)?))
                }
                TokenType::TK_AND => {
                    self.eat_assert(&[TokenType::TK_AND]);
                    Box::new(Expr::Binary(result, Operator::And, self.parse_expr(pre)?))
                }
                TokenType::TK_EQ => {
                    self.eat_assert(&[TokenType::TK_EQ]);
                    Box::new(Expr::Binary(
                        result,
                        Operator::Equals,
                        self.parse_expr(pre)?,
                    ))
                }
                TokenType::TK_NE => {
                    self.eat_assert(&[TokenType::TK_NE]);
                    Box::new(Expr::Binary(
                        result,
                        Operator::NotEquals,
                        self.parse_expr(pre)?,
                    ))
                }
                TokenType::TK_BETWEEN => {
                    self.eat_assert(&[TokenType::TK_BETWEEN]);
                    let start = self.parse_expr(pre)?;
                    self.eat_expect(&[TokenType::TK_AND])?;
                    let end = self.parse_expr(pre)?;
                    Box::new(Expr::Between {
                        lhs: result,
                        not,
                        start,
                        end,
                    })
                }
                TokenType::TK_IN => {
                    self.eat_assert(&[TokenType::TK_IN]);
                    let tok = self.peek_no_eof()?;
                    match tok.token_type.unwrap() {
                        TokenType::TK_LP => {
                            self.eat_assert(&[TokenType::TK_LP]);
                            let tok = self.peek_no_eof()?;
                            match tok.token_type.unwrap() {
                                TokenType::TK_SELECT | TokenType::TK_WITH => {
                                    let select = self.parse_select()?;
                                    self.eat_expect(&[TokenType::TK_RP])?;
                                    Box::new(Expr::InSelect {
                                        lhs: result,
                                        not,
                                        rhs: select,
                                    })
                                }
                                _ => {
                                    let exprs = self.parse_expr_list()?;
                                    self.eat_expect(&[TokenType::TK_RP])?;
                                    Box::new(Expr::InList {
                                        lhs: result,
                                        not,
                                        rhs: exprs,
                                    })
                                }
                            }
                        }
                        _ => {
                            let name = self.parse_fullname(false)?;
                            let mut exprs = vec![];
                            if let Some(tok) = self.peek_ignore_eof()? {
                                if tok.token_type == Some(TokenType::TK_LP) {
                                    self.eat_assert(&[TokenType::TK_LP]);
                                    exprs = self.parse_expr_list()?;
                                    self.eat_expect(&[TokenType::TK_RP])?;
                                }
                            }

                            Box::new(Expr::InTable {
                                lhs: result,
                                not,
                                rhs: name,
                                args: exprs,
                            })
                        }
                    }
                }
                TokenType::TK_MATCH | TokenType::TK_LIKE_KW => {
                    let tok = self.eat_assert(&[TokenType::TK_MATCH, TokenType::TK_LIKE_KW]);
                    let op = match tok.token_type.unwrap() {
                        TokenType::TK_MATCH => LikeOperator::Match,
                        TokenType::TK_LIKE_KW => {
                            if b"LIKE".eq_ignore_ascii_case(tok.value) {
                                LikeOperator::Like
                            } else if b"GLOB".eq_ignore_ascii_case(tok.value) {
                                LikeOperator::Glob
                            } else if b"REGEXP".eq_ignore_ascii_case(tok.value) {
                                LikeOperator::Regexp
                            } else {
                                unreachable!()
                            }
                        }
                        _ => unreachable!(),
                    };

                    let expr = self.parse_expr(5)?; // do not consume ESCAPE
                    let escape = if let Some(tok) = self.peek_ignore_eof()? {
                        if tok.token_type == Some(TokenType::TK_ESCAPE) {
                            self.eat_assert(&[TokenType::TK_ESCAPE]);
                            Some(self.parse_expr(5)?)
                        } else {
                            None
                        }
                    } else {
                        None
                    };

                    Box::new(Expr::Like {
                        lhs: result,
                        not,
                        op,
                        rhs: expr,
                        escape,
                    })
                }
                TokenType::TK_ISNULL => {
                    self.eat_assert(&[TokenType::TK_ISNULL]);
                    Box::new(Expr::IsNull(result))
                }
                TokenType::TK_NOTNULL => {
                    self.eat_assert(&[TokenType::TK_NOTNULL]);
                    Box::new(Expr::NotNull(result))
                }
                TokenType::TK_LT => {
                    self.eat_assert(&[TokenType::TK_LT]);
                    Box::new(Expr::Binary(result, Operator::Less, self.parse_expr(pre)?))
                }
                TokenType::TK_GT => {
                    self.eat_assert(&[TokenType::TK_GT]);
                    Box::new(Expr::Binary(
                        result,
                        Operator::Greater,
                        self.parse_expr(pre)?,
                    ))
                }
                TokenType::TK_LE => {
                    self.eat_assert(&[TokenType::TK_LE]);
                    Box::new(Expr::Binary(
                        result,
                        Operator::LessEquals,
                        self.parse_expr(pre)?,
                    ))
                }
                TokenType::TK_GE => {
                    self.eat_assert(&[TokenType::TK_GE]);
                    Box::new(Expr::Binary(
                        result,
                        Operator::GreaterEquals,
                        self.parse_expr(pre)?,
                    ))
                }
                TokenType::TK_ESCAPE => unreachable!(),
                TokenType::TK_BITAND => {
                    self.eat_assert(&[TokenType::TK_BITAND]);
                    Box::new(Expr::Binary(
                        result,
                        Operator::BitwiseAnd,
                        self.parse_expr(pre)?,
                    ))
                }
                TokenType::TK_BITOR => {
                    self.eat_assert(&[TokenType::TK_BITOR]);
                    Box::new(Expr::Binary(
                        result,
                        Operator::BitwiseOr,
                        self.parse_expr(pre)?,
                    ))
                }
                TokenType::TK_LSHIFT => {
                    self.eat_assert(&[TokenType::TK_LSHIFT]);
                    Box::new(Expr::Binary(
                        result,
                        Operator::LeftShift,
                        self.parse_expr(pre)?,
                    ))
                }
                TokenType::TK_RSHIFT => {
                    self.eat_assert(&[TokenType::TK_RSHIFT]);
                    Box::new(Expr::Binary(
                        result,
                        Operator::RightShift,
                        self.parse_expr(pre)?,
                    ))
                }
                TokenType::TK_PLUS => {
                    self.eat_assert(&[TokenType::TK_PLUS]);
                    Box::new(Expr::Binary(result, Operator::Add, self.parse_expr(pre)?))
                }
                TokenType::TK_MINUS => {
                    self.eat_assert(&[TokenType::TK_MINUS]);
                    Box::new(Expr::Binary(
                        result,
                        Operator::Subtract,
                        self.parse_expr(pre)?,
                    ))
                }
                TokenType::TK_STAR => {
                    self.eat_assert(&[TokenType::TK_STAR]);
                    Box::new(Expr::Binary(
                        result,
                        Operator::Multiply,
                        self.parse_expr(pre)?,
                    ))
                }
                TokenType::TK_SLASH => {
                    self.eat_assert(&[TokenType::TK_SLASH]);
                    Box::new(Expr::Binary(
                        result,
                        Operator::Divide,
                        self.parse_expr(pre)?,
                    ))
                }
                TokenType::TK_REM => {
                    self.eat_assert(&[TokenType::TK_REM]);
                    Box::new(Expr::Binary(
                        result,
                        Operator::Modulus,
                        self.parse_expr(pre)?,
                    ))
                }
                TokenType::TK_CONCAT => {
                    self.eat_assert(&[TokenType::TK_CONCAT]);
                    Box::new(Expr::Binary(
                        result,
                        Operator::Concat,
                        self.parse_expr(pre)?,
                    ))
                }
                TokenType::TK_PTR => {
                    let tok = self.eat_assert(&[TokenType::TK_PTR]);
                    let op = if tok.value.len() == 2 {
                        Operator::ArrowRight
                    } else {
                        Operator::ArrowRightShift
                    };

                    Box::new(Expr::Binary(result, op, self.parse_expr(pre)?))
                }
                TokenType::TK_COLLATE => {
                    Box::new(Expr::Collate(result, self.parse_collate()?.unwrap()))
                }
                _ => unreachable!(),
            }
        }

        Ok(result)
    }

    #[inline(always)]
    fn parse_collate(&mut self) -> Result<Option<Name>, Error> {
        if let Some(tok) = self.peek_ignore_eof()? {
            if tok.token_type == Some(TokenType::TK_COLLATE) {
                self.eat_assert(&[TokenType::TK_COLLATE]);
            } else {
                return Ok(None);
            }
        } else {
            return Ok(None);
        }

        let tok = self.eat_expect(&[TokenType::TK_ID, TokenType::TK_STRING])?;
        let first_char = tok.value[0]; // no need to check empty
        match first_char {
            b'[' | b'\'' | b'`' | b'"' => Ok(Some(Name::Quoted(from_bytes(tok.value)))),
            _ => Ok(Some(Name::Ident(from_bytes(tok.value)))),
        }
    }

    #[inline(always)]
    fn parse_sort_order(&mut self) -> Result<Option<SortOrder>, Error> {
        match self.peek_ignore_eof()? {
            Some(tok) if tok.token_type == Some(TokenType::TK_ASC) => {
                self.eat_assert(&[TokenType::TK_ASC]);
                Ok(Some(SortOrder::Asc))
            }
            Some(tok) if tok.token_type == Some(TokenType::TK_DESC) => {
                self.eat_assert(&[TokenType::TK_DESC]);
                Ok(Some(SortOrder::Desc))
            }
            _ => Ok(None),
        }
    }

    #[inline(always)]
    fn parse_eid_list(&mut self) -> Result<Vec<IndexedColumn>, Error> {
        if let Some(tok) = self.peek_ignore_eof()? {
            if tok.token_type == Some(TokenType::TK_LP) {
                self.eat_assert(&[TokenType::TK_LP]);
            } else {
                return Ok(vec![]);
            }
        } else {
            return Ok(vec![]);
        }

        let mut columns = vec![];
        loop {
            if self.peek_no_eof()?.token_type == Some(TokenType::TK_RP) {
                self.eat_assert(&[TokenType::TK_RP]);
                break;
            }

            self.peek_nm()?;
            let nm = self.parse_nm();
            let collate = self.parse_collate()?;
            let sort_order = self.parse_sort_order()?;
            columns.push(IndexedColumn {
                col_name: nm,
                collation_name: collate,
                order: sort_order,
            });

            let tok = self.eat_expect(&[TokenType::TK_COMMA, TokenType::TK_RP])?;
            if tok.token_type == Some(TokenType::TK_RP) {
                break;
            }
        }

        Ok(columns)
    }

    fn parse_common_table_expr(&mut self) -> Result<CommonTableExpr, Error> {
        self.peek_nm()?;
        let nm = self.parse_nm();
        let eid_list = self.parse_eid_list()?;
        self.eat_expect(&[TokenType::TK_AS])?;
        let wqas = match self.peek_no_eof()?.token_type.unwrap() {
            TokenType::TK_MATERIALIZED => {
                self.eat_assert(&[TokenType::TK_MATERIALIZED]);
                Materialized::Yes
            }
            TokenType::TK_NOT => {
                self.eat_assert(&[TokenType::TK_NOT]);
                self.eat_expect(&[TokenType::TK_MATERIALIZED])?;
                Materialized::No
            }
            _ => Materialized::Any,
        };
        self.eat_expect(&[TokenType::TK_LP])?;
        let select = self.parse_select()?;
        self.eat_expect(&[TokenType::TK_RP])?;
        Ok(CommonTableExpr {
            tbl_name: nm,
            columns: eid_list,
            materialized: wqas,
            select,
        })
    }

    #[inline(always)]
    fn parse_with(&mut self) -> Result<Option<With>, Error> {
        if let Some(tok) = self.peek_ignore_eof()? {
            if tok.token_type == Some(TokenType::TK_WITH) {
                self.eat_assert(&[TokenType::TK_WITH]);
            } else {
                return Ok(None);
            }
        } else {
            return Ok(None);
        }

        let recursive = if self.peek_no_eof()?.token_type == Some(TokenType::TK_RECURSIVE) {
            self.eat_assert(&[TokenType::TK_RECURSIVE]);
            true
        } else {
            false
        };

        let mut ctes = vec![self.parse_common_table_expr()?];
        if let Some(tok) = self.peek_ignore_eof()? {
            if tok.token_type == Some(TokenType::TK_COMMA) {
                self.eat_assert(&[TokenType::TK_COMMA]);
                loop {
                    ctes.push(self.parse_common_table_expr()?);
                    match self.peek_no_eof()?.token_type.unwrap() {
                        TokenType::TK_COMMA => {
                            self.eat_assert(&[TokenType::TK_COMMA]);
                        }
                        _ => break,
                    }
                }
            }
        }

        Ok(Some(With { recursive, ctes }))
    }

    #[inline(always)]
    fn parse_select_body(&mut self) -> Result<SelectBody, Error> {
        self.eat_expect(&[TokenType::TK_SELECT])?;
        todo!()
    }

    fn parse_sorted_column(&mut self) -> Result<SortedColumn, Error> {
        let expr = self.parse_expr(0)?;
        let sort_order = self.parse_sort_order()?;

        let nulls = match self.peek_ignore_eof()? {
            Some(tok) if tok.token_type == Some(TokenType::TK_NULLS) => {
                self.eat_assert(&[TokenType::TK_NULLS]);
                let tok = self.eat_expect(&[TokenType::TK_FIRST, TokenType::TK_LAST])?;
                match tok.token_type.unwrap() {
                    TokenType::TK_FIRST => Some(NullsOrder::First),
                    TokenType::TK_LAST => Some(NullsOrder::Last),
                    _ => unreachable!(),
                }
            }
            _ => None,
        };

        Ok(SortedColumn {
            expr,
            order: sort_order,
            nulls,
        })
    }

    #[inline(always)]
    fn parse_order_by(&mut self) -> Result<Vec<SortedColumn>, Error> {
        if let Some(tok) = self.peek_ignore_eof()? {
            if tok.token_type == Some(TokenType::TK_ORDER) {
                self.eat_assert(&[TokenType::TK_ORDER]);
            } else {
                return Ok(vec![]);
            }
        } else {
            return Ok(vec![]);
        }

        self.eat_expect(&[TokenType::TK_BY])?;
        let mut columns = vec![self.parse_sorted_column()?];
        if let Some(tok) = self.peek_ignore_eof()? {
            if tok.token_type == Some(TokenType::TK_COMMA) {
                self.eat_assert(&[TokenType::TK_COMMA]);
                loop {
                    columns.push(self.parse_sorted_column()?);
                    match self.peek_no_eof()?.token_type.unwrap() {
                        TokenType::TK_COMMA => {
                            self.eat_assert(&[TokenType::TK_COMMA]);
                        }
                        _ => break,
                    };
                }
            }
        }

        Ok(columns)
    }

    #[inline(always)]
    fn parse_limit(&mut self) -> Result<Option<Limit>, Error> {
        if let Some(tok) = self.peek_ignore_eof()? {
            if tok.token_type == Some(TokenType::TK_LIMIT) {
                self.eat_assert(&[TokenType::TK_LIMIT]);
            } else {
                return Ok(None);
            }
        } else {
            return Ok(None);
        }

        let limit = self.parse_expr(0)?;
        let offset = match self.peek_ignore_eof()? {
            Some(tok) => match tok.token_type.unwrap() {
                TokenType::TK_OFFSET | TokenType::TK_COMMA => {
                    self.eat_assert(&[TokenType::TK_OFFSET, TokenType::TK_COMMA]);
                    Some(self.parse_expr(0)?)
                }
                _ => None,
            },
            _ => None,
        };

        Ok(Some(Limit {
            expr: limit,
            offset,
        }))
    }

    #[inline(always)]
    fn parse_select(&mut self) -> Result<Select, Error> {
        let with = self.parse_with()?;
        let body = self.parse_select_body()?;
        let order_by = self.parse_order_by()?;
        let limit = self.parse_limit()?;
        Ok(Select {
            with,
            body,
            order_by,
            limit,
        })
    }

    #[inline(always)]
    fn parse_create_table_args(&mut self) -> Result<CreateTableBody, Error> {
        let tok = self.eat_expect(&[TokenType::TK_LP, TokenType::TK_AS])?;
        match tok.token_type.unwrap() {
            TokenType::TK_AS => Ok(CreateTableBody::AsSelect(self.parse_select()?)),
            TokenType::TK_LP => todo!(),
            _ => unreachable!(),
        }
    }

    #[inline(always)]
    fn parse_create_table(&mut self, temporary: bool) -> Result<Stmt, Error> {
        self.eat_assert(&[TokenType::TK_TABLE]);
        let if_not_exists = self.parse_if_not_exists()?;
        let tbl_name = self.parse_fullname(false)?;
        let body = self.parse_create_table_args()?;
        Ok(Stmt::CreateTable {
            temporary,
            if_not_exists,
            tbl_name,
            body,
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
                    name: Some(Name::Quoted("'my_transaction'".to_string())),
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
            (
                b"COMMIT".as_slice(),
                vec![Cmd::Stmt(Stmt::Commit { name: None })],
            ),
            (
                b"END".as_slice(),
                vec![Cmd::Stmt(Stmt::Commit { name: None })],
            ),
            (
                b"COMMIT TRANSACTION".as_slice(),
                vec![Cmd::Stmt(Stmt::Commit { name: None })],
            ),
            (
                b"END TRANSACTION".as_slice(),
                vec![Cmd::Stmt(Stmt::Commit { name: None })],
            ),
            (
                b"COMMIT TRANSACTION my_transaction".as_slice(),
                vec![Cmd::Stmt(Stmt::Commit {
                    name: Some(Name::Ident("my_transaction".to_string())),
                })],
            ),
            (
                b"END TRANSACTION my_transaction".as_slice(),
                vec![Cmd::Stmt(Stmt::Commit {
                    name: Some(Name::Ident("my_transaction".to_string())),
                })],
            ),
            // Rollback
            (
                b"ROLLBACK".as_slice(),
                vec![Cmd::Stmt(Stmt::Rollback {
                    tx_name: None,
                    savepoint_name: None,
                })],
            ),
            (
                b"ROLLBACK TO SAVEPOINT my_savepoint".as_slice(),
                vec![Cmd::Stmt(Stmt::Rollback {
                    tx_name: None,
                    savepoint_name: Some(Name::Ident("my_savepoint".to_string())),
                })],
            ),
            (
                b"ROLLBACK TO my_savepoint".as_slice(),
                vec![Cmd::Stmt(Stmt::Rollback {
                    tx_name: None,
                    savepoint_name: Some(Name::Ident("my_savepoint".to_string())),
                })],
            ),
            (
                b"ROLLBACK TRANSACTION my_transaction".as_slice(),
                vec![Cmd::Stmt(Stmt::Rollback {
                    tx_name: Some(Name::Ident("my_transaction".to_string())),
                    savepoint_name: None,
                })],
            ),
            (
                b"ROLLBACK TRANSACTION my_transaction TO my_savepoint".as_slice(),
                vec![Cmd::Stmt(Stmt::Rollback {
                    tx_name: Some(Name::Ident("my_transaction".to_string())),
                    savepoint_name: Some(Name::Ident("my_savepoint".to_string())),
                })],
            ),
            // savepoint
            (
                b"SAVEPOINT my_savepoint".as_slice(),
                vec![Cmd::Stmt(Stmt::Savepoint {
                    name: Name::Ident("my_savepoint".to_string()),
                })],
            ),
            (
                b"SAVEPOINT 'my_savepoint'".as_slice(),
                vec![Cmd::Stmt(Stmt::Savepoint {
                    name: Name::Quoted("'my_savepoint'".to_string()),
                })],
            ),
            // release
            (
                b"RELEASE my_savepoint".as_slice(),
                vec![Cmd::Stmt(Stmt::Release {
                    name: Name::Ident("my_savepoint".to_string()),
                })],
            ),
            (
                b"RELEASE SAVEPOINT my_savepoint".as_slice(),
                vec![Cmd::Stmt(Stmt::Release {
                    name: Name::Ident("my_savepoint".to_string()),
                })],
            ),
            (
                b"RELEASE SAVEPOINT 'my_savepoint'".as_slice(),
                vec![Cmd::Stmt(Stmt::Release {
                    name: Name::Quoted("'my_savepoint'".to_string()),
                })],
            ),
        ];

        for (input, expected) in test_cases {
            println!("Testing input: {:?}", from_bytes(input));
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
