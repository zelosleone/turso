use std::num::NonZero;

#[derive(Clone, Debug)]
pub enum Parameter {
    Anonymous(NonZero<usize>),
    Indexed(NonZero<usize>),
    Named(String, NonZero<usize>),
}

impl PartialEq for Parameter {
    fn eq(&self, other: &Self) -> bool {
        self.index() == other.index()
    }
}

impl Parameter {
    pub fn index(&self) -> NonZero<usize> {
        match self {
            Parameter::Anonymous(index) => *index,
            Parameter::Indexed(index) => *index,
            Parameter::Named(_, index) => *index,
        }
    }
}

#[derive(Debug)]
pub struct Parameters {
    index: NonZero<usize>,
    pub list: Vec<Parameter>,
    remap: Option<Vec<NonZero<usize>>>,
    // Indexes of the referenced insert values to maintain ordering of paramaters
    param_positions: Option<Vec<(usize, NonZero<usize>)>>,
    // For insert statements with multiple rows
    current_insert_row_idx: usize,
    current_col_value_idx: Option<usize>,
}

impl Default for Parameters {
    fn default() -> Self {
        Self::new()
    }
}

impl Parameters {
    pub fn new() -> Self {
        Self {
            index: 1.try_into().unwrap(),
            list: vec![],
            remap: None,
            param_positions: None,
            current_insert_row_idx: 0,
            current_col_value_idx: None,
        }
    }

    pub fn count(&self) -> usize {
        let mut params = self.list.clone();
        params.dedup();
        params.len()
    }

    pub fn set_value_index(&mut self, idx: usize) {
        self.current_col_value_idx = Some(idx);
    }

    /// Add a parameter position to the array used to build the remap.
    pub fn push_parameter_position(&mut self, index: NonZero<usize>) {
        if let Some(cur) = self.current_col_value_idx {
            if let Some(positions) = self.param_positions.as_mut() {
                positions.push((cur, index));
                tracing::debug!("push parameter position: {:?}", positions);
            }
        }
    }

    /// Initialize the stored positions array at the start of an insert statement
    pub fn init_parameter_remap(&mut self, cols: usize) {
        self.param_positions = Some(Vec::with_capacity(cols));
    }

    /// Sorts the stored value indexes and builds and sets the remap array.
    pub fn sort_and_build_remap(&mut self) {
        self.remap = self.param_positions.as_mut().map(|positions| {
            // sort by value_index
            positions.sort_by_key(|(idx, _)| *idx);
            tracing::debug!("param positions: {:?}", positions);
            // collect the parameter indexes
            positions.iter().map(|(_, idx)| *idx).collect::<Vec<_>>()
        })
    }

    /// Returns the remapped index for a given parameter index or the original index if none is found
    pub fn get_remapped_index(&self, idx: NonZero<usize>) -> NonZero<usize> {
        let res = *self
            .remap
            .as_ref()
            .map(|p| p.get(idx.get() - 1).unwrap_or(&idx))
            .unwrap_or(&idx);
        tracing::debug!("get_remapped_value: {idx}, value: {res}");
        res
    }

    pub fn name(&self, index: NonZero<usize>) -> Option<String> {
        self.list.iter().find_map(|p| match p {
            Parameter::Anonymous(i) if *i == index => Some("?".to_string()),
            Parameter::Indexed(i) if *i == index => Some(format!("?{i}")),
            Parameter::Named(name, i) if *i == index => Some(name.to_owned()),
            _ => None,
        })
    }

    pub fn index(&self, name: impl AsRef<str>) -> Option<NonZero<usize>> {
        self.list
            .iter()
            .find_map(|p| match p {
                Parameter::Named(n, index) if n == name.as_ref() => Some(index),
                _ => None,
            })
            .copied()
    }

    pub fn next_index(&mut self) -> NonZero<usize> {
        let index = self.index;
        self.index = self.index.checked_add(1).unwrap();
        index
    }

    pub fn push(&mut self, name: impl AsRef<str>) -> NonZero<usize> {
        match name.as_ref() {
            "" => {
                let index = self.next_index();
                self.list.push(Parameter::Anonymous(index));
                tracing::trace!("anonymous parameter at {index}");
                index
            }
            name if name.starts_with(['$', ':', '@', '#']) => {
                match self
                    .list
                    .iter()
                    .find(|p| matches!(p, Parameter::Named(n, _) if name == n))
                {
                    Some(t) => {
                        let index = t.index();
                        self.list.push(t.clone());
                        tracing::trace!("named parameter at {index} as {name}");
                        index
                    }
                    None => {
                        let index = self.next_index();
                        self.list.push(Parameter::Named(name.to_owned(), index));
                        tracing::trace!("named parameter at {index} as {name}");
                        index
                    }
                }
            }
            index => {
                // SAFETY: Guaranteed from parser that the index is bigger than 0.
                let index: NonZero<usize> = index.parse().unwrap();
                if index > self.index {
                    self.index = index.checked_add(1).unwrap();
                }
                self.list.push(Parameter::Indexed(index));
                tracing::trace!("indexed parameter at {index}");
                index
            }
        }
    }
}
