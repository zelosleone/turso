use crate::model::Query;
use rand::Rng;
use sql_generation::{
    generation::{frequency, Arbitrary, ArbitraryFrom, GenerationContext},
    model::query::{update::Update, Create, Delete, Insert, Select},
};

use super::property::Remaining;

impl ArbitraryFrom<&Remaining> for Query {
    fn arbitrary_from<R: Rng, C: GenerationContext>(
        rng: &mut R,
        context: &C,
        remaining: &Remaining,
    ) -> Self {
        frequency(
            vec![
                (
                    remaining.create,
                    Box::new(|rng| Self::Create(Create::arbitrary(rng, context))),
                ),
                (
                    remaining.read,
                    Box::new(|rng| Self::Select(Select::arbitrary(rng, context))),
                ),
                (
                    remaining.write,
                    Box::new(|rng| Self::Insert(Insert::arbitrary(rng, context))),
                ),
                (
                    remaining.update,
                    Box::new(|rng| Self::Update(Update::arbitrary(rng, context))),
                ),
                (
                    f64::min(remaining.write, remaining.delete),
                    Box::new(|rng| Self::Delete(Delete::arbitrary(rng, context))),
                ),
            ],
            rng,
        )
    }
}
