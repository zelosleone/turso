use crate::{model::Query, SimulatorEnv};
use rand::Rng;
use sql_generation::{
    generation::{frequency, Arbitrary, ArbitraryFrom},
    model::query::{update::Update, Create, Delete, Insert, Select},
};

use super::property::Remaining;

impl ArbitraryFrom<(&SimulatorEnv, &Remaining)> for Query {
    fn arbitrary_from<R: Rng>(rng: &mut R, (env, remaining): (&SimulatorEnv, &Remaining)) -> Self {
        frequency(
            vec![
                (
                    remaining.create,
                    Box::new(|rng| Self::Create(Create::arbitrary(rng))),
                ),
                (
                    remaining.read,
                    Box::new(|rng| Self::Select(Select::arbitrary_from(rng, env))),
                ),
                (
                    remaining.write,
                    Box::new(|rng| Self::Insert(Insert::arbitrary_from(rng, env))),
                ),
                (
                    remaining.update,
                    Box::new(|rng| Self::Update(Update::arbitrary_from(rng, env))),
                ),
                (
                    f64::min(remaining.write, remaining.delete),
                    Box::new(|rng| Self::Delete(Delete::arbitrary_from(rng, env))),
                ),
            ],
            rng,
        )
    }
}
