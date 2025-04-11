use std::{
    collections::HashMap,
    io::{self, Write},
    path::PathBuf,
    process::Command,
};

use crate::{InteractionPlan, Paths};

/// A bug is a run that has been identified as buggy.
#[derive(Clone)]
pub(crate) enum Bug {
    Unloaded { seed: u64 },
    Loaded { seed: u64, plan: InteractionPlan },
}

impl Bug {
    /// Check if the bug is loaded.
    pub(crate) fn is_loaded(&self) -> bool {
        match self {
            Bug::Unloaded { .. } => false,
            Bug::Loaded { .. } => true,
        }
    }

    /// Get the seed of the bug.
    pub(crate) fn seed(&self) -> u64 {
        match self {
            Bug::Unloaded { seed } => *seed,
            Bug::Loaded { seed, .. } => *seed,
        }
    }
}

/// Bug Base is a local database of buggy runs.
pub(crate) struct BugBase {
    /// Path to the bug base directory.
    path: PathBuf,
    /// The list of buggy runs, uniquely identified by their seed
    bugs: HashMap<u64, Bug>,
}

impl BugBase {
    /// Create a new bug base.
    fn new(path: PathBuf) -> Result<Self, String> {
        let mut bugs = HashMap::new();
        // list all the bugs in the path as directories
        if let Ok(entries) = std::fs::read_dir(&path) {
            for entry in entries.flatten() {
                if entry.file_type().is_ok_and(|ft| ft.is_dir()) {
                    let seed = entry
                        .file_name()
                        .to_string_lossy()
                        .to_string()
                        .parse::<u64>()
                        .or(Err(format!(
                            "failed to parse seed from directory name {}",
                            entry.file_name().to_string_lossy()
                        )))?;
                    bugs.insert(seed, Bug::Unloaded { seed });
                }
            }
        }

        Ok(Self { path, bugs })
    }

    /// Load the bug base from one of the potential paths.
    pub(crate) fn load() -> Result<Self, String> {
        let potential_paths = vec![
            // limbo project directory
            BugBase::get_limbo_project_dir()?,
            // home directory
            dirs::home_dir().ok_or("should be able to get home directory".to_string())?,
            // current directory
            std::env::current_dir()
                .or(Err("should be able to get current directory".to_string()))?,
        ];

        for path in potential_paths {
            let path = path.join(".bugbase");
            if path.exists() {
                return BugBase::new(path);
            }
        }

        println!("select bug base location:");
        println!("1. limbo project directory");
        println!("2. home directory");
        println!("3. current directory");
        print!("> ");
        io::stdout().flush().unwrap();
        let mut choice = String::new();
        io::stdin()
            .read_line(&mut choice)
            .expect("failed to read line");

        let choice = choice
            .trim()
            .parse::<u32>()
            .or(Err(format!("invalid choice {choice}")))?;
        let path = match choice {
            1 => BugBase::get_limbo_project_dir()?.join(".bugbase"),
            2 => {
                let home = std::env::var("HOME").or(Err("failed to get home directory"))?;
                PathBuf::from(home).join(".bugbase")
            }
            3 => PathBuf::from(".bugbase"),
            _ => return Err(format!("invalid choice {choice}")),
        };

        if path.exists() {
            unreachable!("bug base already exists at {}", path.display());
        } else {
            std::fs::create_dir_all(&path).or(Err("failed to create bug base"))?;
            log::info!("bug base created at {}", path.display());
            BugBase::new(path)
        }
    }

    /// Add a new bug to the bug base.
    pub(crate) fn add_bug(&mut self, seed: u64, plan: InteractionPlan) -> Result<(), String> {
        log::debug!("adding bug with seed {}", seed);
        if self.bugs.contains_key(&seed) {
            return Err(format!("Bug with hash {} already exists", seed));
        }
        self.save_bug(seed, &plan)?;
        self.bugs.insert(seed, Bug::Loaded { seed, plan });
        Ok(())
    }

    /// Get a bug from the bug base.
    pub(crate) fn get_bug(&self, seed: u64) -> Option<&Bug> {
        self.bugs.get(&seed)
    }

    /// Save a bug to the bug base.
    pub(crate) fn save_bug(&self, seed: u64, plan: &InteractionPlan) -> Result<(), String> {
        let bug_path = self.path.join(seed.to_string());
        std::fs::create_dir_all(&bug_path)
            .or(Err("should be able to create bug directory".to_string()))?;

        let seed_path = bug_path.join("seed.txt");
        std::fs::write(&seed_path, seed.to_string())
            .or(Err("should be able to write seed file".to_string()))?;

        // At some point we might want to save the commit hash of the current
        // version of Limbo.
        // let commit_hash = Self::get_current_commit_hash()?;
        // let commit_hash_path = bug_path.join("commit_hash.txt");
        // std::fs::write(&commit_hash_path, commit_hash)
        //     .or(Err("should be able to write commit hash file".to_string()))?;

        let plan_path = bug_path.join("plan.json");
        std::fs::write(
            &plan_path,
            serde_json::to_string(plan).or(Err("should be able to serialize plan".to_string()))?,
        )
        .or(Err("should be able to write plan file".to_string()))?;

        let readable_plan_path = bug_path.join("plan.sql");
        std::fs::write(&readable_plan_path, plan.to_string())
            .or(Err("should be able to write readable plan file".to_string()))?;
        Ok(())
    }

    pub(crate) fn load_bug(&mut self, seed: u64) -> Result<InteractionPlan, String> {
        let seed_match = self.bugs.get(&seed);

        match seed_match {
            None => Err(format!("No bugs found for seed {}", seed)),
            Some(Bug::Unloaded { .. }) => {
                let plan =
                    std::fs::read_to_string(self.path.join(seed.to_string()).join("plan.json"))
                        .or(Err("should be able to read plan file".to_string()))?;
                let plan: InteractionPlan = serde_json::from_str(&plan)
                    .or(Err("should be able to deserialize plan".to_string()))?;

                let bug = Bug::Loaded {
                    seed,
                    plan: plan.clone(),
                };
                self.bugs.insert(seed, bug);
                log::debug!("Loaded bug with seed {}", seed);
                Ok(plan)
            }
            Some(Bug::Loaded { plan, .. }) => {
                log::warn!(
                    "Bug with seed {} is already loaded, returning the existing plan",
                    seed
                );
                Ok(plan.clone())
            }
        }
    }

    pub(crate) fn remove_bug(&mut self, seed: u64) -> Result<(), String> {
        self.bugs.remove(&seed);
        std::fs::remove_dir_all(self.path.join(seed.to_string()))
            .or(Err("should be able to remove bug directory".to_string()))?;

        log::debug!("Removed bug with seed {}", seed);
        Ok(())
    }
}

impl BugBase {
    /// Get the path to the bug base directory.
    pub(crate) fn path(&self) -> &PathBuf {
        &self.path
    }

    /// Get the path to the database file for a given seed.
    pub(crate) fn db_path(&self, seed: u64) -> PathBuf {
        self.path.join(format!("{}/test.db", seed))
    }

    /// Get paths to all the files for a given seed.
    pub(crate) fn paths(&self, seed: u64) -> Paths {
        let base = self.path.join(format!("{}/", seed));
        Paths::new(&base)
    }
}

impl BugBase {
    pub(crate) fn get_limbo_project_dir() -> Result<PathBuf, String> {
        Ok(PathBuf::from(
            String::from_utf8(
                Command::new("git")
                    .args(["rev-parse", "--git-dir"])
                    .output()
                    .or(Err("should be able to get the git path".to_string()))?
                    .stdout,
            )
            .or(Err("commit hash should be valid utf8".to_string()))?
            .trim()
            .strip_suffix(".git")
            .ok_or("should be able to strip .git suffix".to_string())?,
        ))
    }
}
