CREATE TABLE test.repositories
(
    name     String,
    owner    String,
    stars    UInt32,
    watchers UInt32,
    forks    UInt32,
    language String,
    updated  DateTime
) ENGINE = ReplacingMergeTree(updated)
      ORDER BY (owner, name);

CREATE TABLE test.repositories_authors_commits
(
    date        Date,
    repo        String,
    author      String,
    commits_num UInt32
) ENGINE = ReplacingMergeTree
      PARTITION BY date
      ORDER BY (date, repo, author);

CREATE TABLE test.repositories_positions
(
    date     Date,
    repo     String,
    position UInt32
) ENGINE = ReplacingMergeTree
      PARTITION BY date
      ORDER BY (date, repo);
