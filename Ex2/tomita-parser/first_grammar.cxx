#encoding "utf-8"

Name -> Word<kwtype=celebrities> | Noun<kwtype=attractions>;

Sentence -> AnyWord* Name interp (Tomita_News.name) AnyWord*;
