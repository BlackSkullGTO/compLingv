#encoding "utf-8"

Name -> Word<kwtype=celebrities> | Noun<kwtype=attractions>;

Id -> AnyWord<wff=/[1-9][0-9][0-9][0-9][0-9][0-9][0-9]/>;

Sentence -> Id interp (News.id::not_norm) AnyWord* Name interp (News.name);
