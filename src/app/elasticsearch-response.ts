interface Shards {
    total: number;
    succesful: number;
    failed: number;
}

interface Hits {
    total: number;
    max_score: number;
    hits: Hit[];
}

interface Hit {
    _index: string;
    _type: string;
    _id: string;
    _score: number;
    _source: Object;
}

export interface ElasticsearchResponse {
    took: number;
    timed_out: boolean;
    _shards: Shards;
    hits: Hits;
}
