export interface ElasticsearchResponse {
    took: number;
    timed_out: boolean;
    _shards: {
        total: number,
        succesful: number,
        failed: number
    };
    hits: {
        total: number,
        max_score: number,
        hits: {
            _index: string,
            _type: string,
            _id: string,
            _score: number,
            _source: Object
        }[]
    };
    aggregations: {
        range: {
            buckets: {
                doc_count: number,
                from: number,
                from_as_string: string,
                key: string,
                resolution: {
                    buckets: {
                        key_as_string: string,
                        key: number,
                        doc_count: number,
                        avg_ask: {
                            value: number
                        },
                        avg_bid: {
                            value: number
                        },
                        median_ask: {
                            values
                        },
                        median_bis: {
                            values
                        }
                    }[]
                },
                to: number,
                to_as_string: string
            }[]
        }

    };
}
