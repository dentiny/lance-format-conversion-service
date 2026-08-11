ALTER TABLE jobs ADD COLUMN blob_columns_json TEXT NOT NULL DEFAULT '[]' CHECK (
    json_valid(blob_columns_json)
    AND json_type(blob_columns_json) = 'array'
);

ALTER TABLE jobs ADD COLUMN indices_json TEXT NOT NULL DEFAULT '[]' CHECK (
    json_valid(indices_json)
    AND json_type(indices_json) = 'array'
);
