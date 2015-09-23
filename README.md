## Description

DBIx::Class::Elasticsearch is a Module to link your DBIx::Class Schema to Elastic faster

## Setting up your DBIx::Model

### Adding role to your Schema Class

    with 'DBIx::Class::Elasticsearch::Role::ElasticSchema';

In advanced you need to handle over your Schema the connection informations for Elastic

    $schema->connect_elastic( { host => "localhost", port => 9200, index => "MyApp" } );

### Adding role to your ResultSet Class

    with 'DBIx::Class::Elasticsearch::Role::ElasticResultSet';

### Adding role to your Result Class

    with 'DBIx::Class::Elasticsearch::Role::ElasticResult';

### Setting up your columns to use

In your row object, you have to modify your add_columns section

    # assuming a car model

    __PACKAGE__->add_columns(
        "car_id",
        {
            data_type => "integer",
            is_auto_increment => 1,
            searchable => 1 # adds this field to the index
            elastic_mapping => {
                index => "analyzed"
            } # overwrites defaults for mapping
        }
    );

### Default Type mappings

DBIx::Class::Row data_type are mapped as following

        varchar  => { type => "string", index            => "analyzed" },
        enum     => { type => "string", index            => "not_analyzed", store => "yes" },
        char     => { type => "string", index            => "not_analyzed", store => "yes" },
        date     => { type => "date",   ignore_malformed => 1 },
        datetime => { type => "date",   ignore_malformed => 1 },
        text     => { type => "string", index            => "analyzed", store => "yes", "term_vector" => "with_positions_offsets" },
        integer => { type => "integer", index => "not_analyzed", store => "yes" },
        float   => { type => "float",   index => "not_analyzed", store => "yes" },
        decimal => { type => "float",   index => "not_analyzed", store => "yes" },

### Running your Application

DBIx::Class::Elasticsearch::Role will hook into your insert, update and delete DBIx::Class::Row methods. If you change Data in your Database, it will be synced with the Elastic Storage.

### Mapping

In the Schema Role we provide an 'es_create_mapping' and an 'es_drop_mapping' function. Use them to create or delete your mapping in the Elastic Storage

    $schema->es_create_mapping;
    $schema->es_drop_mapping;

### Batch indexing

The Schema role provides 'es_index_all' which will batch index all searchable rows in your application

    $schema->es_index_all;
