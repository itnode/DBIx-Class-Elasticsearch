## Under Construction

This Fork is under heavy changes. I will update the Docs soon

## Description

DBIx::Class::Elasticsearch is a Module to link your DBIx::Class Schema to Elastic faster

## Setting up your DBIx::Model

### Adding role to your Schema Class

    with 'DBIx::Class::Elasticsearch::Role::ElasticSchema';

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
            is_auto:increment => 1,
            searchable => 1 # adds this field to the index
            elastic_mapping => {
                index => "analyzed"
            } # overwrites defaults for mapping
        }
    );


