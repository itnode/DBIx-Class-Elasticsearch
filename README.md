<<<<<<< HEAD
# NAME

DBIx::Class::Elasticsearch - It's new $module

# SYNOPSIS

    use DBIx::Class::Elasticsearch;

# DESCRIPTION

DBIx::Class::Elasticsearch is ...

# LICENSE

Copyright (C) Patrick Kilter.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

# AUTHOR

Patrick Kilter <pas.freelance@googlemail.com>
=======
## Description

DBIx::Class::Elasticsearch is a Module to link your DBIx::Class Schema to Elastic faster.

It helps you, to denormalize your relational database schema to fit into the document orientated elastic store

## Warning

This repository is under development. API changes are possible at this point of time. We will create more documentation if we tested this in the wild.

## Setting up your DBIx::Model

### Adding role to your Schema Class

    with 'DBIx::Class::Elasticsearch::Role::ElasticSchema';

In advanced you need to handle over your Schema the connection informations for Elastic

    $schema->connect_elastic( { host => "localhost", port => 9200, index => "MyApp" } );

### Adding role to your Result Class

    with 'DBIx::Class::Elasticsearch::Role::ElasticResult';

### Building your own ElasticResultSet Classes

    extends 'Elasticsearch::ResultSet';

### Running your Application

DBIx::Class::Elasticsearch::Role will hook into your insert, update and delete DBIx::Class::Row methods. If you change Data in your Database, it will be synced with the Elastic Storage.

## Credits

This module is based on Chris 'SchepFc3' Shepherd work, which you can find here:

    https://github.com/ShepFc3/ElasticDBIx

## Authors

* Jens Gassmann  <jg@gassmann.it>
* Patrick Kilter <pk@gassmann.it>
>>>>>>> 9646092f4b671d3ea67c786c058b9f4f6a222c8a
