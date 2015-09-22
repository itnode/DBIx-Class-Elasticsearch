## Under Construction

This Fork is under heavy changes. I will update the Docs soon

## Description

DBIx::Class::Elasticsearch is a Module to link your DBIx::Class Schema to Elastic faster

## Installation
### Install as a submodule
    $ git submodule add git@github.com:ShepFc3/DBIx::Class::Elasticsearch.git lib/DBIx::Class::Elasticsearch
    $ git submodule init
    $ git submodule update

## Setup
### Copy elastic_search.yml.sample to elastic_search.yml
*edit file to reflect your elastic search settings*  

### Include the base directory
    use lib '/base/dir/lib';
*you only need this if lib is already in your path*  

### Include DBIx::Class::Elasticsearch::Schema (optional) 
    use base qw(DBIx::Class::Elasticsearch::Schema DBIx::Class::Schema);
*you need this if you want to use index_all*  

### Create ElasticResult 
**Create a new class that includes DBIx::Class::Elasticsearch::Result and DBIx::Class::Core**  

    package MyApp::ElasticResult;

    use strict;
    use warnings;
    use base qw(DBIx::Class::Elasticsearch::Result DBIx::Class::Core);

    1;

### Use ElasticResult in your Result class
    use base qw(MyApp::ElasticResult);

### Define searchable columns in your Result class
    __PACKAGE__->add_columns("id", { searchable => 1 });

### Create ElasticResultSet (optional)
    package MyApp::ElasticResultSet;
    
    use strict;
    use warnings;
    use base qw(DBIx::Class::Elasticsearch::ResultSet DBIx::Class::ResultSet);
    
    1;

### Use ElasticResult
    use base qw(MyApp::ElasticResultSet);

*use this to be able to batch_index a specific resultset*  
*neccessary when using DBIx::Class::Elasticsearch::Schema*  

## Synopsis
### Batch index all DBIx classes with searchable fields
    $schema->index_all;

### Index all searchable fields for a row
    my $result = $schema->resultset('Artist')->find(1);
    $result->index();

### Batch index all searchable fields within the given resultset
    $schema->resultset('Artist')->batch_index;
