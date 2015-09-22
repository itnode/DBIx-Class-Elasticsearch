package DBIx::Class::Elasticsearch::Role::ElasticBase;
use strict;
use warnings;

use Search::Elasticsearch;

use YAML::Syck;
use File::Basename;
use Data::Dumper;
use Moose::Role;

has es_store => (
    is  => 'rw',
    isa => 'Object'
);

has settings_store => (
    is  => 'rw',
    isa => 'HashRef'
);

1;
