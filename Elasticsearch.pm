package DBIx::Class::Elasticsearch;
use strict;
use warnings;

use Search::Elasticsearch;

use YAML::Syck;
use File::Basename;
use Data::Dumper;
use Moose;

has es_store => (
    is  => 'rw',
    isa => 'Object'
);

has settings_store => (
    is  => 'rw',
    isa => 'HashRef'
);

# TODO: move to resultset row
sub has_searchable {
    my $self = shift;

    return scalar $self->searchable_fields;
}

# TODO: move to resultset row
sub searchable_fields {
    my $self = shift;

    my $klass             = $self->result_class;
    my $cols              = $klass->columns_info;
    my @searchable_fields = grep { $cols->{$_}->{searchable} } keys %{$cols};

    return @searchable_fields;
}

# TODO move to schema
sub es {

    my ($self) = @_;

    my $settings = $self->settings;

    $self->es_store( Search::Elasticsearch->new( nodes => sprintf( '%s:%s', $settings->{host}, $settings->{port} ) ) ) unless $self->es_store;

    return $self->es_store;
}

# TODO move to schema
sub settings {
    my $self = shift;
    my $path = dirname(__FILE__);

    if ( !$self->settings_store ) {
        my $yml = YAML::Syck::LoadFile("$path/elastic_search.yml");
        die "Could not load settings. elastic_search.yml not found" unless $yml;
        $self->settings_store($yml);
    }

    return $self->settings_store;
}

# TODO move to Row
sub es_index {

    my ( $self, $body ) = @_;

    my $type = $self->result_source->name;

    $self->es->index(
        index => $self->settings->{index},
        id    => sprintf( "%s_%s", $type, $self->es_id ),
        type  => $type,
        body  => $body

    );

}

# TODO move to RS
sub es_bulk {

    my ( $self, $data ) = @_;

    my $bulk = $self->es->bulk_helper;

    for my $row (@$data) {

        my $type = $self->result_source->name;

        my $params = {
            index  => $self->settings->{index},
            id     => sprintf( "%s_%s", $type, $row->{es_id} ),
            type   => $type,
            source => $row
        };

        $bulk->index($params);
    }

    $bulk->flush;
}

# TODO move to Row
sub es_delete {

    my ( $self, $entry ) = @_;

    my $pk = $self->primary_key;

    my $type = $self->result_source->name;

    $self->es->delete(
        id    => sprintf( "%s_%s", $type, $self->es_id ),
        type  => $type,
        index => $self->settings->{index},
    );
}

# use row->id
sub primary_keys {
    my $self = shift;

    my @ids = $self->result_source->primary_columns;
    return @ids;
}

# use row->id
sub es_id {
    my $self = shift;

    my $concat_id = [];

    for my $id ( $self->primary_keys ) {

        push @$concat_id, $self->$id if $self->$id;
    }

    return join '_', @$concat_id;
}

1;
