package DBIx::Class::Elasticsearch::Role::ElasticResult;

use strict;
use warnings;

use Moose::Role;

sub es_index {

    my $self = shift;

    my $schema = $self->result_source->schema;
    my $class  = $self->result_source->source_name;

    my $elastic_rs  = $schema->dispatcher->{$class};
    my $dbix_rs     = $self->result_source->resultset;
    my $me          = $dbix_rs->current_source_alias;
    my $dbix_params = { map { $me . "." . $_ => $self->$_ } $self->primary_columns };

    $dbix_rs = $self->result_source->resultset->search_rs($dbix_params);

    for my $rs (@$elastic_rs) {

        eval "use $rs";

        warn $@ if $@;

        $rs->($dbix_rs);
    }
}

after 'insert' => sub {
    my $self = shift;

    return do {

        warn "Inserting ...";
        $self->es_index;
    };
};

after 'update' => sub {
    my $self = shift;

    return do {

        warn "Updating ...";
        $self->es_index;
    };
};

after 'delete' => sub {
    my $self = shift;

    return do {

        warn "Deleting...\n";
        $self->es_delete;
    };
};

sub es_index_transfer {

    my ( $self, $body ) = @_;

    my $type = $self->result_source->name;

    my $parent = {};
    if ( $self->es_is_child ) {

        $parent = { parent => $self->es_parent };
    }

}

sub es {

    return shift->result_source->schema->es;
}

sub schema {

}

sub es_delete {

    my ( $self, $entry ) = @_;

    my $type = $self->result_source->name;

    my %columns;

    $self->es->delete(
        id    => $self->es_id( \%columns ),
        type  => $type,
        index => $self->result_source->schema->es_index_name,
    );
}

1;
