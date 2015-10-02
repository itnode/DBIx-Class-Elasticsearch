package DBIx::Class::Elasticsearch::Role::ElasticResult;

use strict;
use warnings;

use Moose::Role;

=head2
sub es_index {
    my $self = shift;

    return unless $self->es_has_searchable;

    # reload object to proceed DateTimeColumn etc
    $self->discard_changes;

    warn "Indexing...\n";

    my $rs       = $self->result_source->resultset;
    my $prefetch = $self->result_source->resultset->es_build_prefetch;

    my %columns = $self->get_columns;

    my $me = $rs->current_source_alias;

    my $query = { map { sprintf( '%s.%s', $me, $_ ) => $columns{$_} } $self->result_source->primary_columns };

    $rs = $rs->search_rs( $query, $prefetch );
    $rs->result_class('DBIx::Class::ResultClass::HashRefInflator');

    my $body = [ $rs->all ];

    return $self->es_index( $body->[0] );
}
=cut

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

        $rs->es_index($dbix_rs);
    }
}

=head2
sub es_searchable_fields {

    return shift->result_source->resultset->es_searchable_fields;
}

sub es_has_searchable {

    return shift->result_source->resultset->es_has_searchable;
}

sub es_id {

    return shift->result_source->resultset->es_id(shift);
}

sub es_is_primary {

    return shift->result_source->resultset->es_is_primary;
}

sub es_is_child {

    return shift->result_source->resultset->es_is_child;
}

sub es_parent {

    return shift->result_source->resultset->es_parent;
}
=cut

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
