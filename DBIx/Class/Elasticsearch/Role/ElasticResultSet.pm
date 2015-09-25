package DBIx::Class::Elasticsearch::Role::ElasticResultSet;

use strict;
use warnings;

use DBIx::Class::ResultClass::HashRefInflator;
use Hash::Flatten qw(:all);

use Moose::Role;

sub es_has_searchable {
    my $self = shift;

    return scalar $self->es_searchable_fields;
}

sub es_has_denormalized_relations {

    my $self = shift;

    return scalar @{ $self->es_denormalize_relations };
}

sub es_denormalize_relations {

    my $self = shift;

    my $class = $self->result_class;
    my @rels  = $class->relationships;

    my $denormalized_rels = [];

    for my $rel (@rels) {

        push @$denormalized_rels, $rel if $class->relationship_info($rel)->{es_denormalize};
    }

    return $denormalized_rels;
}

sub es_searchable_fields {
    my $self = shift;

    my $class             = $self->result_class;
    my $cols              = $class->columns_info;
    my @searchable_fields = grep { $cols->{$_}->{searchable} } keys %{$cols};

    return @searchable_fields;
}

sub es_build_prefetch_columns {

    my ( $self, $wanted_relations_path ) = @_;

    my $flat = flatten( { paths => $wanted_relations_path } );

    my $columns = [ $self->es_searchable_fields ];

    for my $key ( keys %$flat ) {

        my $rs       = $self;
        my $rel_path = $key;
        $rel_path =~ s/paths:\d+\.?//;

        if ($rel_path) {    # is a relation path

            my @relations = split( /\./, $rel_path );

            # every relation in path
            for my $rel ( @relations ) {

                # rs is going deeper for each key
                $rs = $self->result_source->schema->resultset( $rs->result_source->related_class($rel) );

                my $rel_fields = [ $rs->es_searchable_fields ];

                for my $rel_field (@$rel_fields) {

                    push @$columns, sprintf( '%s.%s', $rel, $rel_field );
                }
            }

            # last relation is a value not a key
            my $rel = $flat->{$key};

            $rs = $self->result_source->schema->resultset( $rs->result_source->related_class($rel) );
            my $rel_fields = [ $rs->es_searchable_fields ];

            for my $rel_field (@$rel_fields) {

                push @$columns, sprintf( '%s.%s', $rel, $rel_field );
            }

        } else {    # is a single relation

            my $rel = $flat->{$key};

            $rs = $self->result_source->schema->resultset( $rs->result_source->related_class($rel) );
            my $rel_fields = [ $rs->es_searchable_fields ];

            for my $rel_field (@$rel_fields) {

                push @$columns, sprintf( '%s.%s', $rel, $rel_field );
            }
        }
    }

    return $columns;
}

sub es_build_prefetch {

    my ($self) = @_;

    return $self unless my $wanted_relations_path = $self->result_source->source_info->{es_wanted_relations_path};

    return { prefetch => $wanted_relations_path, '+columns' => $self->es_build_prefetch_columns($wanted_relations_path) };
}

sub batch_index {
    warn "Batch Indexing...\n";
    my $self = shift;
    my $batch_size = shift || 1000;
    my ( $data, $rows ) = ( [], 0 );

    return unless $self->es_has_searchable;

    my @fields = $self->es_searchable_fields;

    my $denormalize_rels = $self->es_denormalize_rels;
    my $prefetch = $self->es_build_prefetch;

    my $results = [ $self->search( undef, { select => \@fields, %$prefetch } )->all ];    # add prefetches if they are any

    $results->result_class('DBIx::Class::ResultClass::HashRefInflator');

    my $count = $results->count;

    while ( defined( my $row = shift @$results ) ) {
        $rows++;

        $row->{es_id} = $self->es_id($row);

        push( @$data, $row );
        if ( $rows == $batch_size || $rows == $count ) {
            warn "Batched $rows rows\n";
            $self->es_bulk($data);

            $count = $count - $rows;
            ( $data, $rows ) = ( [], 0 );
        }
    }

    1;
}

sub es_id {

    my $self = shift;
    my $row  = shift;

    my @pks = $self->result_source->primary_columns;

    my $ids = [];

    for my $pk (@pks) {

        push @$ids, $row->{$pk};
    }

    return join '_', @$ids;
}

sub es {

    return shift->result_source->schema->es;
}

sub es_bulk {

    my ( $self, $data ) = @_;

    my $bulk   = $self->es->bulk_helper;
    my $schema = $self->result_source->schema;

    for my $row_raw (@$data) {

        my $type = $self->result_source->name;

        my $row = {};

        for my $key ( keys %$row_raw ) {

            $row->{$key} = $row_raw->{$key} if $row_raw->{$key};
        }

        my $params = {
            index  => $schema->es_index_name,
            id     => sprintf( "%s_%s", $type, $row->{es_id} ),
            type   => $type,
            source => $row
        };

        $bulk->index($params);
    }

    $bulk->flush;
}

sub es_mapping {

    my ($self) = @_;

    my $source = $self->result_source;

    return unless $self->es_has_searchable;

    my @fields = $self->es_searchable_fields;

    my $mapping = {};

    my $type_translations = {

        varchar  => { type => "string", index            => "analyzed" },
        enum     => { type => "string", index            => "not_analyzed", store => "yes" },
        char     => { type => "string", index            => "not_analyzed", store => "yes" },
        date     => { type => "date",   ignore_malformed => 1 },
        datetime => { type => "date",   ignore_malformed => 1 },
        text     => { type => "string", index            => "analyzed", store => "yes", "term_vector" => "with_positions_offsets" },
        integer => { type => "integer", index => "not_analyzed", store => "yes" },
        float   => { type => "float",   index => "not_analyzed", store => "yes" },
        decimal => { type => "float",   index => "not_analyzed", store => "yes" },

    };

    for my $field (@fields) {

        my $column_info         = $source->column_info($field);
        my $mapping_translation = $type_translations->{ $column_info->{data_type} } || {};
        my $elastic_mapping     = $column_info->{elastic_mapping} || {};

        my $merged = { %$mapping_translation, %$elastic_mapping };

        $mapping->{$field} = $merged if $merged;
    }

    return $mapping;

}

1;
