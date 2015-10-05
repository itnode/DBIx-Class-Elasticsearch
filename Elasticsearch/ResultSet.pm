package Elasticsearch::ResultSet;

use strict;
use warnings;

use DBIx::Class::ResultClass::HashRefInflator;
use Search::Elasticsearch::Compat::QueryParser;
use namespace::autoclean;

use Moose;

has body => (
    is       => 'rw',
    isa      => 'HashRef',
    required => 0,
    default => sub { {} }
);

has queries => (
    is       => 'rw',
    isa      => 'ArrayRef',
    required => 0,
    default => sub { [] }
);

has filters => (
    is       => 'rw',
    isa      => 'ArrayRef',
    required => 0,
    default => sub { [] }
);


=head2 size

    Max size of Result

=cut

sub size {

    my ( $self, $size ) = @_;
    $self->body->{size} = $size;
    return $self;

}

=head2 from

    Offset for Result

=cut 

sub from {

    my ( $self, $from ) = @_;
    $self->body->{from} = $from;
    return $self;
}

sub order_by {

    my ( $self, $order ) = @_;

    $self->body->{sort} = $order || [ { "_score" => 'desc' } ];
    return $self;
}

=head2 query_string

=cut

sub query_string {

    my ( $self, $searchstring ) = @_;

    my $qp                    = Search::Elasticsearch::Compat::QueryParser->new();
    my $filtered_query_string = $qp->filter($searchstring);
    $self->body->{query}{query_string}{query} = $filtered_query_string;

    return $self;
}

=head2 query

    add query 

=cut

sub query {

    my ( $self, $query ) = @_;
    push @{ $self->{queries} }, $query;
    return $self;
}

sub filter {

    my ( $self, $filter ) = @_;
    push @{ $self->filters }, $filter;
    return $self;

}

=head2 highlighter

    Add the highlighter on $field, must be mapped with term_vector => "with_positions_offsets"

    "text" => { type => "string", index => "analyzed", "store" => "yes", "term_vector" => "with_positions_offsets" },


=cut

sub highlighter {

    my ( $self, $field ) = @_;

    die "Missing field for highlighter" unless $field;

    $self->body->{highlight} = {
        "number_of_fragments" => 3,
        "fragment_size"       => 150,
        "tags_schema"         => "styled",
        "pre_tags"            => [ '<span class="marking1">', '<span class="marking2">', '<span class="marking3">' ],
        "post_tags"           => [ "</span>", "</span>", "</span>" ],
        "fields"              => { $field => { "number_of_fragments" => 5 }, }
    };

    return $self;
}

=head2 all

    Return all items, based on chain

=cut

sub all {

    my ($self) = @_;
    $self->body->{track_scores} = 1;

    use Data::Printer;

    my $queries = $self->queries;
    my $filters = $self->filters;

    if ( @$queries == 1 ) {

        $self->body->{query} = $queries->[0];

    } elsif ( @$queries > 1 ) {

        $self->body->{query}{and} = $queries;

    }

    if ( @$filters == 1 ) {

        $self->body->{filter} = $filters->[0];

    } elsif ( @$filters > 1 ) {

        $self->body->{filter}{and} = $filters;

    }

    p $self->body;

    my $matches = $self->schema->es->search(
        index => $self->type,
        type  => $self->type,
        body  => $self->body,
    );

    my $result = [];

    foreach my $match ( @{ $matches->{hits}{hits} } ) {

        my $doc = $match->{_source};

        if ( $self->body->{highlight} ) {

            # TODO fix fix text-assign
            $doc->{highlight} = join( " … ", @{ $match->{highlight}{text} || [] } );

        }

        push @$result, $doc;
    }

    return $result;
}

sub es_index {

    my $self    = shift;
    my $dbic_rs = shift;

    $dbic_rs->result_class('DBIx::Class::ResultClass::HashRefInflator');

    while ( my $row = $dbic_rs->next ) {

        $row->{es_id} = $self->es_id( $row, $dbic_rs );

        $self->es->index(
            {
                index => $self->type,
                id    => $row->{es_id},
                type  => $self->type,
                body  => $row,
            }
        );
    }

}

sub es_delete {

    my $self    = shift;
    my $dbic_rs = shift;

    $dbic_rs->result_class('DBIx::Class::ResultClass::HashRefInflator');

    while ( my $row = $dbic_rs->next ) {

        my $id = $self->es_id( $row, $dbic_rs );

        $self->es->delete(
            id    => $id,
            type  => $self->type,
            index => $self->type,
        );
    }
}

sub es_is_primary {

    my $self = shift;
    my $class = shift;

    return 1 if $self->relation_dispatcher->{primary} eq $class;
}

sub es_is_nested {

    my $self = shift;
    my $class = shift;

    return 1 if $self->relation_dispatcher->{nested}{ $class };
}

sub es_batch_index {
    warn "Batch Indexing...\n";

    my $self = shift;
    my $rs   = shift;

    my $batch_size = shift || 1000;
    my $data = [];

    my $results = $self->index_rs;    # add prefetches

    $results->result_class('DBIx::Class::ResultClass::HashRefInflator');

    my $counter = 0;

    while ( my $row = $results->next ) {
        $counter++;

        $row->{es_id} = $self->es_id( $row, $results );

        push( @$data, $row );
        if ( $counter == $batch_size ) {
            warn "Batched $counter rows\n";
            $self->es_bulk($data);

            ( $data, $counter ) = ( [], 0 );
        }
    }

    if ( scalar @$data ) {
        warn "Batched " . scalar @$data . " rows\n";
        $self->es_bulk($data) if scalar @$data;
    }

    1;
}

sub es_id {

    my $self = shift;
    my $row  = shift;

    my $pks = $self->es_id_columns;

    my $ids = [];

    for my $pk (@$pks) {

        push @$ids, $row->{$pk};
    }

    return join '_', @$ids;
}

sub es {

    return shift->schema->es;
}

sub es_bulk {

    my ( $self, $data ) = @_;

    my $bulk   = $self->es->bulk_helper;
    my $schema = $self->schema;

    for my $row_raw (@$data) {

        my $row = {};

        for my $key ( keys %$row_raw ) {

            $row->{$key} = $row_raw->{$key} if $row_raw->{$key};
        }

        my $params = {
            index  => $self->type,
            id     => $row->{es_id},
            type   => $self->type,
            source => $row,
        };

        $bulk->index($params);
    }

    $bulk->flush;
}

__PACKAGE__->meta->make_immutable;

1;
