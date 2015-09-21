package DBIx::Class::Elasticsearch;
use strict;
use warnings;

use Search::Elasticsearch;

use YAML::Syck;
use File::Basename;
use Data::Dumper;
use mro 'c3';

my $es;
my $settings;

sub has_searchable {
    my $self = shift;

    return scalar $self->searchable_fields;
}

sub searchable_fields {
    my $self = shift;

    my $klass             = $self->result_class;
    my $cols              = $klass->columns_info;
    my @searchable_fields = grep { $cols->{$_}->{searchable} } keys %{$cols};

    return @searchable_fields;
}

sub es {

    my ($self) = @_;

    my $settings = $self->settings;

    $es = Search::Elasticsearch->new( nodes => sprintf( '%s:%s', $settings->{host}, $settings->{port} ) ) unless $es;

    return $es;
}

sub settings {
    my $self = shift;
    my $path = dirname(__FILE__);

    if ( !$settings ) {
        my $yml = YAML::Syck::LoadFile("$path/elastic_search.yml");
        die "Could not load settings. elastic_search.yml not found" unless $yml;
        $settings = $yml;
    }

    return $settings;
}

sub es_index {

    my ( $self, $body ) = @_;

    $self->es->index(
        index => $self->settings->{index},
        id    => $self->primary_key,
        type  => $self->result_source->name,
        body  => $body

    );

}

sub es_bulk {

    my ( $self, $data ) = @_;

    my $bulk = $self->es->bulk_helper;

    for my $row (@$data) {

        my $params = {
            index  => $self->settings->{index},
            id     => $row->{ $self->primary_key },
            type   => $self->result_source->name,
            source => $row
        };

        $bulk->index($params);
    }

    $bulk->flush;
}

sub es_delete {

    my ( $self, $entry ) = @_;

    my $pk = $self->primary_key;

    $self->es->delete(
        id    => $self->$pk,
        type  => $self->result_source->name,
        index => $self->settings->{index},
    );
}

sub post {
    my ( $self, $url, $content ) = @_;

    my $request = HTTP::Request->new( POST => $url );
    $request->content_type('application/json');
    $request->content($content);

    #return $self->user_agent->request($request);
}

sub get {
    my ( $self, $url, $content ) = @_;

    my $request = HTTP::Request->new( GET => $url );
    $request->content_type('application/json');
    $request->content($content);

    #return $self->user_agent->request($request);
}

sub http_delete {
    my ( $self, $url ) = @_;

    my $request = HTTP::Request->new( DELETE => $url );

    #return $self->user_agent->request($request);
}

sub primary_key {
    my $self = shift;

    my @ids = $self->result_source->primary_columns;
    return $ids[0];
}

1;
