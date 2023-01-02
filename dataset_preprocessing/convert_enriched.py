import itertools
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd


def save_str_to_file(data: str, file_path: Path | str) -> None:
    with open(file_path, 'w') as f:
        f.write(data)


def save_df_to_file(
        df: pd.DataFrame,
        file_path: Path | str,
        header: bool = False,
        columns: Optional[Iterable] = None,
) -> None:
    df.to_csv(file_path, index=False, header=header, columns=columns if columns else df.columns)


def process_venue_entities(venues_path: Path, output_dir: Path) -> None:
    df = pd.read_csv(venues_path, index_col=0)

    df = df[['venue_ID', 'full_name', 'h_index_calculated']]

    df.rename(columns={
        'venue_ID': 'venue_ID:ID(Venue-ID)',
        'h_index_calculated': 'h_index_calculated:int',
    }, inplace=True)

    df[':LABEL'] = 'Venue'

    header_path = output_dir / 'venues_header.csv'
    content_path = output_dir / 'venues.csv'

    header = 'venue_ID:ID(Venue-ID),full_name,h_index_calculated:int,:LABEL'
    save_str_to_file(header, header_path)

    columns = [
        'venue_ID:ID(Venue-ID)',
        'full_name',
        'h_index_calculated:int',
        ':LABEL',
    ]
    save_df_to_file(df, content_path, columns=columns)


def process_author_entities(authors_path: Path, output_dir: Path):
    df = pd.read_csv(authors_path, index_col=0)

    df = df[['author_ID', 'full_name', 'h_idex_real', 'h_idex_calculated']]

    df.rename(columns={
        'author_ID': 'author_ID:ID(Author-ID)',
        'h_idex_real': 'h_index_real:int',
        'h_idex_calculated': 'h_index_calculated:int',
    }, inplace=True)

    df['h_index_real:int'] = df['h_index_real:int'].fillna(-1).astype(int)

    df[':LABEL'] = 'Author'

    header_path = output_dir / 'authors_header.csv'
    content_path = output_dir / 'authors.csv'

    header = 'author_ID:ID(Author-ID),full_name,h_index_real:int,h_index_calculated:int,:LABEL'
    save_str_to_file(header, header_path)

    columns = [
        'author_ID:ID(Author-ID)',
        'full_name',
        'h_index_real:int',
        'h_index_calculated:int',
        ':LABEL',
    ]
    save_df_to_file(df, content_path, columns=columns)


def process_affiliation_entities(affiliations_path: Path, output_dir: Path):
    df = pd.read_csv(affiliations_path, index_col=0)

    df = df[['affiliation_ID', 'institution_name', 'institution_place']]

    df['affiliation_ID'] = df['affiliation_ID'].astype(int)

    df.rename(columns={
        'affiliation_ID': 'affiliation_ID:ID(Affiliation-ID)',
        'institution_name': 'name',
        'institution_place': 'place',
    }, inplace=True)

    df[':LABEL'] = 'Affiliation'

    header_path = output_dir / 'affiliations_header.csv'
    content_path = output_dir / 'affiliations.csv'

    header = 'affiliation_ID:ID(Affiliation-ID),name,place,:LABEL'
    save_str_to_file(header, header_path)

    columns = [
        'affiliation_ID:ID(Affiliation-ID)',
        'name',
        'place',
        ':LABEL',
    ]
    save_df_to_file(df, content_path, columns=columns)


def process_publication_entities(
        publications_path: Path,
        publications_to_venues_path: Path,
        venues_path: Path,
        output_dir: Path,
):
    df = pd.read_csv(publications_path, index_col=0)

    df = df[['publication_ID', 'title', 'DOI', 'date']]

    df['title'] = df['title'].apply(lambda x: x.replace('\n', '').strip())
    df['title'] = df['title'].apply(lambda x: x.replace('  ', ' ').strip())

    df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
    df['date'] = df['date'].dt.strftime('%Y')

    publications_to_venues_df = pd.read_csv(publications_to_venues_path, index_col=0)[['publication_ID', 'venue_ID']]

    df = df.merge(publications_to_venues_df, left_on='publication_ID', right_on='publication_ID', how='left')

    venues_df = pd.read_csv(venues_path, index_col=0)[['venue_ID', 'full_name']]

    df = df.merge(venues_df, left_on='venue_ID', right_on='venue_ID', how='left')

    df = df.drop_duplicates(subset=['publication_ID'])

    df.rename(columns={
        'publication_ID': 'publication_ID:ID(Publication-ID)',
        'full_name': 'venue',
        'DOI': 'doi',
        'date': 'year:int',
    }, inplace=True)

    df[':LABEL'] = 'Publication'

    header_path = output_dir / 'publications_header.csv'
    content_path = output_dir / 'publications.csv'

    header = 'publication_ID:ID(Publication-ID),title,doi,year:int,venue,:LABEL'
    save_str_to_file(header, header_path)

    columns = [
        'publication_ID:ID(Publication-ID)',
        'title',
        'doi',
        'year:int',
        'venue',
        ':LABEL',
    ]
    save_df_to_file(df, content_path, columns=columns)


def process_author_of_relationships(df: pd.DataFrame, output_dir: Path):
    df = df[['author_ID', 'publication_ID']]

    df = df.rename(columns={
        'author_ID': ':START_ID(Author-ID)',
        'publication_ID': ':END_ID(Publication-ID)',
    })

    df[':TYPE'] = 'AUTHOR_OF'

    header_path = output_dir / 'author_of_header.csv'
    content_path = output_dir / 'author_of.csv'

    header = ':START_ID(Author-ID),:END_ID(Publication-ID),:TYPE'
    save_str_to_file(header, header_path)

    columns = [
        ':START_ID(Author-ID)',
        ':END_ID(Publication-ID)',
        ':TYPE',
    ]
    save_df_to_file(df, content_path, columns=columns)


def process_collaborates_with_relationships(df: pd.DataFrame, output_dir: Path):
    df = df[['author_ID', 'publication_ID']]

    result = pd.DataFrame(columns=['author_ID_1', 'author_ID_2'])

    for publication_id, publication_df in df.groupby('publication_ID'):
        author_ids = publication_df['author_ID'].values

        author_ids_permutations = list(itertools.permutations(author_ids, 2))

        result = pd.concat([result, pd.DataFrame(author_ids_permutations, columns=['author_ID_1', 'author_ID_2'])])

    result = result.rename(columns={
        'author_ID_1': ':START_ID(Author-ID)',
        'author_ID_2': ':END_ID(Author-ID)',
    })

    result[':TYPE'] = 'COLLABORATES_WITH'

    header_path = output_dir / 'collaborates_with_header.csv'
    content_path = output_dir / 'collaborates_with.csv'

    header = ':START_ID(Author-ID),:END_ID(Author-ID),:TYPE'
    save_str_to_file(header, header_path)

    columns = [
        ':START_ID(Author-ID)',
        ':END_ID(Author-ID)',
        ':TYPE',
    ]
    save_df_to_file(result, content_path, columns=columns)


def process_author_affiliation_relationships(author_to_affiliations_path: Path, output_dir: Path):
    df = pd.read_csv(author_to_affiliations_path, index_col=0)

    df = df[['author_ID', 'affiliation_ID']]

    df = df.rename(columns={
        'author_ID': ':START_ID(Author-ID)',
        'affiliation_ID': ':END_ID(Affiliation-ID)',
    })

    df[':TYPE'] = 'WORKS_AT'

    header_path = output_dir / 'works_at_header.csv'
    content_path = output_dir / 'works_at.csv'

    header = ':START_ID(Author-ID),:END_ID(Affiliation-ID),:TYPE'
    save_str_to_file(header, header_path)

    columns = [
        ':START_ID(Author-ID)',
        ':END_ID(Affiliation-ID)',
        ':TYPE',
    ]
    save_df_to_file(df, content_path, columns=columns)


def process_published_in_relationships(publications_to_venues_path: Path, output_dir: Path):
    df = pd.read_csv(publications_to_venues_path, index_col=0)

    df = df[['publication_ID', 'venue_ID']]

    df = df.rename(columns={
        'publication_ID': ':START_ID(Publication-ID)',
        'venue_ID': ':END_ID(Venue-ID)',
    })

    df[':TYPE'] = 'PUBLISHED_IN'

    header_path = output_dir / 'published_in_header.csv'
    content_path = output_dir / 'published_in.csv'

    header = ':START_ID(Publication-ID),:END_ID(Venue-ID),:TYPE'
    save_str_to_file(header, header_path)

    columns = [
        ':START_ID(Publication-ID)',
        ':END_ID(Venue-ID)',
        ':TYPE',
    ]
    save_df_to_file(df, content_path, columns=columns)


if __name__ == '__main__':
    output_dir = Path('../import/enriched')
    output_dir.mkdir(exist_ok=True)

    venues_path = Path('../dataset/enriched/venues.csv')
    process_venue_entities(venues_path, output_dir)

    authors_path = Path('../dataset/enriched/authors.csv')
    process_author_entities(authors_path, output_dir)

    affiliations_path = Path('../dataset/enriched/affiliations.csv')
    process_affiliation_entities(affiliations_path, output_dir)

    publications_path = Path('../dataset/enriched/publications.csv')
    publications_to_venues_path = Path('../dataset/enriched/pub2venue_.csv')
    process_publication_entities(publications_path, publications_to_venues_path, venues_path, output_dir)

    author_to_publications_path = Path('../dataset/enriched/author2pub.csv')
    a2p_df = pd.read_csv(author_to_publications_path, index_col=0)
    process_author_of_relationships(a2p_df, output_dir)
    process_collaborates_with_relationships(a2p_df, output_dir)

    author_to_affiliations_path = Path('../dataset/enriched/author2affiliation.csv')
    process_author_affiliation_relationships(author_to_affiliations_path, output_dir)

    process_published_in_relationships(publications_to_venues_path, output_dir)
