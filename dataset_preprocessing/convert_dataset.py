import json
from pathlib import Path

import pandas as pd


def read_sample(data_path: Path | str) -> pd.DataFrame:
    df = pd.DataFrame()

    with data_path.open('r') as f:
        for line in f:
            obj = json.loads(line)
            df = df.append(obj, ignore_index=True)

    df = df[['authors_parsed', 'title', 'id', 'journal-ref', 'doi', 'categories', 'update_date']]

    df = df.explode('authors_parsed')
    df['authors_parsed'] = df['authors_parsed'].apply(lambda x: ' '.join(x[::-1]).strip())

    df['title'] = df['title'].apply(lambda x: x.replace('\n', '').strip())
    df['title'] = df['title'].apply(lambda x: x.replace('  ', ' ').strip())

    return df


def extract_authors(df: pd.DataFrame) -> pd.DataFrame:
    result = df['authors_parsed'].drop_duplicates()

    return result


def extract_publications(df: pd.DataFrame) -> pd.DataFrame:
    result = df[['id', 'title', 'journal-ref', 'doi', 'update_date']]
    result = result.drop_duplicates(subset=['id'])

    result['update_date'] = pd.to_datetime(result['update_date'], format='%Y-%m-%d')
    result['update_date'] = result['update_date'].dt.strftime('%Y')

    return result


def extract_venues(df: pd.DataFrame) -> pd.DataFrame:
    result = df['journal-ref'].drop_duplicates()
    result = result.dropna()

    result = result.apply(lambda x: x.split(',')[0].strip())

    return result


def get_header_str(df: pd.DataFrame) -> str:
    columns = df.columns.tolist()
    header = ','.join(columns)

    return header


def save_str_to_file(data: str, file_path: Path | str) -> None:
    with open(file_path, 'w') as f:
        f.write(data)


def save_df_to_file(df: pd.DataFrame, file_path: Path | str, header: bool = False) -> None:
    df.to_csv(file_path, index=False, header=header)


def process_author_entities(df: pd.DataFrame, output_dir: Path) -> None:
    authors = extract_authors(df)

    entity_header = 'full_name:ID'

    authors_header_path = output_dir / 'authors_header.csv'
    authors_content_path = output_dir / 'authors.csv'

    save_str_to_file(entity_header, authors_header_path)
    save_df_to_file(authors, authors_content_path)


def process_author_relationships(df: pd.DataFrame, output_dir: Path) -> None:
    # Author -> Publication, AUTHOR_OF

    author_publication = df[['authors_parsed', 'id']]
    author_publication = author_publication.rename(columns={'authors_parsed': ':START_ID', 'id': ':END_ID'})
    author_publication[':TYPE'] = 'AUTHOR_OF'

    author_publication_header = ':START_ID,:END_ID,:TYPE'
    author_publication_content_path = output_dir / 'author_publication_rel.csv'
    author_publication_header_path = output_dir / 'author_publication_rel_header.csv'

    save_str_to_file(author_publication_header, author_publication_header_path)
    author_publication.to_csv(author_publication_content_path, index=False, header=False, mode='w')

    # Author -> Venue, PUBLISHES_AT

    author_venue = df[['authors_parsed', 'journal-ref']]
    author_venue = author_venue.rename(columns={'authors_parsed': ':START_ID', 'journal-ref': ':END_ID'})
    author_venue[':TYPE'] = 'PUBLISHES_AT'
    author_venue = author_venue.dropna()

    author_venue_header = ':START_ID,:END_ID,:TYPE'
    author_venue_content_path = output_dir / 'author_venue_rel.csv'
    author_venue_header_path = output_dir / 'author_venue_rel_header.csv'

    save_str_to_file(author_venue_header, author_venue_header_path)
    author_venue.to_csv(author_venue_content_path, index=False, header=False, mode='w')

    # Author -> Author, CO_AUTHOR

    author_author = pd.DataFrame(columns=[':START_ID', ':END_ID', ':TYPE'])
    for (pub_id, group) in df.groupby('id'):
        authors = group['authors_parsed'].tolist()
        for i in range(len(authors)):
            for j in range(i + 1, len(authors)):
                author_author = author_author.append(
                    {
                        ':START_ID': authors[i],
                        ':END_ID': authors[j],
                        ':TYPE': 'CO_AUTHOR'
                    },
                    ignore_index=True
                )

    author_author_header = ':START_ID,:END_ID,:TYPE'
    author_author_content_path = output_dir / 'author_author_rel.csv'
    author_author_header_path = output_dir / 'author_author_rel_header.csv'

    save_str_to_file(author_author_header, author_author_header_path)
    author_author.to_csv(author_author_content_path, index=False, header=False, mode='w')


def process_publication_entities(df: pd.DataFrame, output_dir: Path) -> None:
    publications = extract_publications(df)

    header = 'id:ID,title,venue,doi,update_date'

    publications_header_path = output_dir / 'publications_header.csv'
    publications_content_path = output_dir / 'publications.csv'

    save_str_to_file(header, publications_header_path)
    save_df_to_file(publications, publications_content_path)


def process_publication_relationships(df: pd.DataFrame, output_dir: Path) -> None:
    # Publication -> Venue, PUBLISHED_IN

    publication_venue = df[['id', 'journal-ref']]
    publication_venue = publication_venue.rename(columns={'id': ':START_ID', 'journal-ref': ':END_ID'})
    publication_venue[':TYPE'] = 'PUBLISHED_IN'
    publication_venue = publication_venue.dropna()

    publication_venue_header = ':START_ID,:END_ID,:TYPE'
    publication_venue_content_path = output_dir / 'publication_venue_rel.csv'
    publication_venue_header_path = output_dir / 'publication_venue_rel_header.csv'

    save_str_to_file(publication_venue_header, publication_venue_header_path)
    publication_venue.to_csv(publication_venue_content_path, index=False, header=False, mode='w')


def process_venue_entities(df: pd.DataFrame, output_dir: Path) -> None:
    venues = extract_venues(df)

    header = 'venue:ID,:LABEL'

    venues_header_path = output_dir / 'venues_header.csv'
    venues_content_path = output_dir / 'venues.csv'

    save_str_to_file(header, venues_header_path)
    save_df_to_file(venues, venues_content_path)


if __name__ == '__main__':
    data_path = Path('../dataset/sample.json')

    df = read_sample(data_path)

    output_dir = Path('../import')
    output_dir.mkdir(exist_ok=True)

    process_author_entities(df, output_dir)
    process_publication_entities(df, output_dir)
    process_venue_entities(df, output_dir)

    process_author_relationships(df, output_dir)
    process_publication_relationships(df, output_dir)
