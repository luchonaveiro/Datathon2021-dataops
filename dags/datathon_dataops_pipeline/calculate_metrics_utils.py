import pandas as pd
import logging

# Define logger
logger = logging.getLogger(__name__)


def calculate_movie_with_ratings_metrics(df: pd.DataFrame) -> pd.Series:
    numVotes = int(df["numVotes"].sum())
    runtimeMinutes = round(
        df[df["runtimeMinutes"] != "\\N"]["runtimeMinutes"].astype(int).mean(), 2
    )
    averageRating = round(df["averageRating"].mean(), 2)

    return pd.Series(
        {
            "numVotes": numVotes,
            "runtimeMinutes": runtimeMinutes,
            "averageRating": averageRating,
        }
    )


def calculate_director_metrics(df: pd.DataFrame) -> pd.Series:
    numDirectors = len(df[df["directors"] != "\\N"]["directors"].unique())

    return pd.Series({"numDirectors": numDirectors})


def calculate_writer_metrics(df: pd.DataFrame) -> pd.Series:
    numWriters = len(df[df["writers"] != "\\N"]["writers"].unique())

    return pd.Series({"numWriters": numWriters})


def normalize_list(l: list) -> str:
    s = str(l).replace("[", "").replace("]", "").replace(",", ";").replace("'", "")
    return s


def get_directors_with_most_content(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df = df[df["directors"] != "\\N"]
    df = (
        df.groupby(["startYear", "genres", "primaryName"])["tconst"]
        .count()
        .reset_index(drop=False)
    )
    # df['rank'] = df.groupby(['startYear', 'genres'])['tconst'].rank(pct=True)

    max_ranks = (
        df.groupby(["startYear", "genres"])["tconst"].max().reset_index(drop=False)
    )
    max_ranks = max_ranks.rename(columns={"tconst_y": "max_rank"})
    df = df.merge(max_ranks, on=["startYear", "genres"], how="inner")

    df = df.query("tconst_x==tconst_y")
    df = (
        df.groupby(["startYear", "genres"])["primaryName"]
        .apply(list)
        .reset_index(drop=False)
    )
    df["primaryName"] = df["primaryName"].apply(normalize_list)
    df = df.rename(columns={"primaryName": "topDirectors"})

    return df


def calculate_metrics() -> None:

    # Load movie data and filter out unwanted data
    logger.info("Importing data...")
    basics = pd.read_csv("extracted_data/title.basics.tsv", sep="\t")
    basics = basics.query(
        "titleType=='movie' and startYear in ('2015', '2016', '2017', '2018', '2018', '2019', '2020')"
    )

    # Load ratings data and join it with move data
    ratings = pd.read_csv("extracted_data/title.ratings.tsv", sep="\t")

    movie_data_with_ratings = basics.merge(ratings, on="tconst", how="inner")

    movie_data_with_ratings["genres"] = movie_data_with_ratings["genres"].str.split(",")

    movie_data_with_ratings = movie_data_with_ratings.explode("genres").reset_index(
        drop=True
    )

    # Load crew data and join it with move data
    crew = pd.read_csv("extracted_data/title.crew.tsv", sep="\t")

    names = pd.read_csv("extracted_data/name.basics.tsv", sep="\t")

    crew_with_movie_data = crew.merge(basics, on="tconst", how="inner")

    crew_with_movie_data["genres"] = crew_with_movie_data["genres"].str.split(",")
    crew_with_movie_data = crew_with_movie_data.explode("genres").reset_index(drop=True)
    crew_with_movie_data["directors"] = crew_with_movie_data["directors"].str.split(",")
    crew_with_movie_data["writers"] = crew_with_movie_data["writers"].str.split(",")

    director_with_movie_data = crew_with_movie_data.explode("directors").reset_index(
        drop=True
    )
    director_with_movie_data = director_with_movie_data.merge(
        names, left_on="directors", right_on="nconst", how="left"
    )

    writer_with_movie_data = crew_with_movie_data.explode("writers").reset_index(
        drop=True
    )

    logger.info("Data imported OK")

    # Compute desired metrics and create results data frame
    logger.info("Calculating metrics...")
    movie_data_with_ratings_metrics = (
        movie_data_with_ratings.groupby(["startYear", "genres"])
        .apply(lambda x: calculate_movie_with_ratings_metrics(x))
        .reset_index(drop=False)
    )
    director_metrics = (
        director_with_movie_data.groupby(["startYear", "genres"])
        .apply(lambda x: calculate_director_metrics(x))
        .reset_index(drop=False)
    )
    writer_metrics = (
        writer_with_movie_data.groupby(["startYear", "genres"])
        .apply(lambda x: calculate_writer_metrics(x))
        .reset_index(drop=False)
    )
    director_with_most_content = get_directors_with_most_content(
        director_with_movie_data
    )

    results = movie_data_with_ratings_metrics.merge(
        director_metrics, on=["startYear", "genres"], how="left"
    )

    results = results.merge(writer_metrics, on=["startYear", "genres"], how="left")

    results = results.merge(
        director_with_most_content, on=["startYear", "genres"], how="left"
    )

    results["numVotes"] = results["numVotes"].astype(int)

    # Export results
    results.to_csv("results.csv", index=False)
    logger.info("results.csv exported OK")
