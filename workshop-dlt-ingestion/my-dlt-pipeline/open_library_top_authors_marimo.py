import marimo

__generated_with = "0.11.14"
app = marimo.App(width="medium")


@app.cell
def __():
    import marimo as mo
    import ibis
    import altair as alt

    return alt, ibis, mo


@app.cell
def __(ibis):
    con = ibis.duckdb.connect("open_library_pipeline.duckdb")

    books = con.table("books", database="open_library_pipeline_dataset")
    authors = con.table("books__author_name", database="open_library_pipeline_dataset")

    top_10_authors = (
        authors.group_by(authors.value.name("author"))
        .aggregate(book_count=authors._dlt_parent_id.nunique())
        .order_by(ibis.desc("book_count"), ibis.asc("author"))
        .limit(10)
    )

    top_10_authors_df = top_10_authors.execute()
    return top_10_authors_df


@app.cell
def __(alt, top_10_authors_df):
    top_10_chart = (
        alt.Chart(top_10_authors_df)
        .mark_bar()
        .encode(
            x=alt.X("book_count:Q", title="Book count"),
            y=alt.Y("author:N", sort="-x", title="Author"),
            tooltip=["author", "book_count"],
        )
        .properties(title="Top 10 Authors by Book Count")
    )

    top_10_chart


@app.cell
def __(mo, top_10_authors_df):
    mo.md("## Top 10 Authors table")
    top_10_authors_df


if __name__ == "__main__":
    app.run()
