import marimo

__generated_with = "0.11.14"
app = marimo.App(width="medium")


@app.cell
def __():
    import altair as alt
    import ibis
    import marimo as mo

    return alt, ibis, mo


@app.cell
def __(ibis):
    con = ibis.duckdb.connect("taxi_pipeline.duckdb")

    trips = con.table("nyc_taxi_data", database="nyc_taxi")

    payment_share = (
        trips.filter(trips.payment_type.notnull() & (trips.payment_type != ""))
        .group_by(method=trips.payment_type)
        .aggregate(trips=lambda t: t.count())
        .mutate(share=lambda t: t.trips / t.trips.sum())
        .order_by(ibis.desc("trips"), ibis.asc("method"))
    )

    payment_share_df = payment_share.execute()
    payment_share_df["share_pct"] = (payment_share_df["share"] * 100).round(2)

    return payment_share_df


@app.cell
def __(alt, payment_share_df):
    chart = (
        alt.Chart(payment_share_df)
        .mark_bar()
        .encode(
            x=alt.X("share_pct:Q", title="Share of trips (%)"),
            y=alt.Y("method:N", sort="-x", title="Payment method"),
            tooltip=[
                alt.Tooltip("method:N", title="Payment method"),
                alt.Tooltip("trips:Q", title="Trips", format=",d"),
                alt.Tooltip("share_pct:Q", title="Share (%)", format=".2f"),
            ],
        )
        .properties(title="Share of NYC taxi trips by payment method")
    )

    chart


@app.cell
def __(mo, payment_share_df):
    mo.md("## Payment method share table")
    payment_share_df


if __name__ == "__main__":
    app.run()