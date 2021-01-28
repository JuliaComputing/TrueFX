# This is a demo of how to load the TrueFX dataset with DataSets.jl
#
# To use this on JuliaHub, you'll need to add a copy of the TrueFX dataset
# under your user name. Here we assume that the input dataset is arranged in a
# single directory containing a pile of CSV files, with each file gzipped to
# save on bandwidth.
#
# A copy of this dataset is available in gzipped form at
#   s3://datasets-jl-test-us-east-1/truefx_gzipped/*

using DataSets

# To test this locally, make sure you've got a copy of part of the TrueFX
# dataset in the TrueFX directory (see LocalData.toml), and uncomment the
# following
#
# DataSets.load_project!(path"LocalData.toml")

# To work with the data in the REPL, we currently have a slightly dubious but
# really convenient unscoped version of open():
#
#   truefx_data = open(BlobTree, dataset("TrueFX"))

using DataFrames
using CSV
using CodecZlib
using Dates
using Underscores

function load_truefx_csv(csv_blob)
    @info "Loading $(basename(csv_blob))"
    open(IO, csv_blob) do io
        buf = read(GzipDecompressorStream(io))
        CSV.read(buf, DataFrame,
                 header=["pair", "timestamp", "bid", "ask"],
                 dateformat=dateformat"yyyymmdd H:M:S.s",
                 types=Dict(:date=>DateTime))
    end
end

# Compute open-high-low-close of bid price by partitioning into days
function ohlc(data)
    @_ data |>
       transform(__, :timestamp=>(t->Date.(t))=>:date) |>
       groupby(__, [:pair, :date]) |>
       combine(__, :bid=>minimum=>:bid_low, :bid=>maximum=>:bid_high,
                   :bid=>first=>:bid_open,  :bid=>last=>:bid_close)
end

open(BlobTree, dataset("TrueFX")) do truefx_data
    # BlobTrees are iterable and this one is only a single-level tree so we can
    # just broadcast over it:
    summaries = ohlc.(load_truefx_csv.(truefx_data))
    ohlc_data = vcat(summaries...)

    @info "OHLC summary of time series" ohlc_data

    CSV.write("summary.csv", ohlc_data)

    # JuliaHub-specific data output
    ENV["OUTPUTS"] = "{\"num_days\":$(size(ohlc_data,1))}"
    ENV["RESULTS_FILE_TO_UPLOAD"] = "summary.csv"
end


# The following is an alternative way to package up the application more
# reusably. May not mesh well with Distributed jobs, so it's commented out for
# now.

#=
@datafunc function truefx_ohlc(truefx_data::BlobTree=>BlobTree)
    for csv in truefx_data
        @info "Processing data" csv
        data = load_truefx_csv(csv)
        @info "" ohlc(data)
    end
end
=#

# @datarun truefx_ohlc("TrueFX")

