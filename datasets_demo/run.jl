# This is a demo of how to load the TrueFX dataset with DataSets.jl
#
# To use this on JuliaHub, you'll need to add a copy of the TrueFX dataset
# under your user name. Here we assume that the input dataset is arranged in a
# single directory containing a pile of CSV files, with each file gzipped to
# save on bandwidth.
#
# A copy of this dataset is available in gzipped form at
#   s3://datasets-jl-test-us-east-1/truefx_gzipped/*

# To test this locally, make sure you've got a copy of part of the TrueFX
# dataset in the TrueFX directory (see LocalData.toml), and uncomment the
# following
#
# DataSets.load_project!(path"LocalData.toml")

# To work with the data in the REPL, we currently have a slightly dubious but
# really convenient unscoped version of open():
#
#   truefx_data = open(BlobTree, dataset("HistoricalTrueFX"))
using Distributed
@everywhere using DataSets, DataFrames, CSV, CodecZlib, Dates, Underscores, JuMP, GLPK, GLPKMathProgInterface
@everywhere @eval Base.Filesystem function mktemp(parent::AbstractString=tempdir(); cleanup::Bool=true)
    isdir(parent) || mkdir(parent)
    b = joinpath(parent, temp_prefix * "XXXXXX")
    p = ccall(:mkstemp, Int32, (Cstring,), b) # modifies b
    systemerror(:mktemp, p == -1)
    cleanup && temp_cleanup_later(b)
    return (b, fdio(p, true))
end
# Setup for arbitrage
@everywhere fiveminutes(t) = floor(t, Dates.Minute(5))
@everywhere function byfive!(df)
    df.timestamp .= fiveminutes.(df.timestamp)
    combine(groupby(df, :timestamp), :bid=>maximum=>:bid, :ask=>minimum=>:ask, :pair=>unique=>:pair)
end
@everywhere function fx_arbitrage(rates)
    nrow(rates) == 6 || return 0.0
    eurusd = rates[rates.pair .== "EUR/USD", 2][]
    gbpusd = rates[rates.pair .== "GBP/USD", 2][]
    eurgbp = rates[rates.pair .== "EUR/GBP", 2][]
    usdjpy = rates[rates.pair .== "USD/JPY", 2][]
    eurjpy = rates[rates.pair .== "EUR/JPY", 2][]
    gbpjpy = rates[rates.pair .== "GBP/JPY", 2][]
    usdeur = 1.0/eurusd
    usdgbp = 1.0/gbpusd
    gbpeur = 1.0/eurgbp
    jpyusd = 1.0/usdjpy
    jpyeur = 1.0/eurjpy
    jpygbp = 1.0/gbpjpy

    m = JuMP.Model(solver = GLPKMathProgInterface.GLPKSolverLP(msg_lev = GLPK.MSG_ERR))

    JuMP.@variables m begin
        de; dp; dy; ed; ep; ey; pd; pe; py; yd; ye; yp; profit
    end

    JuMP.@objective(m, Max, profit)

    JuMP.@constraints(m, begin
        0 <= de <= 100
        0 <= dp <= 100
        0 <= dy <= 100
        0 <= ed <= 100
        0 <= ep <= 100
        0 <= ey <= 100
        0 <= pd <= 100
        0 <= pe <= 100
        0 <= py <= 100
        0 <= yd <= 100
        0 <= ye <= 100
        0 <= yp <= 100
        de + dp + dy - eurusd*ed - gbpusd*pd - jpyusd*yd == profit
        ed + ep + ey - usdeur*de - gbpeur*pe - jpyeur*ye == 0.0
        pd + pe + py - usdgbp*dp - eurgbp*ep - jpygbp*yp == 0.0
        yd + ye + yp - usdjpy*dy - eurjpy*ey - gbpjpy*py == 0.0
    end)

    JuMP.solve(m)

    JuMP.getvalue(profit)
end
@everywhere const DATASET = open(BlobTree, dataset("HistoricalTrueFX"))
@everywhere function arbitrage_month(idx_set)
    blobs = collect(DATASET)[idx_set]
    month = reduce(vcat, open(IO, b) do io
        byfive!(CSV.read(GzipDecompressorStream(io), DataFrame,
            header=["pair", "timestamp", "bid", "ask"],
            dateformat=dateformat"yyyymmdd H:M:S.s",
            types=Dict(:date=>DateTime)))
    end for b in blobs)
    ts = groupby(month, :timestamp, sort=true)
    DataFrame(((timestamp=rates.timestamp[1], profit=fx_arbitrage(rates)) for rates in ts))
end

# Split the dataset
currencies = ["EUR", "USD", "GBP", "JPY"]
filename(blob) = blob.path.components[end]
paths = filename.(collect(DATASET))
function splitter(p)
    ps = split(p, ['.','-'])
    return (c1=ps[1][1:3], c2=ps[1][4:end], month=Date(parse.(Int, ps[2:3])...))
end
df = DataFrame(splitter.(paths))
mask = in.(df.c1, (currencies,)) .& in.(df.c2, (currencies,))
groups = unique(df[mask, :month])
bymonth = [findall(mask .& (g .== df.month)) for g in groups]

profits = @distributed (vcat) for set in bymonth
    arbitrage_month(set)
end

ENV["OUTPUTS"] = """{
    "begin":"$(Date(minimum(profits.timestamp)))",
    "end":"$(Date(maximum(profits.timestamp)))",
    "profit":$(sum(profits.profit))
}"""
CSV.write("profits.csv", profits)
ENV["RESULTS_FILE"] = joinpath(pwd(), "profits.csv")

# @everywhere begin
# function load_truefx_csv(csv_blob)
#     @info "Loading $(basename(csv_blob))"
#     open(IO, csv_blob) do io
#         buf = read(GzipDecompressorStream(io))
#         CSV.read(buf, DataFrame,
#                  header=["pair", "timestamp", "bid", "ask"],
#                  dateformat=dateformat"yyyymmdd H:M:S.s",
#                  types=Dict(:date=>DateTime))
#     end
# end

# # Compute open-high-low-close of bid price by partitioning into days
# function ohlc(data)
#     @_ data |>
#        transform(__, :timestamp=>(t->Date.(t))=>:date) |>
#        groupby(__, [:pair, :date]) |>
#        combine(__, :bid=>minimum=>:bid_low, :bid=>maximum=>:bid_high,
#                    :bid=>first=>:bid_open,  :bid=>last=>:bid_close)
# end

# function summarize(r)
#     open(BlobTree, dataset("HistoricalTrueFX")) do truefx_data
#         summaries = ohlc.(load_truefx_csv.(collect(truefx_data)[r]))
#         reduce(vcat, summaries)
#     end
# end

# end

# N = open(length∘collect, BlobTree, dataset("HistoricalTrueFX"))
# ohlc_data = reduce(vcat, pmap(summarize, Iterators.partition(1:N, N÷nworkers())))
# CSV.write("summary.csv", ohlc_data)
# ENV["OUTPUTS"] = "{\"num_days\":$(size(ohlc_data,1))}"
# ENV["RESULTS_FILE"] = joinpath(pwd(), "summary.csv")

# open(BlobTree, dataset("HistoricalTrueFX")) do truefx_data
#     # BlobTrees are iterable and this one is only a single-level tree so we can
#     # just broadcast over it:
#     summaries = ohlc.(load_truefx_csv.(truefx_data))
#     ohlc_data = vcat(summaries...)
# 
#     @info "OHLC summary of time series" ohlc_data
# 
#     CSV.write("summary.csv", ohlc_data)
# 
#     # JuliaHub-specific data output
#     ENV["OUTPUTS"] = "{\"num_days\":$(size(ohlc_data,1))}"
#     ENV["RESULTS_FILE_TO_UPLOAD"] = "summary.csv"
# end

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

