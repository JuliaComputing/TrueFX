JuliaDB TrueFX demonstration
============================

This is a demonstration of JuliaDB loading and manipulating the TrueFX tick dataset (available from https://www.truefx.com/?page=downloads)

This requires the following packages:
 - JuliaDB.jl
 - Interact.jl
 - Plots.jl
 - GR.jl
 - OnlineStats.jl
 - JuMP.jl
 - GLPK.jl
 - GLPKMathProgInterface.jl

The CSV data should be in the directory `indata`, and there should be another directory `bindata` which will be used to store the binary dataset (I set up symlinks to avoid duplicating data).

